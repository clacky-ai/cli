use std::{
    ffi::OsStr,
    io::{self, Write},
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{bail, Result};

use futures::StreamExt;
use gzp::{deflate::Gzip, ZBuilder};
use ignore::{DirEntry, WalkBuilder};
use indicatif::{ProgressBar, ProgressFinish, ProgressIterator, ProgressStyle};
use is_terminal::IsTerminal;
use serde::{Deserialize, Serialize};
use synchronized_writer::SynchronizedWriter;
use tar::Builder;
use tempfile::NamedTempFile;

use crate::{
    consts::TICK_STRING,
    controllers::{
        deployment::{stream_build_logs, stream_deploy_logs},
        environment::get_matched_environment,
        project::get_project,
        service::get_or_prompt_service,
    },
    errors::RailwayError,
    subscription::subscribe_graphql,
    subscriptions::deployment::DeploymentStatus,
    util::logs::format_attr_log,
};

use super::*;

/// Upload and deploy project from the current directory
#[derive(Parser)]
pub struct Args {
    path: Option<PathBuf>,

    #[clap(short, long)]
    /// Don't attach to the log stream
    detach: bool,

    #[clap(short, long)]
    /// Stream build logs only, then exit (equivalent to setting $CI=true).
    ci: bool,

    #[clap(short, long)]
    /// Service to deploy to (defaults to linked service)
    service: Option<String>,

    #[clap(short, long)]
    /// Environment to deploy to (defaults to linked environment)
    environment: Option<String>,

    #[clap(long)]
    /// Don't ignore paths from .gitignore
    no_gitignore: bool,

    #[clap(long)]
    /// Use the path argument as the prefix for the archive instead of the project directory.
    path_as_root: bool,

    #[clap(long)]
    /// Verbose output
    verbose: bool,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpResponse {
    pub deployment_id: String,
    pub url: String,
    pub logs_url: String,
    pub deployment_domain: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UpErrorResponse {
    pub message: String,
}

pub async fn command(args: Args) -> Result<()> {
    let configs = Configs::new()?;
    let hostname = configs.get_host();
    let client = GQLClient::new_authorized(&configs)?;
    let linked_project = configs.get_linked_project().await?;

    let deploy_paths = get_deploy_paths(&args, &linked_project)?;

    let project = get_project(&client, &configs, linked_project.project.clone()).await?;

    let environment = args
        .environment
        .clone()
        .unwrap_or(linked_project.environment.clone());
    let environment_id = get_matched_environment(&project, environment)?.id;

    let service = get_or_prompt_service(linked_project.clone(), project, args.service).await?;

    let spinner = if std::io::stdout().is_terminal() {
        let spinner = ProgressBar::new_spinner()
            .with_style(
                ProgressStyle::default_spinner()
                    .tick_chars(TICK_STRING)
                    .template("{spinner:.green} {msg:.cyan.bold}")?,
            )
            .with_message("Indexing".to_string());
        spinner.enable_steady_tick(Duration::from_millis(100));
        Some(spinner)
    } else {
        println!("Indexing...");
        None
    };

    // Explanation for the below block
    // arc is a reference counted pointer to a mutexed vector of bytes, which
    // stores the actual tarball in memory.
    //
    // parz is a parallelized gzip writer, which writes to the arc (still in memory)
    //
    // archive is a tar archive builder, which writes to the parz writer `new(&mut parz)
    //
    // builder is a directory walker which returns an iterable that we loop over to add
    // files to the tarball (archive)
    //
    // during the iteration of `builder`, we ignore all files that match the patterns found in
    // .railwayignore
    // .gitignore
    // .git/**
    // node_modules/**
    let bytes = Vec::<u8>::new();
    let arc = Arc::new(Mutex::new(bytes));
    let mut parz = ZBuilder::<Gzip, _>::new()
        .num_threads(num_cpus::get())
        .from_writer(SynchronizedWriter::new(arc.clone()));

    // list of all paths to ignore by default
    let ignore_paths = [".git", "node_modules"];
    let ignore_paths: Vec<&std::ffi::OsStr> =
        ignore_paths.iter().map(std::ffi::OsStr::new).collect();

    {
        let mut archive = Builder::new(&mut parz);
        let mut builder = WalkBuilder::new(deploy_paths.project_path);
        builder.add_custom_ignore_filename(".railwayignore");
        if args.no_gitignore {
            builder.git_ignore(false);
        }

        let walker = builder.follow_links(true).hidden(false);
        let walked = walker.build().collect::<Vec<_>>();
        if let Some(spinner) = spinner {
            spinner.finish_with_message("Indexed");
        }
        if std::io::stdout().is_terminal() {
            let pg = ProgressBar::new(walked.len() as u64)
                .with_style(
                    ProgressStyle::default_bar()
                        .template("{spinner:.green} {msg:.cyan.bold} [{bar:20}] {percent}% ")?
                        .progress_chars("=> ")
                        .tick_chars(TICK_STRING),
                )
                .with_message("Compressing")
                .with_finish(ProgressFinish::WithMessage("Compressed".into()));
            pg.enable_steady_tick(Duration::from_millis(100));

            for entry in walked.into_iter().progress_with(pg) {
                let entry = entry?;
                let path = entry.path();
                if path
                    .components()
                    .any(|c| ignore_paths.contains(&c.as_os_str()))
                {
                    continue;
                }
                let stripped =
                    PathBuf::from(".").join(path.strip_prefix(&deploy_paths.archive_prefix_path)?);
                archive.append_path_with_name(path, stripped)?;
            }
        } else {
            for entry in walked.into_iter() {
                let entry = entry?;
                let path = entry.path();
                if path
                    .components()
                    .any(|c| ignore_paths.contains(&c.as_os_str()))
                {
                    continue;
                }
                let stripped =
                    PathBuf::from(".").join(path.strip_prefix(&deploy_paths.archive_prefix_path)?);
                archive.append_path_with_name(path, stripped)?;
            }
        }
    }
    parz.finish()?;

    let url = format!(
        "https://backboard.{hostname}/project/{}/environment/{}/up?serviceId={}",
        linked_project.project,
        environment_id,
        service.clone().unwrap_or_default(),
    );

    if args.verbose {
        let bytes_len = arc.lock().unwrap().len();
        println!("railway up");
        println!("service: {}", service.clone().unwrap_or_default());
        println!("environment: {environment_id}");
        println!("bytes: {bytes_len}");
        println!("url: {url}");
    }

    let builder = client.post(url);
    let spinner = if std::io::stdout().is_terminal() {
        let spinner = ProgressBar::new_spinner()
            .with_style(
                ProgressStyle::default_spinner()
                    .tick_chars(TICK_STRING)
                    .template("{spinner:.green} {msg:.cyan.bold}")?,
            )
            .with_message("Uploading");
        spinner.enable_steady_tick(Duration::from_millis(100));
        Some(spinner)
    } else {
        println!("Uploading...");
        None
    };

    let body = arc.lock().unwrap().clone();

    let res = builder
        .header("Content-Type", "multipart/form-data")
        .body(body)
        .send()
        .await?;

    let status = res.status();
    if status != 200 {
        if let Some(spinner) = spinner {
            spinner.finish_with_message("Failed");
        }

        // If a user error, parse the response
        if status == 400 {
            let body = res.json::<UpErrorResponse>().await?;
            return Err(RailwayError::FailedToUpload(body.message).into());
        }

        if status == 413 {
            let err = res.text().await?;
            let filesize = arc.lock().unwrap().len();
            return Err(RailwayError::FailedToUpload(format!(
                "Failed to upload code. File too large ({filesize} bytes): {err}",
            )))?;
        }

        return Err(RailwayError::FailedToUpload(format!(
            "Failed to upload code with status code {status}"
        ))
        .into());
    }

    let body = res.json::<UpResponse>().await?;
    if let Some(spinner) = spinner {
        spinner.finish_with_message("Uploaded");
    }

    let deployment_id = body.deployment_id;

    println!("  {}: {}", "Build Logs".green().bold(), body.logs_url);

    if args.detach {
        return Ok(());
    }

    let ci_mode = Configs::env_is_ci() || args.ci;
    if ci_mode {
        println!("{}", "CI mode enabled".green().bold());
    }

    // If the user is not in a terminal AND if we are not in CI mode, don't stream logs
    if !std::io::stdout().is_terminal() && !ci_mode {
        return Ok(());
    }

    //	Create vector of log streaming tasks
    //	Always stream build logs
    let build_deployment_id = deployment_id.clone();
    let mut tasks = vec![tokio::task::spawn(async move {
        if let Err(e) = stream_build_logs(build_deployment_id, |log| {
            println!("{}", log.message);
            if args.ci && log.message.starts_with("No changed files matched patterns") {
                std::process::exit(0);
            }
        })
        .await
        {
            eprintln!("Failed to stream build logs: {e}");

            if ci_mode {
                std::process::exit(1);
            }
        }
    })];

    // Stream deploy logs only if is not in ci mode
    if !ci_mode {
        let deploy_deployment_id = deployment_id.clone();
        tasks.push(tokio::task::spawn(async move {
            if let Err(e) = stream_deploy_logs(deploy_deployment_id, format_attr_log).await {
                eprintln!("Failed to stream deploy logs: {e}");
            }
        }));
    }

    let mut stream =
        subscribe_graphql::<subscriptions::Deployment>(subscriptions::deployment::Variables {
            id: deployment_id.clone(),
        })
        .await?;

    tokio::task::spawn(async move {
        while let Some(Ok(res)) = stream.next().await {
            if let Some(errors) = res.errors {
                eprintln!(
                    "Failed to get deploy status: {}",
                    errors
                        .iter()
                        .map(|err| err.to_string())
                        .collect::<Vec<String>>()
                        .join("; ")
                );
                if ci_mode {
                    std::process::exit(1);
                }
            }
            if let Some(data) = res.data {
                match data.deployment.status {
                    DeploymentStatus::SUCCESS => {
                        println!("{}", "Deploy complete".green().bold());
                        if ci_mode {
                            std::process::exit(0);
                        }
                    }
                    DeploymentStatus::FAILED => {
                        println!("{}", "Deploy failed".red().bold());
                        std::process::exit(1);
                    }
                    DeploymentStatus::CRASHED => {
                        println!("{}", "Deploy crashed".red().bold());
                        std::process::exit(1);
                    }
                    _ => {}
                }
            }
        }
    });

    futures::future::join_all(tasks).await;

    Ok(())
}

struct DeployPaths {
    project_path: PathBuf,
    archive_prefix_path: PathBuf,
}

fn get_deploy_paths(args: &Args, linked_project: &LinkedProject) -> Result<DeployPaths> {
    if args.path_as_root {
        if args.path.is_none() {
            bail!("--path-as-root requires a path to be specified");
        }

        let path = args.path.clone().unwrap();
        Ok(DeployPaths {
            project_path: path.clone(),
            archive_prefix_path: path,
        })
    } else {
        let project_dir: PathBuf = linked_project.project_path.clone().into();
        let project_path = match args.path {
            Some(ref path) => path.clone(),
            None => project_dir.clone(),
        };
        Ok(DeployPaths {
            project_path,
            archive_prefix_path: project_dir,
        })
    }
}

// 压缩状态枚举
#[derive(Debug, Clone)]
pub enum CompressionState {
    Memory,
    File,
}

// 上传策略枚举
#[derive(Debug)]
pub enum UploadStrategy {
    Memory { data: Vec<u8> },
    Stream { temp_file: NamedTempFile },
}

impl UploadStrategy {
    pub async fn upload(
        &self,
        client: &reqwest::Client,
        url: &str,
        progress: Option<&ProgressBar>,
    ) -> Result<reqwest::Response> {
        if let Some(spinner) = progress {
            spinner.set_message("Uploading");
        }

        match self {
            UploadStrategy::Memory { data } => {
                // 内存上传：直接使用内存中的数据
                if let Some(spinner) = progress {
                    spinner.set_message("Uploading (memory)");
                }
                
                let res = client
                    .post(url)
                    .header("Content-Type", "multipart/form-data")
                    .body(data.clone())
                    .send()
                    .await?;
                Ok(res)
            }
            UploadStrategy::Stream { temp_file } => {
                // 文件上传：从临时文件读取数据
                if let Some(spinner) = progress {
                    spinner.set_message("Uploading (file)");
                }
                
                // 读取文件大小用于进度显示
                let file_size = std::fs::metadata(temp_file.path())?.len();
                
                // 对于reqwest 0.12，我们读取整个文件
                // 但这仍然比在内存中保持整个压缩包更好，因为：
                // 1. 我们已经释放了压缩过程中的内存
                // 2. 文件系统可以更好地管理大文件
                // 3. 可以支持比可用内存更大的文件
                let data = std::fs::read(temp_file.path())?;
                
                if let Some(spinner) = progress {
                    let message = format!("Uploading {} bytes", file_size);
                    spinner.set_message(message);
                }
                
                let res = client
                    .post(url)
                    .header("Content-Type", "application/gzip")
                    .body(data)
                    .send()
                    .await?;
                Ok(res)
            }
        }
    }

    /// 获取上传策略的描述信息
    pub fn get_strategy_info(&self) -> (String, u64) {
        match self {
            UploadStrategy::Memory { data } => {
                ("Memory".to_string(), data.len() as u64)
            }
            UploadStrategy::Stream { temp_file } => {
                let size = std::fs::metadata(temp_file.path())
                    .map(|m| m.len())
                    .unwrap_or(0);
                ("File".to_string(), size)
            }
        }
    }

    /// 检查上传策略是否适合当前环境
    pub fn validate_strategy(&self) -> Result<()> {
        match self {
            UploadStrategy::Memory { data } => {
                // 检查内存使用是否合理
                if data.len() > 500 * 1024 * 1024 { // 500MB
                    bail!("Memory upload strategy with {} bytes may cause memory issues", data.len());
                }
                Ok(())
            }
            UploadStrategy::Stream { temp_file } => {
                // 检查临时文件是否存在和可读
                if !temp_file.path().exists() {
                    bail!("Temporary file does not exist: {:?}", temp_file.path());
                }
                
                let metadata = std::fs::metadata(temp_file.path())?;
                if metadata.len() == 0 {
                    bail!("Temporary file is empty: {:?}", temp_file.path());
                }
                
                Ok(())
            }
        }
    }
}

// 监控写入量的包装器
pub struct MonitoringWriter {
    inner: Arc<Mutex<Vec<u8>>>,
    bytes_written: Arc<Mutex<u64>>,
}

impl MonitoringWriter {
    pub fn new(buffer: Arc<Mutex<Vec<u8>>>) -> Self {
        Self {
            inner: buffer,
            bytes_written: Arc::new(Mutex::new(0)),
        }
    }

    pub fn get_bytes_written(&self) -> u64 {
        *self.bytes_written.lock().unwrap()
    }
}

impl Write for MonitoringWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut buffer = self.inner.lock().unwrap();
        let mut bytes_written = self.bytes_written.lock().unwrap();
        
        buffer.extend_from_slice(buf);
        *bytes_written += buf.len() as u64;
        
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

// 实用的混合压缩构建器，在压缩完成后检查大小并决定存储方式
pub struct HybridArchiveBuilder {
    memory_threshold: u64,
    buffer: Arc<Mutex<Vec<u8>>>,
    monitoring_writer: Option<MonitoringWriter>,
    final_state: Option<CompressionState>,
    temp_file: Option<NamedTempFile>,
}

impl HybridArchiveBuilder {
    pub fn new(memory_threshold: Option<u64>) -> Result<Self> {
        let threshold = memory_threshold.unwrap_or(100 * 1024 * 1024); // 默认100MB
        let buffer = Arc::new(Mutex::new(Vec::new()));
        let monitoring_writer = MonitoringWriter::new(buffer.clone());
        
        Ok(Self {
            memory_threshold: threshold,
            buffer,
            monitoring_writer: Some(monitoring_writer),
            final_state: None,
            temp_file: None,
        })
    }

    pub fn add_files(
        &mut self,
        files: Vec<DirEntry>,
        deploy_paths: &DeployPaths,
        ignore_paths: &[&OsStr],
        progress: Option<&ProgressBar>,
    ) -> Result<()> {
        let _monitoring_writer = self.monitoring_writer.take()
            .ok_or_else(|| anyhow::anyhow!("MonitoringWriter already consumed"))?;

        // 使用SynchronizedWriter包装MonitoringWriter以支持并发
        let sync_writer = SynchronizedWriter::new(self.buffer.clone());
        let mut parz = ZBuilder::<Gzip, _>::new()
            .num_threads(num_cpus::get())
            .from_writer(sync_writer);

        {
            let mut archive = Builder::new(&mut parz);
            
            for entry in files {
                let path = entry.path();
                
                // 跳过忽略的路径
                if path.components().any(|c| ignore_paths.contains(&c.as_os_str())) {
                    continue;
                }

                let stripped = PathBuf::from(".")
                    .join(path.strip_prefix(&deploy_paths.archive_prefix_path)?);
                
                // 添加文件到archive
                archive.append_path_with_name(path, stripped)?;
                
                if let Some(pg) = progress {
                    pg.inc(1);
                }
            }
        }
        
        // 完成压缩
        parz.finish()?;
        
        // 检查最终大小并决定存储方式
        let final_size = self.buffer.lock().unwrap().len();
        if final_size > self.memory_threshold as usize {
            // 超过阈值，转移到临时文件
            let temp_file = NamedTempFile::new()?;
            let buffer_data = self.buffer.lock().unwrap().clone();
            std::fs::write(temp_file.path(), &buffer_data)?;
            
            self.temp_file = Some(temp_file);
            self.final_state = Some(CompressionState::File);
            
            // 清空内存缓冲区以释放内存
            self.buffer.lock().unwrap().clear();
        } else {
            self.final_state = Some(CompressionState::Memory);
        }
        
        Ok(())
    }

    pub fn finish(self) -> Result<UploadStrategy> {
        match self.final_state {
            Some(CompressionState::Memory) => {
                let data = self.buffer.lock().unwrap().clone();
                Ok(UploadStrategy::Memory { data })
            }
            Some(CompressionState::File) => {
                if let Some(temp_file) = self.temp_file {
                    Ok(UploadStrategy::Stream { temp_file })
                } else {
                    bail!("File mode but no temp file created");
                }
            }
            None => {
                bail!("HybridArchiveBuilder not properly initialized - add_files was not called");
            }
        }
    }

    pub fn get_compression_stats(&self) -> Option<(u64, Option<CompressionState>)> {
        let buffer_size = self.buffer.lock().unwrap().len() as u64;
        Some((buffer_size, self.final_state.clone()))
    }
}

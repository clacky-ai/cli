#!/usr/bin/env bash

set -euo pipefail

load_env_from_clacky() {
  local env_file=""

  if [[ -f "${HOME}/.clacky.deploy.env" ]]; then
    env_file="${HOME}/.clacky.deploy.env"
  fi

  if [[ -n "${env_file}" ]]; then
    # shellcheck disable=SC1090
    set -a
    source "${env_file}"
    set +a
  fi
}

find_railway_bin() {
  if [[ -n "${CLACKY_RAILWAY_BIN:-}" && -x "${CLACKY_RAILWAY_BIN}" ]]; then
    echo "${CLACKY_RAILWAY_BIN}"
    return 0
  fi

  if command -v railway >/dev/null 2>&1; then
    command -v railway
    return 0
  fi

  if [[ -x "/agent/deploytools/railway" ]]; then
    echo "/agent/deploytools/railway"
    return 0
  fi

  return 1
}

print_wrapper_help() {
  cat >&2 <<'EOF'
clackycli: Interact with platforms to deploy your project.

Usage:
  clackycli [railway subcommand and arguments...]

  use `-s <service_id>` to specify the service to manipulate.

Examples:
  clackycli up -s <service_id>
  clackycli variables -s <service_id> --set "VAR=${{Redis.REDIS_URL}}"
  clackycli connect <database_service.service_id>

Commands:
  connect        Connect to a database's shell (psql for Postgres, mongosh for MongoDB, etc.)
  run            Run a local command using variables from the active environment
  up             Upload and deploy project from the current directory
  variables      Show variables for active environment
  volume         Manage project volumes
  help           Print this message or the help of the given subcommand(s)
EOF
}

main() {
  load_env_from_clacky

  if ! railway_bin="$(find_railway_bin)"; then
    echo "Error: Railway executable not found. Please install Railway CLI and ensure it's in PATH." >&2
    exit 127
  fi

  if [[ $# -eq 0 || "${1:-}" == "help" || "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    print_wrapper_help
    exit 0
  fi

  # Intercept: Any variables query hides values in terminal, only outputs variable names (doesn't intercept when --set is used)
  if [[ "${1:-}" == "variables" ]]; then
    has_set_flag=false
    has_kv_flag=false
    for arg in "$@"; do
      case "$arg" in
        --set|--set=*) has_set_flag=true ;;
        -k|--kv|--kv=*) has_kv_flag=true ;;
      esac
    done

    if [[ "$has_set_flag" == false ]]; then
      # 仅隐藏包含 PASSWORD 的 key 以及 URL 中 :和@之间的部分，替换为等长的 *
      set +e
      mask_output() {
        awk -F= '
        function mask(str,   n, out) {
          n = length(str)
          out = ""
          for (i = 1; i <= n; i++) out = out "*"
          return out
        }
        # 处理每一行
        {
          key = $1
          val = substr($0, index($0, "=") + 1)
          if (val == "") { print $0; next }
          # 如果 key 包含 PASSWORD，全部值替换为 *
          if (toupper(key) ~ /PASSWORD/) {
            print key "=" mask(val)
            next
          }
          # 如果 value 是 URL，隐藏 :和@之间的部分
          if (val ~ /:\/\/[^:@]+:[^@]+@/) {
            # 匹配协议://用户名:密码@主机
            match(val, /:\/\/[^:@]+:([^@]+)@/)
            if (RSTART > 0) {
              pw_start = RSTART + RLENGTH - length(substr(val, RSTART, RLENGTH)) + 1
              pw_len = RLENGTH - 2 - index(substr(val, RSTART, RLENGTH), ":")
              pw = substr(val, pw_start, pw_len)
              masked_pw = mask(pw)
              # 替换密码部分
              prefix = substr(val, 1, pw_start - 1)
              suffix = substr(val, pw_start + pw_len)
              print key "=" prefix masked_pw suffix
              next
            }
          }
          print $0
        }'
      }
      if [[ "$has_kv_flag" == true ]]; then
        "${railway_bin}" "$@" | mask_output
      else
        "${railway_bin}" "$@" --kv | mask_output
      fi
      echo "For security reasons, sensitive values are masked in terminal"
      exit_code=$?
      set -e
      exit "$exit_code"
    fi
  fi

  exec "${railway_bin}" "$@"
}

main "$@"

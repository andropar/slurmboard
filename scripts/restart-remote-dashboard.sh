#!/usr/bin/env bash
# Restart a dashboard that runs in tmux on a remote cluster host, optionally
# installing a git ref first. The wheel is built locally and copied over, so
# the cluster does not need GitHub access.
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: scripts/restart-remote-dashboard.sh [options]

Restart the Slurm Dashboard tmux session on a remote host and wait until it
answers HTTP requests. Jobs are never submitted, cancelled or modified.

Options:
  --ref REF          Build REF locally and install it before restarting
                     (for example origin/main). Default: restart only.
  --host HOST        SSH host or alias              (default: agent_3d)
  --venv PATH        Remote virtualenv              (default: ~/LiteReality-Agent/.venv)
  --launcher PATH    Remote launcher command        (default: <venv>/TTSD-meta/slurm-jobs/start_dashboard)
  --session NAME     Remote tmux session            (default: ttsd-slurm-dashboard)
  --log PATH         Remote server log (appended)   (default: ~/.local/state/slurm-dashboard/server.log)
  --port PORT        Remote dashboard port          (default: 8787)
  -h, --help         Show this help.

Each option can also be set with an environment variable:
SLURMBOARD_HOST, SLURMBOARD_VENV, SLURMBOARD_LAUNCHER, SLURMBOARD_SESSION,
SLURMBOARD_LOG and SLURMBOARD_PORT. Remote paths may start with "~/".
EOF
}

ref=""
host="${SLURMBOARD_HOST:-agent_3d}"
venv="${SLURMBOARD_VENV:-~/LiteReality-Agent/.venv}"
launcher="${SLURMBOARD_LAUNCHER:-}"
session="${SLURMBOARD_SESSION:-ttsd-slurm-dashboard}"
log="${SLURMBOARD_LOG:-~/.local/state/slurm-dashboard/server.log}"
port="${SLURMBOARD_PORT:-8787}"

while (($#)); do
  case "$1" in
    --ref) ref="${2:?--ref needs a value}"; shift 2 ;;
    --host) host="${2:?--host needs a value}"; shift 2 ;;
    --venv) venv="${2:?--venv needs a value}"; shift 2 ;;
    --launcher) launcher="${2:?--launcher needs a value}"; shift 2 ;;
    --session) session="${2:?--session needs a value}"; shift 2 ;;
    --log) log="${2:?--log needs a value}"; shift 2 ;;
    --port) port="${2:?--port needs a value}"; shift 2 ;;
    -h | --help) usage; exit 0 ;;
    *) printf 'Unknown option: %s\n\n' "$1" >&2; usage >&2; exit 2 ;;
  esac
done
launcher="${launcher:-$venv/TTSD-meta/slurm-jobs/start_dashboard}"
if [[ ! "$port" =~ ^[0-9]+$ ]]; then
  printf 'Port must be numeric: %s\n' "$port" >&2
  exit 2
fi

ssh_opts=(-o BatchMode=yes -o ConnectTimeout=10)
workdir="$(mktemp -d)"
trap 'rm -rf "$workdir"' EXIT
remote_wheel=""
commit=""

if [[ -n "$ref" ]]; then
  repo="$(git -C "$(dirname -- "${BASH_SOURCE[0]}")" rev-parse --show-toplevel)"
  commit="$(git -C "$repo" rev-parse --verify "$ref^{commit}")"
  printf 'Building %s (%s) locally...\n' "$ref" "${commit:0:12}"
  mkdir "$workdir/src" "$workdir/dist"
  git -C "$repo" archive "$commit" | tar -x -C "$workdir/src"
  if command -v uv >/dev/null; then
    (cd "$workdir/src" && uv build --wheel --quiet --out-dir "$workdir/dist")
  else
    python3 -m pip wheel --quiet --no-deps --wheel-dir "$workdir/dist" "$workdir/src"
  fi
  wheels=("$workdir"/dist/slurm_dashboard-*.whl)
  if [[ ! -f "${wheels[0]}" ]]; then
    printf 'Wheel build produced no slurm_dashboard wheel.\n' >&2
    exit 1
  fi
  remote_wheel="/tmp/$(basename -- "${wheels[0]}")"
  scp -q "${ssh_opts[@]}" "${wheels[0]}" "$host:$remote_wheel"
fi

# Arguments are quoted here so the remote shell receives them unchanged.
printf -v remote_args '%q ' "$venv" "$launcher" "$session" "$log" "$port" "$remote_wheel" "$commit"
# shellcheck disable=SC2029
ssh "${ssh_opts[@]}" "$host" "bash -s -- $remote_args" <<'REMOTE'
set -euo pipefail
expand() { local p="$1"; [[ "$p" == "~/"* ]] && p="$HOME/${p#\~/}"; printf '%s' "$p"; }
venv="$(expand "$1")" launcher="$(expand "$2")" session="$3"
log="$(expand "$4")" port="$5" wheel="$6" commit="$7"
marker="$venv/.slurm-dashboard-deployed"
url="http://127.0.0.1:$port/"
listening() { ss -ltn | awk '{print $4}' | grep -qE "[:.]$port\$"; }
if [[ -n "$wheel" ]]; then trap 'rm -f "$wheel"' EXIT; fi

for tool in tmux curl ss; do
  command -v "$tool" >/dev/null || { printf 'Missing on remote: %s\n' "$tool" >&2; exit 1; }
done
[[ -x "$launcher" ]] || { printf 'Launcher not executable: %s\n' "$launcher" >&2; exit 1; }
if ! tmux has-session -t "=$session" 2>/dev/null && listening; then
  printf 'Port %s is in use without tmux session %s; nothing was restarted.\n' \
    "$port" "$session" >&2
  exit 1
fi

if [[ -n "$wheel" ]]; then
  previous="$(cat "$marker" 2>/dev/null || true)"
  printf 'Previously deployed commit: %s\n' "${previous:-unknown}"
  export PATH="$HOME/.local/bin:$PATH"
  if command -v uv >/dev/null; then
    uv pip install --quiet --offline --python "$venv/bin/python" --no-deps \
      --reinstall-package slurm-dashboard "$wheel"
  else
    "$venv/bin/python" -m pip install --quiet --no-index --no-deps \
      --force-reinstall "$wheel"
  fi
  printf '%s\n' "$commit" >"$marker"
  printf 'Installed %s\n' "$commit"
fi

if tmux has-session -t "=$session" 2>/dev/null; then
  tmux kill-session -t "=$session"
  for _ in $(seq 20); do listening || break; sleep 0.5; done
  if listening; then
    printf 'Port %s is still in use after stopping %s.\n' "$port" "$session" >&2
    exit 1
  fi
fi

mkdir -p "$(dirname -- "$log")"
printf -v command 'exec %q >> %q 2>&1' "$launcher" "$log"
tmux new-session -d -s "$session" "$command"
for _ in $(seq 30); do
  if curl --fail --silent --max-time 2 --output /dev/null "$url"; then
    printf '%s is responding at %s\n' "$session" "$url"
    exit 0
  fi
  tmux has-session -t "=$session" 2>/dev/null || break
  sleep 1
done
printf '%s did not become ready; last log lines from %s:\n' "$session" "$log" >&2
tail -n 20 "$log" >&2 || true
exit 1
REMOTE

"""Pure parsers for the text formats Slurm commands emit."""

from __future__ import annotations

import re
from datetime import datetime
from typing import Dict, List, Optional, Tuple

_MEM_MULTIPLIERS = {"K": 1024, "M": 1024**2, "G": 1024**3, "T": 1024**4, "P": 1024**5}
_NO_TIME = {"", "Unknown", "None", "N/A", "(null)", "NONE"}

# States that mean the job is still known to the scheduler.
ACTIVE_STATES = {
    "RUNNING",
    "PENDING",
    "CONFIGURING",
    "COMPLETING",
    "SUSPENDED",
    "REQUEUED",
    "REQUEUE_HOLD",
    "REQUEUE_FED",
    "RESIZING",
    "STAGE_OUT",
    "SIGNALING",
    "STOPPED",
    "RESV_DEL_HOLD",
    "SPECIAL_EXIT",
}


def parse_gpu_count(gres_str: str) -> int:
    """Parse the GPU count from a GRES/TRES string.

    Handles "gpu:2", "gpu:a100:4", "gres/gpu=2", "gres/gpu:a100=4" and full
    TRES strings such as "billing=10,cpu=4,gres/gpu=2,mem=16G". When a TRES
    string lists both the generic and the typed GPU entry, the generic one wins
    so GPUs are not counted twice.
    """
    if not gres_str:
        return 0
    text = gres_str.lower()
    generic = re.search(r"gres/gpu=(\d+)", text)
    if generic:
        return int(generic.group(1))
    match = re.search(r"gpu[^=:,]*(?::[^=:,]*)?[=:](\d+)", text)
    return int(match.group(1)) if match else 0


def parse_time_to_seconds(time_str: str) -> float:
    """Parse a Slurm duration to seconds.

    Accepts "D-HH:MM:SS", "HH:MM:SS", "MM:SS" and fractional seconds
    ("MM:SS.mmm", as sacct prints TotalCPU for short jobs).
    """
    if not time_str:
        return 0.0
    text = time_str.strip()
    if text in {"UNLIMITED", "INVALID", "Partition_Limit", "N/A", "NOT_SET"}:
        return 0.0

    days = 0
    if "-" in text:
        day_part, text = text.split("-", 1)
        try:
            days = int(day_part)
        except ValueError:
            return 0.0

    try:
        parts = [float(p) for p in text.split(":")]
    except ValueError:
        return 0.0

    if len(parts) == 3:
        hours, mins, secs = parts
    elif len(parts) == 2:
        # With a day prefix Slurm means HH:MM, otherwise MM:SS.
        if days:
            hours, mins, secs = parts[0], parts[1], 0.0
        else:
            hours, mins, secs = 0.0, parts[0], parts[1]
    elif len(parts) == 1:
        if days:
            hours, mins, secs = parts[0], 0.0, 0.0
        else:
            hours, mins, secs = 0.0, parts[0], 0.0
    else:
        return 0.0
    return days * 86400 + hours * 3600 + mins * 60 + secs


def parse_memory(mem_str: str) -> float:
    """Parse a Slurm memory value ("4Gn", "1024M", "102400K") to bytes.

    A bare number is megabytes, which is what Slurm assumes for requests.
    The per-node / per-cpu suffix is ignored here; see `requested_memory_bytes`.
    """
    if not mem_str:
        return 0.0
    text = mem_str.strip().upper().rstrip("NC")
    match = re.match(r"([\d.]+)([KMGTP]?)", text)
    if not match:
        return 0.0
    try:
        value = float(match.group(1))
    except ValueError:
        return 0.0
    return value * _MEM_MULTIPLIERS.get(match.group(2), 1024**2)


def requested_memory_bytes(req_mem: str, cpus: int, nodes: int) -> float:
    """Total memory a job requested, resolving per-cpu / per-node suffixes."""
    base = parse_memory(req_mem)
    if not base:
        return 0.0
    suffix = req_mem.strip()[-1:].lower()
    if suffix == "c":
        return base * max(cpus, 1)
    if suffix == "n":
        return base * max(nodes, 1)
    return base


def parse_timestamp(value: str) -> Optional[str]:
    """Convert a Slurm local timestamp to an ISO string with UTC offset.

    Slurm prints naive timestamps in the cluster's local time. Attaching the
    offset here keeps the browser from re-interpreting them in its own zone.
    """
    if not value or value.strip() in _NO_TIME:
        return None
    try:
        naive = datetime.strptime(value.strip()[:19], "%Y-%m-%dT%H:%M:%S")
    except ValueError:
        return None
    return naive.astimezone().isoformat()


def timestamp_epoch(iso_value: Optional[str]) -> Optional[float]:
    """Epoch seconds for a value produced by `parse_timestamp`."""
    if not iso_value:
        return None
    try:
        return datetime.fromisoformat(iso_value).timestamp()
    except ValueError:
        return None


def normalize_state(raw: str) -> str:
    """Reduce a raw state ("CANCELLED by 1000", "COMPLETED+") to one word."""
    if not raw:
        return "UNKNOWN"
    return raw.strip().split()[0].rstrip("+").upper() or "UNKNOWN"


def state_category(state: str) -> str:
    """Map a normalized state to the category the UI colours by."""
    s = state.upper()
    if s in {"RUNNING", "COMPLETING", "STAGE_OUT", "SIGNALING", "RESIZING"}:
        return "running"
    if s in {"PENDING", "CONFIGURING", "SUSPENDED", "STOPPED", "RESV_DEL_HOLD"}:
        return "pending"
    if s.startswith("REQUEUE") or s == "SPECIAL_EXIT":
        return "pending"
    if s == "COMPLETED":
        return "completed"
    if s == "TIMEOUT" or s == "DEADLINE":
        return "timeout"
    if s in {"CANCELLED", "PREEMPTED", "REVOKED"}:
        return "cancelled"
    if s in {"FAILED", "NODE_FAIL", "OUT_OF_MEMORY", "BOOT_FAIL"}:
        return "failed"
    return "unknown"


def is_active_state(state: str) -> bool:
    """True while the scheduler still tracks the job."""
    return state.upper() in ACTIVE_STATES


def split_job_id(job_id: str) -> Tuple[str, Optional[str]]:
    """Split a job ID into (parent job ID, step name or None).

    "123.batch" -> ("123", "batch"); "123_4.0" -> ("123_4", "0");
    "123_[1-5%2]" -> ("123_[1-5%2]", None).
    """
    if "." in job_id and not job_id.endswith("]"):
        parent, step = job_id.split(".", 1)
        return parent, step
    return job_id, None


def array_parts(job_id: str) -> Tuple[str, Optional[str]]:
    """Split "123_4" into ("123", "4"); non-array IDs return (id, None)."""
    if "_" in job_id:
        base, task = job_id.split("_", 1)
        return base, task
    return job_id, None


def parse_exit_code(value: str) -> Optional[str]:
    """Return the exit status from Slurm's "code:signal" pair."""
    if not value:
        return None
    return value.split(":", 1)[0]


def parse_dependencies(dep_str: str) -> List[Tuple[str, str]]:
    """Parse a dependency string into (job_id, type) pairs.

    "afterok:123:456(unfulfilled),afterany:789" ->
    [("123", "afterok"), ("456", "afterok"), ("789", "afterany")]
    """
    if not dep_str or dep_str in {"(null)", "N/A"}:
        return []
    deps: List[Tuple[str, str]] = []
    for clause in re.split(r"[,?]", dep_str):
        match = re.match(r"\s*(after\w*|expand):(.+)", clause)
        if not match:
            continue
        dep_type = match.group(1)
        for job_id in re.findall(r"(\d+)(?:_\S*?)?(?:\+\d+)?(?:\(\w+\))?(?::|$)", match.group(2)):
            deps.append((job_id, dep_type))
    return deps


def parse_scontrol_job(output: str) -> Dict[str, str]:
    """Parse `scontrol show job` output into a key/value dict.

    Values can contain spaces (Command, WorkDir), so a value runs until the
    next " Key=" token on the line rather than the next space.
    """
    info: Dict[str, str] = {}
    key_re = re.compile(r"(?:^|\s)([A-Za-z][A-Za-z0-9_:/\[\]-]*)=")
    for line in output.splitlines():
        matches = list(key_re.finditer(line))
        for idx, match in enumerate(matches):
            end = matches[idx + 1].start() if idx + 1 < len(matches) else len(line)
            info.setdefault(match.group(1), line[match.end() : end].strip())
    return info


def format_duration(seconds: float) -> str:
    """Format seconds as a short human-readable duration."""
    seconds = int(seconds)
    if seconds < 60:
        return f"{seconds}s"
    if seconds < 3600:
        return f"{seconds // 60}m"
    if seconds < 86400:
        hours, mins = seconds // 3600, (seconds % 3600) // 60
        return f"{hours}h {mins}m" if mins else f"{hours}h"
    days, hours = seconds // 86400, (seconds % 86400) // 3600
    return f"{days}d {hours}h" if hours else f"{days}d"


def format_elapsed(seconds: float) -> str:
    """Format seconds the way Slurm prints elapsed time ([D-]HH:MM:SS)."""
    seconds = int(seconds)
    days, rem = divmod(seconds, 86400)
    hours, rem = divmod(rem, 3600)
    mins, secs = divmod(rem, 60)
    clock = f"{hours:02d}:{mins:02d}:{secs:02d}"
    return f"{days}-{clock}" if days else clock


def format_bytes(value: float) -> str:
    """Format a byte count with a Slurm-style unit suffix."""
    if value >= 1024**3:
        return f"{value / 1024**3:.1f}G"
    if value >= 1024**2:
        return f"{value / 1024**2:.0f}M"
    if value >= 1024:
        return f"{value / 1024:.0f}K"
    return f"{int(value)}B"

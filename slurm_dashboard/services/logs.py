"""Locating, streaming and searching job log files."""

from __future__ import annotations

import codecs
import logging
import os
import re
import time
from collections import deque
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Deque, Dict, Iterator, List, Optional, Tuple

from slurm_dashboard.services.parsing import array_parts
from slurm_dashboard.services.runner import TTLCache

if TYPE_CHECKING:
    from slurm_dashboard.config import LogPattern

logger = logging.getLogger(__name__)

SNAPSHOT_BYTES = 200_000
FULL_SNAPSHOT_BYTES = 8_000_000
LINE_COUNT_LIMIT = 64 * 1024 * 1024
READ_CHUNK = 64 * 1024
MAX_LINE_CHARS = 2000
STREAMS = {"stdout": "out", "stderr": "err"}


def human_size(size: float) -> str:
    """Convert bytes to a human-readable size string."""
    value = float(size)
    for unit in ("B", "KB", "MB", "GB"):
        if value < 1024:
            return f"{value:.1f}{unit}"
        value /= 1024
    return f"{value:.1f}TB"


# ------------------------------------------------------------------ discovery


class LogIndex:
    """Index of log files under ``log_root`` that match the configured pattern.

    Used for jobs whose output path Slurm does not report, and to list jobs
    that have logs on disk but have aged out of the accounting window.
    """

    def __init__(self, log_root: Path, pattern: "LogPattern", ttl: int = 20) -> None:
        self.log_root = log_root
        self.pattern = pattern
        self.ttl = ttl
        self._cache = TTLCache()

    def entries(self) -> Dict[str, dict]:
        """Map job ID -> {"name", "out", "err"} for every matching log file."""
        entries, _ = self._cache.get(("index",), self.ttl, self._scan)
        return entries or {}

    def _scan(self) -> Dict[str, dict]:
        entries: Dict[str, dict] = {}
        if not self.log_root.is_dir():
            return entries
        try:
            for path in self.log_root.glob(self.pattern.to_glob_pattern()):
                info = self.pattern.extract_job_info(self.log_root, path)
                if not info:
                    continue
                entry = entries.setdefault(
                    info["id"], {"name": info["name"], "out": None, "err": None}
                )
                entry[info["stream"]] = path
        except OSError as exc:
            logger.warning("Could not scan %s: %s", self.log_root, exc)
        return entries

    def lookup(self, job: dict, stream: str) -> Optional[Path]:
        """Find the indexed log of a job, trying its display and raw IDs."""
        entries = self.entries()
        for job_id in (job.get("id"), job.get("raw_id")):
            entry = entries.get(job_id or "")
            if entry and entry.get(stream):
                return entry[stream]
        return None

    def orphans(self, known_ids: set, limit: int = 100) -> List[dict]:
        """Jobs that only exist as log files, newest (highest ID) first."""
        unknown = [job_id for job_id in self.entries() if job_id not in known_ids]
        unknown.sort(key=lambda j: [int(p) for p in re.findall(r"\d+", j)], reverse=True)
        rows = []
        for job_id in unknown[:limit]:
            entry = self.entries()[job_id]
            path = entry.get("out") or entry.get("err")
            try:
                mtime = path.stat().st_mtime
            except OSError:
                continue
            rows.append(
                {
                    "id": job_id,
                    "name": entry["name"],
                    "state": "UNKNOWN",
                    "state_category": "unknown",
                    "end": datetime.fromtimestamp(mtime).astimezone().isoformat(timespec="seconds"),
                    "log_key": job_id,
                    "source": "logs",
                    "owned": False,
                }
            )
        return rows


def expand_filename_pattern(pattern: str, job: dict) -> str:
    """Expand Slurm filename patterns (%j, %A, %a, %x, %u, %N, %%) for a job."""
    base, task = array_parts(job.get("id", ""))
    raw_id = job.get("raw_id") or job.get("id", "")
    first_node = re.sub(r"\[(\d+)[^\]]*\]", r"\1", (job.get("nodelist") or "").split(",")[0])
    values = {
        "j": raw_id,
        "J": raw_id,
        "A": base if task else raw_id,
        "a": task or "4294967294",
        "x": job.get("name", ""),
        "u": job.get("user", ""),
        "N": first_node,
        "%": "%",
    }

    def replace(match: "re.Match[str]") -> str:
        width, key = match.group(1), match.group(2)
        value = values.get(key)
        if value is None:
            return match.group(0)
        return value.zfill(int(width)) if width and value.isdigit() else value

    return re.sub(r"%(\d*)([jJAaxuN%])", replace, pattern)


def candidate_paths(job: dict, kind: str, scontrol: Dict[str, str], index: LogIndex) -> List[Path]:
    """Possible locations of a job's log, most reliable first.

    Every path is derived from scheduler data or the configured pattern, never
    from anything the browser sent.
    """
    stream = STREAMS[kind]
    work_dir = scontrol.get("WorkDir") or job.get("work_dir") or ""
    candidates: List[Path] = []

    def add(raw: Optional[str]) -> None:
        if not raw or raw in {"(null)", "/dev/null"}:
            return
        path = Path(expand_filename_pattern(raw, job))
        if not path.is_absolute():
            if not work_dir:
                return
            path = Path(work_dir) / path
        if path not in candidates:
            candidates.append(path)

    # 1. What the scheduler says (already expanded for active jobs).
    add(scontrol.get("StdOut" if kind == "stdout" else "StdErr"))
    # 2. The pattern recorded in accounting; stderr defaults to the stdout file.
    add(job.get("stdout_pattern" if kind == "stdout" else "stderr_pattern"))
    if kind == "stderr" and not job.get("stderr_pattern"):
        add(job.get("stdout_pattern"))
    # 3. The dashboard's --log-pattern under --log-root.
    indexed = index.lookup(job, stream)
    if indexed and indexed not in candidates:
        candidates.append(indexed)
    # 4. Slurm's default file name in the working directory.
    add("slurm-%A_%a.out" if job.get("array_task_id") else "slurm-%j.out")
    return candidates


def resolve_log(
    job: dict, kind: str, scontrol: Dict[str, str], index: LogIndex
) -> Tuple[Optional[Path], List[str]]:
    """Return (existing log file or None, every path that was tried)."""
    tried = candidate_paths(job, kind, scontrol, index)
    for path in tried:
        try:
            if path.is_file():
                return path, [str(p) for p in tried]
        except OSError:
            continue
    return None, [str(p) for p in tried]


# ------------------------------------------------------------------ streaming


def _count_lines(handle, end: int) -> int:
    """Number of newlines in the first `end` bytes of an open binary file."""
    handle.seek(0)
    count, remaining = 0, end
    while remaining > 0:
        chunk = handle.read(min(1024 * 1024, remaining))
        if not chunk:
            break
        count += chunk.count(b"\n")
        remaining -= len(chunk)
    return count


def tail_events(
    path: Path,
    poll: float = 1.0,
    heartbeat: float = 15.0,
    snapshot_bytes: int = SNAPSHOT_BYTES,
) -> Iterator[dict]:
    """Yield log updates for a file: one snapshot, then appended chunks.

    Event dicts carry one of ``snapshot``, ``append``, ``reset``, ``error`` or
    ``ping``. The snapshot is the last `snapshot_bytes` of the file and says
    which line number it starts at (None for files too large to count cheaply).
    Pings let the server notice a closed browser tab, so idle streams of
    finished jobs do not keep a thread forever.
    """
    try:
        handle = path.open("rb")
    except OSError as exc:
        yield {"error": f"Cannot open log: {exc.strerror or exc}"}
        return

    with handle:
        decoder = codecs.getincrementaldecoder("utf-8")("replace")
        size = os.fstat(handle.fileno()).st_size
        start = max(size - snapshot_bytes, 0)
        handle.seek(start)
        data = handle.read()
        position = handle.tell()
        first_line: Optional[int] = 1
        if start > 0:
            # Drop the partial first line of the tail.
            newline = data.find(b"\n")
            if newline >= 0:
                start += newline + 1
                data = data[newline + 1 :]
            first_line = None
            if size <= LINE_COUNT_LIMIT:
                first_line = _count_lines(handle, start) + 1
                handle.seek(position)
        yield {
            "snapshot": decoder.decode(data),
            "truncated": start > 0,
            "size": size,
            "first_line": first_line,
        }

        idle = 0.0
        while True:
            chunk = handle.read(READ_CHUNK)
            if chunk:
                position += len(chunk)
                idle = 0.0
                text = decoder.decode(chunk)
                if text:
                    yield {"append": text}
                continue
            try:
                current = os.stat(path).st_size
            except OSError:
                yield {"error": "Log file was removed"}
                return
            if current < position:
                # File was truncated or rewritten (e.g. a requeued job).
                handle.seek(0)
                position = 0
                decoder.reset()
                yield {"reset": True}
                continue
            time.sleep(poll)
            idle += poll
            if idle >= heartbeat:
                idle = 0.0
                yield {"ping": True}


# -------------------------------------------------------------------- search


def search_log(
    path: Path, pattern: str, context_lines: int = 3, max_matches: int = 500, use_regex: bool = True
) -> dict:
    """Search a log file line by line without loading it into memory.

    Returns:
        Dict with "matches" (each with line number, text and context),
        "total_matches" and "truncated".
    """
    try:
        regex = re.compile(pattern if use_regex else re.escape(pattern), re.IGNORECASE)
    except re.error as exc:
        return {"error": f"Invalid regex: {exc}", "matches": [], "total_matches": 0}

    matches: List[dict] = []
    waiting: List[dict] = []  # matches still collecting context_after
    before: Deque[dict] = deque(maxlen=max(context_lines, 0))
    total = 0
    try:
        with path.open("r", errors="replace") as handle:
            for number, raw in enumerate(handle, start=1):
                line = {"line_number": number, "text": raw.rstrip("\n")[:MAX_LINE_CHARS]}
                for match in waiting:
                    match["context_after"].append(line)
                waiting = [m for m in waiting if len(m["context_after"]) < context_lines]
                if regex.search(line["text"]):
                    total += 1
                    if len(matches) < max_matches:
                        match = dict(line, context_before=list(before), context_after=[])
                        matches.append(match)
                        if context_lines > 0:
                            waiting.append(match)
                before.append(line)
    except OSError as exc:
        return {
            "error": f"Could not read file: {exc.strerror or exc}",
            "matches": [],
            "total_matches": 0,
        }

    return {"matches": matches, "total_matches": total, "truncated": total > max_matches}

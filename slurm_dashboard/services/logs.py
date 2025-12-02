"""Log file handling for Slurm Dashboard."""

import re
import time
from pathlib import Path
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from slurm_dashboard.config import LogPattern


def search_log(
    path: Path,
    pattern: str,
    context_lines: int = 3,
    max_matches: int = 500,
    use_regex: bool = True,
) -> dict:
    """
    Search a log file for a pattern and return matching lines with context.

    Args:
        path: Path to the log file
        pattern: Search pattern (regex or literal)
        context_lines: Number of context lines before/after each match
        max_matches: Maximum matches to return
        use_regex: Whether to treat pattern as regex (False for literal)

    Returns:
        Dict with 'matches' list and 'total_matches' count
    """
    try:
        if use_regex:
            regex = re.compile(pattern, re.IGNORECASE)
        else:
            # Escape for literal search
            regex = re.compile(re.escape(pattern), re.IGNORECASE)
    except re.error as e:
        return {"error": f"Invalid regex: {e}", "matches": [], "total_matches": 0}

    matches = []
    all_lines = []

    try:
        with path.open("r", errors="replace") as f:
            all_lines = f.readlines()
    except OSError as e:
        return {"error": f"Could not read file: {e}", "matches": [], "total_matches": 0}

    total_matches = 0
    matched_line_indices = []

    # First pass: find all matching line indices
    for i, line in enumerate(all_lines):
        if regex.search(line):
            matched_line_indices.append(i)
            total_matches += 1

    # Second pass: build match results with context
    for match_idx in matched_line_indices:
        if len(matches) >= max_matches:
            break

        # Calculate context range
        start = max(0, match_idx - context_lines)
        end = min(len(all_lines), match_idx + context_lines + 1)

        context_before = [
            {"line_number": j + 1, "text": all_lines[j].rstrip()}
            for j in range(start, match_idx)
        ]
        context_after = [
            {"line_number": j + 1, "text": all_lines[j].rstrip()}
            for j in range(match_idx + 1, end)
        ]

        matches.append({
            "line_number": match_idx + 1,
            "text": all_lines[match_idx].rstrip(),
            "context_before": context_before,
            "context_after": context_after,
        })

    return {
        "matches": matches,
        "total_matches": total_matches,
        "truncated": total_matches > max_matches,
    }


def human_size(size: int) -> str:
    """Convert bytes to human-readable size string."""
    units = ["B", "KB", "MB", "GB", "TB"]
    value = float(size)
    for unit in units:
        if value < 1024 or unit == units[-1]:
            return f"{value:.1f}{unit}"
        value /= 1024
    return f"{value:.1f}PB"


def safe_log_path(
    log_key: str, kind: str, log_root: Path, log_pattern: "LogPattern"
) -> Optional[Path]:
    """
    Safely resolve a log path from a log key using the configured pattern.

    Args:
        log_key: Key in format "name::job_id"
        kind: "stdout" or "stderr"
        log_root: Base directory for logs
        log_pattern: Pattern configuration for log paths

    Returns:
        Path to the log file, or None if invalid/doesn't exist
    """
    if "::" not in log_key:
        return None
    name, job_id = log_key.split("::", 1)
    if not job_id.isdigit():
        return None

    stream = "out" if kind == "stdout" else "err"
    target = log_pattern.format_path(log_root, name, job_id, stream)

    # Security: ensure path doesn't escape log_root
    # Resolve both paths to handle symlinks (e.g., /var -> /private/var on macOS)
    try:
        target.relative_to(log_root.resolve())
    except ValueError:
        return None

    if not target.exists():
        return None
    return target


def collect_recent_jobs(
    log_root: Path, log_pattern: "LogPattern", limit: int = 200
) -> list[dict]:
    """
    Collect recent jobs from log files using the configured pattern.

    Args:
        log_root: Base directory for logs
        log_pattern: Pattern configuration for log paths
        limit: Maximum number of jobs to return

    Returns:
        List of job dicts sorted by modification time (newest first)
    """
    entries = []
    seen_jobs = set()  # Track (name, id) pairs to avoid duplicates

    # Use glob pattern to find matching files
    glob_pat = log_pattern.to_glob_pattern()

    for log_file in log_root.glob(glob_pat):
        if not log_file.is_file():
            continue

        # Extract job info from the file path
        info = log_pattern.extract_job_info(log_root, log_file)
        if not info:
            continue

        name = info["name"]
        job_id = info["id"]
        stream = info["stream"]

        # Skip if we've already processed this job
        job_key = (name, job_id)
        if job_key in seen_jobs:
            continue
        seen_jobs.add(job_key)

        # Get file stats for the stdout file (or use this file if it's stdout)
        if stream == "out":
            stdout_path = log_file
        else:
            stdout_path = log_pattern.format_path(log_root, name, job_id, "out")

        stderr_path = log_pattern.format_path(log_root, name, job_id, "err")

        # Calculate size and updated time
        try:
            if stdout_path.exists():
                stdout_stat = stdout_path.stat()
                updated = time.strftime(
                    "%Y-%m-%d %H:%M:%S", time.localtime(stdout_stat.st_mtime)
                )
                size_bytes = stdout_stat.st_size
            else:
                # Fall back to stderr if stdout doesn't exist
                stderr_stat = stderr_path.stat()
                updated = time.strftime(
                    "%Y-%m-%d %H:%M:%S", time.localtime(stderr_stat.st_mtime)
                )
                size_bytes = 0

            if stderr_path.exists():
                size_bytes += stderr_path.stat().st_size

        except OSError:
            continue

        entries.append(
            {
                "updated": updated,
                "name": name,
                "id": job_id,
                "log_key": f"{name}::{job_id}",
                "size": human_size(size_bytes),
                "size_bytes": size_bytes,
            }
        )

    entries.sort(key=lambda row: row["updated"], reverse=True)
    return entries[:limit]

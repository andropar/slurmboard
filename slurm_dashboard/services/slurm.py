"""Slurm command wrappers for querying job information."""
from __future__ import annotations

import re
import subprocess
from pathlib import Path
from typing import Optional


def parse_time_to_seconds(time_str: str) -> float:
    """Parse slurm time format (HH:MM:SS or DD-HH:MM:SS) to seconds."""
    if not time_str or time_str == "00:00:00":
        return 0.0

    days = 0
    if "-" in time_str:
        day_part, time_part = time_str.split("-", 1)
        days = int(day_part)
        time_str = time_part

    parts = time_str.split(":")
    if len(parts) == 3:
        hours, mins, secs = parts
        return days * 86400 + int(hours) * 3600 + int(mins) * 60 + float(secs)
    return 0.0


def parse_memory(mem_str: str) -> float:
    """Parse slurm memory format (e.g., '4Gn', '1024M') to bytes."""
    if not mem_str:
        return 0.0

    mem_str = mem_str.strip().rstrip("n").rstrip("c")
    match = re.match(r"([\d.]+)([KMGT]?)", mem_str.upper())
    if not match:
        return 0.0

    value, unit = match.groups()
    value = float(value)

    multipliers = {"K": 1024, "M": 1024**2, "G": 1024**3, "T": 1024**4, "": 1024**2}
    return value * multipliers.get(unit, 1)


def get_running_jobs(user: str) -> list[dict]:
    """Get list of running jobs for a user."""
    fmt = "%i|%j|%T|%M|%l|%D|%R"
    try:
        proc = subprocess.run(
            ["squeue", "-u", user, "--noheader", f"--format={fmt}"],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
    except FileNotFoundError:
        return []
    if proc.returncode != 0:
        return []

    rows = []
    for line in proc.stdout.strip().splitlines():
        parts = line.split("|", maxsplit=6)
        if len(parts) != 7:
            continue
        job_id, name, state, runtime, limit, nodes, reason = parts
        # Log key is simply name::id - the pattern handles path resolution
        log_key = f"{name}::{job_id}"
        rows.append(
            {
                "id": job_id,
                "name": name,
                "state": state,
                "runtime": runtime,
                "limit": limit,
                "nodes": nodes,
                "reason": reason,
                "log_key": log_key,
            }
        )
    return rows


def get_job_states_batch(job_ids: list[str], user: str) -> dict[str, str]:
    """Get states for multiple jobs in a single sacct query.

    Args:
        job_ids: List of job IDs to query
        user: Username to filter by

    Returns:
        Dictionary mapping job_id to state string
    """
    if not job_ids:
        return {}

    # Query all job IDs at once using comma-separated list
    job_list = ",".join(job_ids)
    try:
        proc = subprocess.run(
            [
                "sacct",
                "-j",
                job_list,
                "-u",
                user,
                "--noheader",
                "--format=JobID,State",
                "-X",  # Only show main job entries, not steps
            ],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return {}

    if proc.returncode != 0:
        return {}

    states = {}
    for line in proc.stdout.strip().splitlines():
        parts = line.split()
        if len(parts) >= 2:
            job_id = parts[0].strip()
            state = parts[1].strip()
            # Handle job IDs that might have array indices (e.g., "12345_0")
            base_id = job_id.split("_")[0]
            states[base_id] = state

    return states


def get_job_details(job_id: str, user: str) -> dict:
    """Get detailed information about a job from sacct."""
    fmt = "JobID,JobName,State,ExitCode,End,CPUTimeRAW,TotalCPU,ReqMem,MaxRSS,AllocCPUS,AllocGRES,Elapsed"
    try:
        proc = subprocess.run(
            [
                "sacct",
                "-j",
                job_id,
                "-u",
                user,
                "--noheader",
                f"--format={fmt}",
                "-X",
            ],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return {}

    if proc.returncode != 0:
        return {}

    for line in proc.stdout.strip().splitlines():
        parts = [p.strip() for p in line.split("|")]
        if len(parts) != 12:
            continue

        (
            _,
            job_name,
            state,
            exit_code,
            end_time,
            cpu_time_raw,
            total_cpu,
            req_mem,
            max_rss,
            alloc_cpus,
            alloc_gres,
            elapsed,
        ) = parts

        cpu_eff = "N/A"
        mem_eff = "N/A"
        job_sus = None

        try:
            if cpu_time_raw and total_cpu and cpu_time_raw != "0":
                cpu_seconds = parse_time_to_seconds(total_cpu)
                cpu_alloc = int(cpu_time_raw)
                if cpu_alloc > 0:
                    cpu_eff = f"{min(100, (cpu_seconds / cpu_alloc) * 100):.1f}%"
        except (ValueError, ZeroDivisionError):
            pass

        try:
            if req_mem and max_rss:
                req_bytes = parse_memory(req_mem)
                used_bytes = parse_memory(max_rss)
                if req_bytes > 0:
                    mem_eff = f"{min(100, (used_bytes / req_bytes) * 100):.1f}%"
        except (ValueError, ZeroDivisionError):
            pass

        # Calculate Service Units (SUs)
        try:
            elapsed_hours = parse_time_to_seconds(elapsed) / 3600.0 if elapsed else 0
            cpus = int(alloc_cpus) if alloc_cpus else 0
            gpus = 0
            if alloc_gres:
                # Parse GPU count from GRES (e.g., "gpu:2" or "gpu:a100:4")
                for gres_part in alloc_gres.split(","):
                    if "gpu" in gres_part.lower():
                        gres_parts = gres_part.split(":")
                        gpus = int(gres_parts[-1]) if gres_parts[-1].isdigit() else 0
                        break
            # SU = CPU-hours + GPU-hours * 10
            job_sus = (cpus * elapsed_hours) + (gpus * elapsed_hours * 10)
        except (ValueError, TypeError):
            pass

        return {
            "state": state,
            "exit_code": exit_code.split(":")[0] if ":" in exit_code else exit_code,
            "cpu_eff": cpu_eff,
            "mem_eff": mem_eff,
            "end_time": end_time if end_time != "Unknown" else "N/A",
            "service_units": job_sus,
        }

    return {}


def cancel_job(job_id: str) -> tuple[bool, str]:
    """Cancel a job. Returns (success, error_message)."""
    try:
        proc = subprocess.run(
            ["scancel", job_id],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
        if proc.returncode == 0:
            return True, ""
        return False, proc.stderr or "Failed to cancel"
    except FileNotFoundError:
        return False, "scancel command not found"
    except subprocess.TimeoutExpired:
        return False, "Command timeout"


def get_job_history(user: str, days: int = 7, limit: int = 500) -> list[dict]:
    """
    Get job history from sacct for the last N days.

    Returns list of jobs with start/end times for timeline visualization.
    """
    from datetime import datetime, timedelta

    start_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%dT00:00:00")
    fmt = "JobID,JobName,State,Start,End,Elapsed,Partition"

    try:
        proc = subprocess.run(
            [
                "sacct",
                "-u", user,
                "--starttime", start_date,
                "--noheader",
                f"--format={fmt}",
                "-X",  # Only show main job, not steps
                "--parsable2",
            ],
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
    except FileNotFoundError:
        return []
    except subprocess.TimeoutExpired:
        return []

    if proc.returncode != 0:
        return []

    jobs = []
    for line in proc.stdout.strip().splitlines():
        parts = line.split("|")
        if len(parts) != 7:
            continue

        job_id, name, state, start, end, elapsed, partition = parts

        # Skip jobs without valid start time
        if not start or start == "Unknown":
            continue

        # Parse state to get base state (remove qualifiers like +)
        base_state = state.split("+")[0].upper() if state else "UNKNOWN"

        # Map states to categories for coloring
        state_category = "unknown"
        if "COMPLETED" in base_state:
            state_category = "completed"
        elif "RUNNING" in base_state:
            state_category = "running"
        elif "PENDING" in base_state:
            state_category = "pending"
        elif "FAILED" in base_state or "CANCELLED" in base_state:
            state_category = "failed"
        elif "TIMEOUT" in base_state:
            state_category = "timeout"

        jobs.append({
            "id": job_id,
            "name": name,
            "state": state,
            "state_category": state_category,
            "start": start,
            "end": end if end and end != "Unknown" else None,
            "elapsed": elapsed,
            "partition": partition,
            "log_key": f"{name}::{job_id}",
        })

        if len(jobs) >= limit:
            break

    return jobs


def get_job_resources(job_id: str) -> dict:
    """
    Get current resource usage for a running job using sstat.

    Returns CPU time, max memory, average memory, etc.
    """
    fmt = "JobID,AveCPU,AveRSS,MaxRSS,MaxVMSize,NTasks"

    try:
        proc = subprocess.run(
            [
                "sstat",
                "-j", job_id,
                "--noheader",
                f"--format={fmt}",
                "-a",  # All steps
                "--parsable2",
            ],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
    except FileNotFoundError:
        return {"error": "sstat not available"}
    except subprocess.TimeoutExpired:
        return {"error": "Command timeout"}

    if proc.returncode != 0:
        # sstat fails for completed jobs or jobs without steps
        return {"error": "No resource data available"}

    # Parse the output - take the aggregate (last row usually has totals)
    results = []
    for line in proc.stdout.strip().splitlines():
        parts = line.split("|")
        if len(parts) != 6:
            continue

        job_step, ave_cpu, ave_rss, max_rss, max_vm, ntasks = parts

        # Skip .batch entries, get main step data
        if ".batch" in job_step:
            continue

        results.append({
            "step": job_step,
            "ave_cpu": ave_cpu,
            "ave_rss": ave_rss,
            "max_rss": max_rss,
            "max_vm": max_vm,
            "ntasks": ntasks,
        })

    if not results:
        return {"error": "No resource data available"}

    # Return the last (most comprehensive) result
    return {
        "ave_cpu": results[-1]["ave_cpu"],
        "ave_rss": results[-1]["ave_rss"],
        "max_rss": results[-1]["max_rss"],
        "max_vm": results[-1]["max_vm"],
        "ntasks": results[-1]["ntasks"],
    }


def get_job_efficiency(job_id: str, user: str) -> dict:
    """
    Get efficiency metrics for a completed job using sacct.

    Returns CPU efficiency, memory efficiency, and allocation info.
    """
    fmt = "JobID,Elapsed,TotalCPU,AllocCPUS,ReqMem,MaxRSS,State,ExitCode"

    try:
        proc = subprocess.run(
            [
                "sacct",
                "-j", job_id,
                "-u", user,
                "--noheader",
                f"--format={fmt}",
                "-X",  # No job steps
                "--parsable2",
            ],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return {"error": "sacct not available"}

    if proc.returncode != 0:
        return {"error": "Failed to get efficiency data"}

    for line in proc.stdout.strip().splitlines():
        parts = line.split("|")
        if len(parts) != 8:
            continue

        job_id_part, elapsed, total_cpu, alloc_cpus, req_mem, max_rss, state, exit_code = parts

        # Calculate CPU efficiency
        cpu_eff = None
        try:
            if elapsed and total_cpu and alloc_cpus:
                elapsed_sec = parse_time_to_seconds(elapsed)
                cpu_sec = parse_time_to_seconds(total_cpu)
                cpus = int(alloc_cpus)
                if elapsed_sec > 0 and cpus > 0:
                    # CPU efficiency = actual CPU time / (wall time * allocated CPUs)
                    cpu_eff = min(100, (cpu_sec / (elapsed_sec * cpus)) * 100)
        except (ValueError, ZeroDivisionError):
            pass

        # Calculate memory efficiency
        mem_eff = None
        try:
            if req_mem and max_rss:
                req_bytes = parse_memory(req_mem)
                used_bytes = parse_memory(max_rss)
                if req_bytes > 0:
                    mem_eff = min(100, (used_bytes / req_bytes) * 100)
        except (ValueError, ZeroDivisionError):
            pass

        return {
            "elapsed": elapsed,
            "total_cpu": total_cpu,
            "alloc_cpus": alloc_cpus,
            "req_mem": req_mem,
            "max_rss": max_rss,
            "cpu_efficiency": round(cpu_eff, 1) if cpu_eff is not None else None,
            "mem_efficiency": round(mem_eff, 1) if mem_eff is not None else None,
            "state": state,
            "exit_code": exit_code.split(":")[0] if ":" in exit_code else exit_code,
        }

    return {"error": "No efficiency data found"}


def get_job_submit_info(job_id: str, user: str) -> dict:
    """
    Get submission information for a job.

    Uses scontrol for running jobs, sacct for completed jobs.
    Returns command, script path, working directory, etc.
    """
    # Try scontrol first (works for running jobs)
    try:
        proc = subprocess.run(
            ["scontrol", "show", "job", job_id],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
        if proc.returncode == 0 and proc.stdout.strip():
            info = parse_scontrol_output(proc.stdout)
            if info:
                return info
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass

    # Fall back to sacct for completed jobs
    fmt = "JobID,SubmitLine,WorkDir,JobName,Partition,Timelimit,ReqMem,ReqCPUS"
    try:
        proc = subprocess.run(
            [
                "sacct",
                "-j", job_id,
                "-u", user,
                "--noheader",
                f"--format={fmt}",
                "-X",
                "--parsable2",
            ],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return {"error": "Could not retrieve submission info"}

    if proc.returncode != 0:
        return {"error": "Could not retrieve submission info"}

    for line in proc.stdout.strip().splitlines():
        parts = line.split("|")
        if len(parts) != 8:
            continue

        job_id_part, submit_line, work_dir, job_name, partition, timelimit, req_mem, req_cpus = parts

        # Try to extract script path from submit line
        script_path = None
        if submit_line:
            # sbatch [options] script.sh -> extract script.sh
            submit_parts = submit_line.split()
            for i, part in enumerate(submit_parts):
                if not part.startswith("-") and part.endswith(".sh"):
                    script_path = part
                    break
                # Look for file after sbatch or after flags
                if i > 0 and not part.startswith("-") and not submit_parts[i-1].startswith("-"):
                    script_path = part
                    break

        return {
            "job_id": job_id,
            "submit_line": submit_line or None,
            "work_dir": work_dir or None,
            "job_name": job_name or None,
            "partition": partition or None,
            "timelimit": timelimit or None,
            "req_mem": req_mem or None,
            "req_cpus": req_cpus or None,
            "script_path": script_path,
        }

    return {"error": "No submission info found"}


def parse_scontrol_output(output: str) -> dict:
    """Parse scontrol show job output into a dictionary."""
    info = {}

    # Parse key=value pairs
    for line in output.split("\n"):
        for item in line.split():
            if "=" in item:
                key, _, value = item.partition("=")
                info[key] = value

    if not info:
        return None

    # Extract script path from Command field
    script_path = info.get("Command")

    return {
        "job_id": info.get("JobId"),
        "submit_line": info.get("Command"),
        "work_dir": info.get("WorkDir"),
        "job_name": info.get("JobName"),
        "partition": info.get("Partition"),
        "timelimit": info.get("TimeLimit"),
        "req_mem": info.get("MinMemoryNode"),
        "req_cpus": info.get("NumCPUs"),
        "script_path": script_path,
        "user": info.get("UserId"),
        "state": info.get("JobState"),
    }


def get_script_content(script_path: str, max_lines: int = 200) -> dict:
    """
    Read the content of a job submission script.

    Returns the script content if accessible, error otherwise.
    Security: Only reads if path exists and is a regular file.
    """
    if not script_path:
        return {"error": "No script path provided"}

    script = Path(script_path)

    # Security checks
    if not script.exists():
        return {"error": "Script file not found"}

    if not script.is_file():
        return {"error": "Not a regular file"}

    # Check file size (limit to 1MB)
    try:
        size = script.stat().st_size
        if size > 1024 * 1024:
            return {"error": "Script file too large"}
    except OSError:
        return {"error": "Cannot access script file"}

    try:
        with script.open("r", errors="replace") as f:
            lines = f.readlines()[:max_lines]
            content = "".join(lines)
            truncated = len(lines) == max_lines

        return {
            "content": content,
            "path": str(script),
            "truncated": truncated,
        }
    except OSError as e:
        return {"error": f"Cannot read script: {e}"}


def resubmit_job(
    script_path: str,
    work_dir: Optional[str] = None,
    partition: Optional[str] = None,
    time_limit: Optional[str] = None,
    memory: Optional[str] = None,
    cpus: Optional[int] = None,
) -> dict:
    """
    Resubmit a job with optional parameter overrides.

    Returns the new job ID on success, error on failure.
    """
    if not script_path:
        return {"error": "No script path provided"}

    script = Path(script_path)
    if not script.exists() or not script.is_file():
        return {"error": "Script file not found"}

    # Build sbatch command
    cmd = ["sbatch"]

    if work_dir:
        cmd.extend(["--chdir", work_dir])
    if partition:
        cmd.extend(["--partition", partition])
    if time_limit:
        cmd.extend(["--time", time_limit])
    if memory:
        cmd.extend(["--mem", memory])
    if cpus:
        cmd.extend(["--cpus-per-task", str(cpus)])

    cmd.append(str(script))

    try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=30,
            cwd=work_dir,
            check=False,
        )
    except FileNotFoundError:
        return {"error": "sbatch command not found"}
    except subprocess.TimeoutExpired:
        return {"error": "Submission timed out"}

    if proc.returncode != 0:
        return {"error": proc.stderr.strip() or "Submission failed"}

    # Parse job ID from output ("Submitted batch job 12345")
    output = proc.stdout.strip()
    match = re.search(r"Submitted batch job (\d+)", output)
    if match:
        return {"job_id": match.group(1), "message": output}

    return {"error": "Could not parse job ID", "output": output}


def get_queue_info(user: str) -> dict:
    """
    Get queue position and wait time estimates for pending jobs.

    Returns queue depth, position for each pending job, and average wait times.
    """
    from datetime import datetime, timedelta

    # Get all pending jobs with their queue info
    fmt = "%i|%j|%T|%r|%Q|%S|%P"  # JobID, Name, State, Reason, Priority, StartTime, Partition
    try:
        proc = subprocess.run(
            ["squeue", "-u", user, "--noheader", f"--format={fmt}"],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return {"error": "squeue not available"}

    if proc.returncode != 0:
        return {"error": "Failed to get queue info"}

    pending_jobs = {}
    for line in proc.stdout.strip().splitlines():
        parts = line.split("|", maxsplit=6)
        if len(parts) != 7:
            continue

        job_id, name, state, reason, priority, start_time, partition = parts

        # Only process pending jobs
        if "PENDING" not in state.upper():
            continue

        pending_jobs[job_id] = {
            "reason": reason,
            "priority": priority,
            "expected_start": start_time if start_time and start_time != "N/A" else None,
            "partition": partition,
        }

    if not pending_jobs:
        return {"pending_jobs": {}}

    # Get historical wait times for estimation
    avg_wait = get_historical_wait_time(user, days=7)

    # Get queue position per partition
    queue_positions = get_queue_positions(user)

    # Combine info
    result = {"pending_jobs": {}}
    for job_id, info in pending_jobs.items():
        partition = info["partition"]
        position = queue_positions.get(job_id, {})

        # Estimate wait time
        estimated_wait = None
        confidence = "low"

        if info["expected_start"] and info["expected_start"] != "N/A":
            # Slurm provided an expected start time
            try:
                start = datetime.strptime(info["expected_start"], "%Y-%m-%dT%H:%M:%S")
                wait_seconds = (start - datetime.now()).total_seconds()
                if wait_seconds > 0:
                    estimated_wait = format_duration(int(wait_seconds))
                    confidence = "high"
            except ValueError:
                pass

        if not estimated_wait and avg_wait.get(partition):
            # Use historical average
            estimated_wait = format_duration(int(avg_wait[partition]))
            confidence = "medium"

        result["pending_jobs"][job_id] = {
            "reason": info["reason"],
            "partition": partition,
            "queue_position": position.get("position"),
            "queue_size": position.get("total"),
            "estimated_wait": estimated_wait,
            "confidence": confidence,
        }

    return result


def get_queue_positions(user: str) -> dict:
    """Get queue position for each pending job per partition."""
    # Get all pending jobs in queue (all users) to determine position
    try:
        proc = subprocess.run(
            ["squeue", "--state=PENDING", "--noheader", "--format=%i|%u|%P|%Q"],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return {}

    if proc.returncode != 0:
        return {}

    # Group by partition and sort by priority
    partitions = {}
    for line in proc.stdout.strip().splitlines():
        parts = line.split("|")
        if len(parts) != 4:
            continue
        job_id, job_user, partition, priority = parts
        if partition not in partitions:
            partitions[partition] = []
        try:
            partitions[partition].append((job_id, job_user, int(priority)))
        except ValueError:
            partitions[partition].append((job_id, job_user, 0))

    # Sort each partition by priority (higher = earlier in queue)
    positions = {}
    for partition, jobs in partitions.items():
        jobs.sort(key=lambda x: x[2], reverse=True)
        total = len(jobs)
        for i, (job_id, job_user, _) in enumerate(jobs):
            if job_user == user:
                positions[job_id] = {
                    "position": i + 1,
                    "total": total,
                    "partition": partition,
                }

    return positions


def get_historical_wait_time(user: str, days: int = 7) -> dict:
    """Get average historical wait time per partition."""
    from datetime import datetime, timedelta

    start_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%dT00:00:00")

    try:
        proc = subprocess.run(
            [
                "sacct",
                "-u", user,
                "--starttime", start_date,
                "--noheader",
                "--format=JobID,Partition,Submit,Start",
                "-X",
                "--parsable2",
            ],
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return {}

    if proc.returncode != 0:
        return {}

    # Calculate average wait time per partition
    wait_times = {}  # partition -> list of wait seconds
    for line in proc.stdout.strip().splitlines():
        parts = line.split("|")
        if len(parts) != 4:
            continue

        job_id, partition, submit_time, start_time = parts

        if not submit_time or not start_time or submit_time == "Unknown" or start_time == "Unknown":
            continue

        try:
            submit = datetime.strptime(submit_time, "%Y-%m-%dT%H:%M:%S")
            start = datetime.strptime(start_time, "%Y-%m-%dT%H:%M:%S")
            wait_seconds = (start - submit).total_seconds()
            if wait_seconds >= 0:
                if partition not in wait_times:
                    wait_times[partition] = []
                wait_times[partition].append(wait_seconds)
        except ValueError:
            continue

    # Calculate averages
    averages = {}
    for partition, times in wait_times.items():
        if times:
            averages[partition] = sum(times) / len(times)

    return averages


def format_duration(seconds: int) -> str:
    """Format seconds into human-readable duration."""
    if seconds < 60:
        return f"{seconds}s"
    elif seconds < 3600:
        return f"{seconds // 60}m"
    elif seconds < 86400:
        hours = seconds // 3600
        mins = (seconds % 3600) // 60
        return f"{hours}h {mins}m" if mins else f"{hours}h"
    else:
        days = seconds // 86400
        hours = (seconds % 86400) // 3600
        return f"{days}d {hours}h" if hours else f"{days}d"


def get_job_dependencies(user: str) -> dict:
    """
    Get job dependency information for building a DAG visualization.

    Returns a dict with:
        - nodes: List of jobs with their state and metadata
        - edges: List of dependency relationships between jobs
        - pipelines: Grouped jobs that form connected pipelines
    """
    import re

    # Get all jobs (running, pending, and recent completed) with dependency info
    # Format: JobID|Name|State|Dependency|StartTime|EndTime|Partition
    fmt = "%i|%j|%T|%E|%S|%e|%P"
    result = subprocess.run(
        ["squeue", "-u", user, "-o", fmt, "--noheader"],
        capture_output=True,
        text=True,
        timeout=10,
    )

    nodes = {}
    edges = []
    job_to_deps = {}

    # Parse current queue jobs
    for line in result.stdout.strip().split("\n"):
        if not line.strip():
            continue
        parts = line.split("|")
        if len(parts) >= 7:
            job_id = parts[0].strip()
            # Handle array jobs - extract base job ID
            base_job_id = job_id.split("_")[0]

            nodes[base_job_id] = {
                "id": base_job_id,
                "name": parts[1].strip(),
                "state": parts[2].strip(),
                "state_category": _categorize_state(parts[2].strip()),
                "dependency_str": parts[3].strip(),
                "start_time": parts[4].strip() if parts[4].strip() != "N/A" else None,
                "end_time": parts[5].strip() if parts[5].strip() != "N/A" else None,
                "partition": parts[6].strip(),
            }

            # Parse dependency string
            dep_str = parts[3].strip()
            if dep_str and dep_str not in ("(null)", ""):
                deps = _parse_dependency_string(dep_str)
                job_to_deps[base_job_id] = deps

    # Also get recently completed jobs to show full pipeline
    try:
        sacct_result = subprocess.run(
            [
                "sacct",
                "-u",
                user,
                "--starttime=now-2days",
                "--format=JobID,JobName,State,Start,End,Partition",
                "--parsable2",
                "--noheader",
            ],
            capture_output=True,
            text=True,
            timeout=15,
        )

        for line in sacct_result.stdout.strip().split("\n"):
            if not line.strip():
                continue
            parts = line.split("|")
            if len(parts) >= 6:
                job_id = parts[0].strip()
                # Skip batch/extern steps
                if "." in job_id or "batch" in job_id or "extern" in job_id:
                    continue
                # Handle array jobs
                base_job_id = job_id.split("_")[0]

                if base_job_id not in nodes:
                    state = parts[2].strip()
                    nodes[base_job_id] = {
                        "id": base_job_id,
                        "name": parts[1].strip(),
                        "state": state,
                        "state_category": _categorize_state(state),
                        "dependency_str": "",
                        "start_time": parts[3].strip() if parts[3].strip() else None,
                        "end_time": parts[4].strip() if parts[4].strip() else None,
                        "partition": parts[5].strip() if len(parts) > 5 else "",
                    }
    except (subprocess.TimeoutExpired, subprocess.SubprocessError):
        pass

    # Build edges from dependency info
    for job_id, deps in job_to_deps.items():
        for dep_job_id, dep_type in deps:
            if dep_job_id in nodes:
                edges.append({
                    "from": dep_job_id,
                    "to": job_id,
                    "type": dep_type,
                })

    # Group into connected pipelines
    pipelines = _find_connected_pipelines(nodes, edges)

    # Calculate progress for each pipeline
    for pipeline in pipelines:
        completed = sum(
            1 for jid in pipeline["job_ids"] if nodes.get(jid, {}).get("state_category") == "completed"
        )
        total = len(pipeline["job_ids"])
        pipeline["progress"] = round((completed / total) * 100) if total > 0 else 0
        pipeline["completed"] = completed
        pipeline["total"] = total

    return {
        "nodes": list(nodes.values()),
        "edges": edges,
        "pipelines": pipelines,
    }


def _parse_dependency_string(dep_str: str) -> list:
    """
    Parse Slurm dependency string into list of (job_id, type) tuples.

    Examples:
        "afterok:12345" -> [("12345", "afterok")]
        "afterok:123,afterok:456" -> [("123", "afterok"), ("456", "afterok")]
        "afterany:123:456" -> [("123", "afterany"), ("456", "afterany")]
    """
    import re

    deps = []
    if not dep_str or dep_str in ("(null)", "(dependency)"):
        return deps

    # Handle different dependency formats
    # Format 1: type:jobid,type:jobid
    # Format 2: type:jobid:jobid:jobid
    patterns = [
        r"(after\w*):(\d+)",  # afterok:123, afterany:456, etc.
        r"(singleton)",  # singleton dependency
    ]

    for pattern in patterns:
        matches = re.findall(pattern, dep_str)
        for match in matches:
            if isinstance(match, tuple):
                dep_type, job_id = match[0], match[1] if len(match) > 1 else None
                if job_id:
                    deps.append((job_id, dep_type))
            else:
                # singleton case
                deps.append((None, match))

    return deps


def _categorize_state(state: str) -> str:
    """Categorize job state for visualization."""
    s = state.upper()
    if "RUNNING" in s:
        return "running"
    if "PENDING" in s or "CONFIGURING" in s:
        return "pending"
    if "COMPLETED" in s:
        return "completed"
    if "FAILED" in s or "CANCELLED" in s or "TIMEOUT" in s or "NODE_FAIL" in s:
        return "failed"
    return "pending"


def _find_connected_pipelines(nodes: dict, edges: list) -> list:
    """Find connected components in the job dependency graph."""
    from collections import defaultdict

    # Build adjacency list (undirected for finding components)
    adj = defaultdict(set)
    for edge in edges:
        adj[edge["from"]].add(edge["to"])
        adj[edge["to"]].add(edge["from"])

    # Find connected components using DFS
    visited = set()
    pipelines = []

    def dfs(node, component):
        if node in visited:
            return
        visited.add(node)
        component.add(node)
        for neighbor in adj[node]:
            dfs(neighbor, component)

    # Only consider jobs that have dependencies
    jobs_with_deps = set()
    for edge in edges:
        jobs_with_deps.add(edge["from"])
        jobs_with_deps.add(edge["to"])

    for job_id in jobs_with_deps:
        if job_id not in visited and job_id in nodes:
            component = set()
            dfs(job_id, component)
            if len(component) > 1:  # Only include if it's actually a pipeline
                # Determine pipeline name from the jobs
                job_names = [nodes[jid]["name"] for jid in component if jid in nodes]
                # Find common prefix or use first job name
                pipeline_name = _find_common_prefix(job_names) or job_names[0] if job_names else "Pipeline"

                pipelines.append({
                    "name": pipeline_name,
                    "job_ids": list(component),
                })

    return pipelines


def _find_common_prefix(strings: list) -> str:
    """Find common prefix among job names."""
    if not strings:
        return ""
    if len(strings) == 1:
        return strings[0]

    prefix = strings[0]
    for s in strings[1:]:
        while not s.startswith(prefix) and prefix:
            prefix = prefix[:-1]
    # Clean up prefix - remove trailing numbers, underscores, hyphens
    import re

    prefix = re.sub(r"[-_\d]+$", "", prefix)
    return prefix.strip() if len(prefix) >= 3 else ""


def get_job_insights(user: str, days: int = 30) -> dict:
    """
    Analyze historical job data to provide insights and recommendations.

    Returns:
        - memory_insights: Memory usage patterns and recommendations
        - time_insights: Runtime patterns and recommendations
        - failure_patterns: Common failure modes detected
        - efficiency_score: Overall efficiency metrics
        - predictions: Completion time predictions for running jobs
    """
    from datetime import datetime, timedelta

    insights = {
        "memory_insights": None,
        "time_insights": None,
        "failure_patterns": [],
        "efficiency_score": None,
        "job_stats": None,
    }

    # Get historical job data with resource info
    try:
        result = subprocess.run(
            [
                "sacct",
                "-u",
                user,
                f"--starttime=now-{days}days",
                "--format=JobID,JobName,State,Elapsed,ReqMem,MaxRSS,ReqCPUS,TotalCPU,Timelimit,Partition,ExitCode",
                "--parsable2",
                "--noheader",
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )
    except (subprocess.TimeoutExpired, subprocess.SubprocessError):
        return insights

    jobs = []
    for line in result.stdout.strip().split("\n"):
        if not line.strip():
            continue
        parts = line.split("|")
        if len(parts) >= 11:
            job_id = parts[0].strip()
            # Skip batch/extern steps, only look at main job entries
            if "." in job_id or "batch" in job_id or "extern" in job_id:
                continue

            jobs.append({
                "job_id": job_id.split("_")[0],  # Handle array jobs
                "name": parts[1].strip(),
                "state": parts[2].strip(),
                "elapsed": parts[3].strip(),
                "req_mem": parts[4].strip(),
                "max_rss": parts[5].strip(),
                "req_cpus": parts[6].strip(),
                "total_cpu": parts[7].strip(),
                "timelimit": parts[8].strip(),
                "partition": parts[9].strip(),
                "exit_code": parts[10].strip(),
            })

    if not jobs:
        return insights

    # Calculate statistics
    insights["job_stats"] = _calculate_job_stats(jobs)
    insights["memory_insights"] = _analyze_memory_usage(jobs)
    insights["time_insights"] = _analyze_time_usage(jobs)
    insights["failure_patterns"] = _detect_failure_patterns(jobs)
    insights["efficiency_score"] = _calculate_efficiency_score(jobs)

    return insights


def _parse_memory_to_bytes(mem_str: str) -> int:
    """Parse memory string like '4G', '512M', '4096K' to bytes."""
    if not mem_str or mem_str in ("", "0", "N/A"):
        return 0

    import re
    # Handle formats like "4G", "4Gn", "4Gc", "4000M", etc.
    match = re.match(r"([\d.]+)\s*([KMGT])?", mem_str.upper())
    if not match:
        return 0

    value = float(match.group(1))
    unit = match.group(2) or "K"  # Default to KB if no unit

    multipliers = {"K": 1024, "M": 1024**2, "G": 1024**3, "T": 1024**4}
    return int(value * multipliers.get(unit, 1024))


def _parse_time_to_seconds(time_str: str) -> int:
    """Parse time string like '1-02:30:00', '02:30:00', '30:00' to seconds."""
    if not time_str or time_str in ("", "UNLIMITED", "N/A"):
        return 0

    try:
        parts = time_str.replace("-", ":").split(":")
        parts = [int(p) for p in parts]

        if len(parts) == 4:  # days-hours:mins:secs
            return parts[0] * 86400 + parts[1] * 3600 + parts[2] * 60 + parts[3]
        elif len(parts) == 3:  # hours:mins:secs
            return parts[0] * 3600 + parts[1] * 60 + parts[2]
        elif len(parts) == 2:  # mins:secs
            return parts[0] * 60 + parts[1]
        else:
            return int(parts[0])
    except (ValueError, IndexError):
        return 0


def _calculate_job_stats(jobs: list) -> dict:
    """Calculate basic job statistics."""
    total = len(jobs)
    completed = sum(1 for j in jobs if "COMPLETED" in j["state"].upper())
    failed = sum(1 for j in jobs if "FAILED" in j["state"].upper())
    cancelled = sum(1 for j in jobs if "CANCELLED" in j["state"].upper())
    timeout = sum(1 for j in jobs if "TIMEOUT" in j["state"].upper())

    return {
        "total_jobs": total,
        "completed": completed,
        "failed": failed,
        "cancelled": cancelled,
        "timeout": timeout,
        "success_rate": round((completed / total) * 100, 1) if total > 0 else 0,
    }


def _analyze_memory_usage(jobs: list) -> dict:
    """Analyze memory usage patterns and generate recommendations."""
    memory_data = []

    for job in jobs:
        if "COMPLETED" not in job["state"].upper():
            continue

        req_mem = _parse_memory_to_bytes(job["req_mem"])
        max_rss = _parse_memory_to_bytes(job["max_rss"])

        if req_mem > 0 and max_rss > 0:
            efficiency = (max_rss / req_mem) * 100
            memory_data.append({
                "req_mem": req_mem,
                "max_rss": max_rss,
                "efficiency": efficiency,
            })

    if not memory_data:
        return None

    avg_efficiency = sum(d["efficiency"] for d in memory_data) / len(memory_data)
    median_req = sorted(d["req_mem"] for d in memory_data)[len(memory_data) // 2]
    median_used = sorted(d["max_rss"] for d in memory_data)[len(memory_data) // 2]

    # Generate recommendation
    recommendation = None
    if avg_efficiency < 50:
        suggested = _format_bytes(int(median_used * 1.3))  # 30% headroom
        current_median = _format_bytes(median_req)
        recommendation = f"You typically use {avg_efficiency:.0f}% of requested memory. Consider requesting {suggested} instead of {current_median}."

    return {
        "avg_efficiency": round(avg_efficiency, 1),
        "sample_count": len(memory_data),
        "median_requested": _format_bytes(median_req),
        "median_used": _format_bytes(median_used),
        "recommendation": recommendation,
    }


def _analyze_time_usage(jobs: list) -> dict:
    """Analyze time limit usage patterns and generate recommendations."""
    time_data = []

    for job in jobs:
        if "COMPLETED" not in job["state"].upper():
            continue

        elapsed = _parse_time_to_seconds(job["elapsed"])
        timelimit = _parse_time_to_seconds(job["timelimit"])

        if elapsed > 0 and timelimit > 0:
            efficiency = (elapsed / timelimit) * 100
            time_data.append({
                "elapsed": elapsed,
                "timelimit": timelimit,
                "efficiency": efficiency,
            })

    if not time_data:
        return None

    avg_efficiency = sum(d["efficiency"] for d in time_data) / len(time_data)
    p90_elapsed = sorted(d["elapsed"] for d in time_data)[int(len(time_data) * 0.9)]
    median_limit = sorted(d["timelimit"] for d in time_data)[len(time_data) // 2]

    # Generate recommendation
    recommendation = None
    if avg_efficiency < 30:
        suggested = format_duration(int(p90_elapsed * 1.2))  # 20% headroom over p90
        current = format_duration(median_limit)
        recommendation = f"90% of your jobs complete in under {format_duration(p90_elapsed)}. Consider requesting {suggested} instead of {current}."

    return {
        "avg_efficiency": round(avg_efficiency, 1),
        "sample_count": len(time_data),
        "p90_runtime": format_duration(p90_elapsed),
        "median_limit": format_duration(median_limit),
        "recommendation": recommendation,
    }


def _detect_failure_patterns(jobs: list) -> list:
    """Detect recurring failure patterns in job history."""
    from collections import defaultdict

    patterns = []

    # Group failures by partition
    partition_failures = defaultdict(lambda: {"failed": 0, "total": 0})
    for job in jobs:
        partition = job["partition"]
        partition_failures[partition]["total"] += 1
        if "FAILED" in job["state"].upper() or "TIMEOUT" in job["state"].upper():
            partition_failures[partition]["failed"] += 1

    for partition, counts in partition_failures.items():
        if counts["total"] >= 5:  # Need at least 5 jobs for significance
            failure_rate = (counts["failed"] / counts["total"]) * 100
            if failure_rate > 20:  # More than 20% failure rate
                patterns.append({
                    "type": "partition_failures",
                    "partition": partition,
                    "failure_rate": round(failure_rate, 1),
                    "sample_size": counts["total"],
                    "message": f"Jobs on partition '{partition}' have a {failure_rate:.0f}% failure rate ({counts['failed']}/{counts['total']})",
                })

    # Group failures by job name pattern
    name_failures = defaultdict(lambda: {"failed": 0, "total": 0, "names": set()})
    for job in jobs:
        # Extract base name (remove numbers and common suffixes)
        import re
        base_name = re.sub(r"[-_]?\d+$", "", job["name"])
        if len(base_name) >= 3:
            name_failures[base_name]["total"] += 1
            name_failures[base_name]["names"].add(job["name"])
            if "FAILED" in job["state"].upper() or "TIMEOUT" in job["state"].upper():
                name_failures[base_name]["failed"] += 1

    for base_name, counts in name_failures.items():
        if counts["total"] >= 3 and counts["failed"] >= 2:
            failure_rate = (counts["failed"] / counts["total"]) * 100
            if failure_rate > 30:
                patterns.append({
                    "type": "name_pattern_failures",
                    "pattern": f"{base_name}*",
                    "failure_rate": round(failure_rate, 1),
                    "sample_size": counts["total"],
                    "message": f"Jobs matching '{base_name}*' have a {failure_rate:.0f}% failure rate",
                })

    # Detect timeout patterns
    timeout_count = sum(1 for j in jobs if "TIMEOUT" in j["state"].upper())
    if timeout_count >= 3 and len(jobs) >= 10:
        timeout_rate = (timeout_count / len(jobs)) * 100
        if timeout_rate > 10:
            patterns.append({
                "type": "timeout_rate",
                "timeout_rate": round(timeout_rate, 1),
                "count": timeout_count,
                "message": f"{timeout_rate:.0f}% of your jobs are timing out. Consider increasing time limits or optimizing code.",
            })

    return patterns


def _calculate_efficiency_score(jobs: list) -> dict:
    """Calculate overall efficiency score based on resource utilization."""
    completed_jobs = [j for j in jobs if "COMPLETED" in j["state"].upper()]

    if not completed_jobs:
        return None

    # Memory efficiency
    mem_efficiencies = []
    for job in completed_jobs:
        req_mem = _parse_memory_to_bytes(job["req_mem"])
        max_rss = _parse_memory_to_bytes(job["max_rss"])
        if req_mem > 0 and max_rss > 0:
            mem_efficiencies.append(min(100, (max_rss / req_mem) * 100))

    # Time efficiency
    time_efficiencies = []
    for job in completed_jobs:
        elapsed = _parse_time_to_seconds(job["elapsed"])
        timelimit = _parse_time_to_seconds(job["timelimit"])
        if elapsed > 0 and timelimit > 0:
            time_efficiencies.append(min(100, (elapsed / timelimit) * 100))

    # Calculate overall score (weighted average)
    scores = []
    if mem_efficiencies:
        scores.append(sum(mem_efficiencies) / len(mem_efficiencies))
    if time_efficiencies:
        scores.append(sum(time_efficiencies) / len(time_efficiencies))

    overall = sum(scores) / len(scores) if scores else 0

    # Determine grade
    if overall >= 70:
        grade = "A"
        label = "Excellent"
    elif overall >= 50:
        grade = "B"
        label = "Good"
    elif overall >= 30:
        grade = "C"
        label = "Fair"
    else:
        grade = "D"
        label = "Needs Improvement"

    return {
        "overall_score": round(overall, 1),
        "grade": grade,
        "label": label,
        "memory_efficiency": round(sum(mem_efficiencies) / len(mem_efficiencies), 1) if mem_efficiencies else None,
        "time_efficiency": round(sum(time_efficiencies) / len(time_efficiencies), 1) if time_efficiencies else None,
        "jobs_analyzed": len(completed_jobs),
    }


def _format_bytes(bytes_val: int) -> str:
    """Format bytes to human-readable string."""
    if bytes_val >= 1024**3:
        return f"{bytes_val / 1024**3:.1f}G"
    elif bytes_val >= 1024**2:
        return f"{bytes_val / 1024**2:.0f}M"
    elif bytes_val >= 1024:
        return f"{bytes_val / 1024:.0f}K"
    return f"{bytes_val}B"


def predict_job_completion(job_id: str, user: str) -> dict:
    """
    Predict completion time for a running job based on similar historical jobs.

    Returns estimated remaining time and confidence level.
    """
    # Get current job info
    try:
        result = subprocess.run(
            ["squeue", "-j", job_id, "-o", "%j|%M|%l|%P", "--noheader"],
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (subprocess.TimeoutExpired, subprocess.SubprocessError):
        return {"error": "Could not fetch job info"}

    if not result.stdout.strip():
        return {"error": "Job not found or not running"}

    parts = result.stdout.strip().split("|")
    if len(parts) < 4:
        return {"error": "Invalid job info"}

    job_name = parts[0].strip()
    current_runtime = _parse_time_to_seconds(parts[1].strip())
    time_limit = _parse_time_to_seconds(parts[2].strip())
    partition = parts[3].strip()

    # Get historical data for similar jobs
    try:
        hist_result = subprocess.run(
            [
                "sacct",
                "-u",
                user,
                "--starttime=now-30days",
                "--format=JobName,Elapsed,State,Partition",
                "--parsable2",
                "--noheader",
                "--state=COMPLETED",
            ],
            capture_output=True,
            text=True,
            timeout=15,
        )
    except (subprocess.TimeoutExpired, subprocess.SubprocessError):
        return {"error": "Could not fetch historical data"}

    # Find similar jobs (same name pattern or partition)
    import re
    base_name = re.sub(r"[-_]?\d+$", "", job_name)
    similar_runtimes = []

    for line in hist_result.stdout.strip().split("\n"):
        if not line.strip():
            continue
        parts = line.split("|")
        if len(parts) >= 4:
            hist_name = parts[0].strip()
            hist_elapsed = _parse_time_to_seconds(parts[1].strip())
            hist_partition = parts[3].strip()

            # Match by name pattern
            if hist_name.startswith(base_name) and hist_elapsed > 0:
                similar_runtimes.append(hist_elapsed)
            # Or same partition (weaker match)
            elif hist_partition == partition and hist_elapsed > 0:
                similar_runtimes.append(hist_elapsed)

    if len(similar_runtimes) < 3:
        return {
            "error": "Insufficient historical data",
            "current_runtime": format_duration(current_runtime),
            "time_limit": format_duration(time_limit),
        }

    # Calculate prediction
    median_runtime = sorted(similar_runtimes)[len(similar_runtimes) // 2]
    p90_runtime = sorted(similar_runtimes)[int(len(similar_runtimes) * 0.9)]

    estimated_remaining = max(0, median_runtime - current_runtime)
    estimated_total = median_runtime

    # Determine confidence
    if len(similar_runtimes) >= 10:
        confidence = "high"
    elif len(similar_runtimes) >= 5:
        confidence = "medium"
    else:
        confidence = "low"

    # Calculate progress
    progress = min(100, (current_runtime / median_runtime) * 100) if median_runtime > 0 else 0

    return {
        "estimated_remaining": format_duration(estimated_remaining),
        "estimated_total": format_duration(estimated_total),
        "progress_percent": round(progress, 1),
        "confidence": confidence,
        "sample_size": len(similar_runtimes),
        "current_runtime": format_duration(current_runtime),
        "time_limit": format_duration(time_limit),
    }


def submit_job(
    script_path: str = None,
    script_content: str = None,
    job_name: str = None,
    partition: str = None,
    time_limit: str = None,
    memory: str = None,
    cpus: int = None,
    gpus: int = None,
    work_dir: str = None,
    output_file: str = None,
    error_file: str = None,
    dependency: str = None,
    environment: dict = None,
) -> dict:
    """
    Submit a new job to Slurm.

    Either script_path or script_content must be provided.
    If script_content is provided, a temporary script file is created.

    Returns:
        - success: True if job was submitted
        - job_id: The new job ID
        - error: Error message if failed
    """
    import os
    import tempfile

    # Build sbatch command
    cmd = ["sbatch"]

    if job_name:
        cmd.extend(["--job-name", job_name])
    if partition:
        cmd.extend(["--partition", partition])
    if time_limit:
        cmd.extend(["--time", time_limit])
    if memory:
        cmd.extend(["--mem", memory])
    if cpus:
        cmd.extend(["--cpus-per-task", str(cpus)])
    if gpus:
        cmd.extend(["--gpus", str(gpus)])
    if work_dir:
        cmd.extend(["--chdir", work_dir])
    if output_file:
        cmd.extend(["--output", output_file])
    if error_file:
        cmd.extend(["--error", error_file])
    if dependency:
        cmd.extend(["--dependency", dependency])

    # Set up environment
    env = os.environ.copy()
    if environment:
        env.update(environment)

    temp_script = None
    try:
        # Determine script to run
        if script_content:
            # Create temporary script file
            temp_script = tempfile.NamedTemporaryFile(
                mode='w',
                suffix='.sh',
                delete=False
            )
            temp_script.write(script_content)
            temp_script.close()
            os.chmod(temp_script.name, 0o755)
            cmd.append(temp_script.name)
        elif script_path:
            # Validate script path
            script_path = os.path.expanduser(script_path)
            if not os.path.isfile(script_path):
                return {"success": False, "error": f"Script not found: {script_path}"}
            cmd.append(script_path)
        else:
            return {"success": False, "error": "No script provided"}

        # Submit the job
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=30,
            env=env,
            cwd=work_dir if work_dir else None,
        )

        if result.returncode != 0:
            return {
                "success": False,
                "error": result.stderr.strip() or "sbatch failed",
            }

        # Parse job ID from output (format: "Submitted batch job 12345")
        output = result.stdout.strip()
        import re
        match = re.search(r"Submitted batch job (\d+)", output)
        if match:
            return {
                "success": True,
                "job_id": match.group(1),
                "message": output,
            }
        else:
            return {
                "success": True,
                "job_id": None,
                "message": output,
            }

    except subprocess.TimeoutExpired:
        return {"success": False, "error": "Job submission timed out"}
    except subprocess.SubprocessError as e:
        return {"success": False, "error": str(e)}
    finally:
        # Clean up temporary script
        if temp_script and os.path.exists(temp_script.name):
            os.unlink(temp_script.name)


def get_available_partitions() -> list:
    """Get list of available partitions from Slurm."""
    try:
        result = subprocess.run(
            ["sinfo", "-h", "-o", "%P"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        if result.returncode == 0:
            partitions = []
            for line in result.stdout.strip().split("\n"):
                # Remove the '*' from default partition
                partition = line.strip().rstrip("*")
                if partition:
                    partitions.append(partition)
            return partitions
    except (subprocess.TimeoutExpired, subprocess.SubprocessError):
        pass
    return []


def get_cost_data(user: str, days: int = 30) -> dict:
    """
    Get cost/allocation usage data for the user.

    Returns:
        - total_sus: Total service units consumed
        - daily_usage: List of daily usage
        - top_jobs: Most expensive jobs
        - by_partition: Usage breakdown by partition
        - projections: Estimated usage by end of period
    """
    from datetime import datetime, timedelta
    from collections import defaultdict

    # Fetch job history with resource info
    try:
        result = subprocess.run(
            [
                "sacct",
                "-u",
                user,
                f"--starttime=now-{days}days",
                "--format=JobID,JobName,Partition,AllocCPUS,AllocGRES,Elapsed,State,Start",
                "--parsable2",
                "--noheader",
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )
    except (subprocess.TimeoutExpired, subprocess.SubprocessError):
        return {"error": "Could not fetch job history"}

    # Parse jobs and calculate costs
    jobs_data = []
    daily_usage = defaultdict(float)
    partition_usage = defaultdict(float)

    for line in result.stdout.strip().split("\n"):
        if not line.strip():
            continue

        parts = line.split("|")
        if len(parts) < 8:
            continue

        job_id = parts[0].strip()
        # Skip step entries
        if "." in job_id:
            continue

        job_name = parts[1].strip()
        partition = parts[2].strip()
        try:
            cpus = int(parts[3].strip()) if parts[3].strip() else 0
        except ValueError:
            cpus = 0

        gres = parts[4].strip()
        elapsed_str = parts[5].strip()
        state = parts[6].strip()
        start_str = parts[7].strip()

        # Parse GPUs from GRES (format: gpu:2 or gpu:a100:4)
        gpus = 0
        if gres:
            import re
            gpu_match = re.search(r'gpu[^:]*:(\d+)', gres)
            if gpu_match:
                gpus = int(gpu_match.group(1))

        # Parse elapsed time to hours
        elapsed_hours = _parse_time_to_seconds(elapsed_str) / 3600

        # Calculate service units (SUs)
        # Formula: CPU-hours + GPU-hours * 10 (GPUs are more expensive)
        gpu_multiplier = 10
        sus = (cpus * elapsed_hours) + (gpus * elapsed_hours * gpu_multiplier)

        if sus <= 0:
            continue

        # Parse start date
        if start_str and start_str not in ("Unknown", "None", "N/A"):
            try:
                start_dt = datetime.strptime(start_str[:10], "%Y-%m-%d")
                date_key = start_dt.strftime("%Y-%m-%d")
                daily_usage[date_key] += sus
            except ValueError:
                pass

        partition_usage[partition] += sus

        jobs_data.append({
            "job_id": job_id,
            "name": job_name,
            "partition": partition,
            "cpus": cpus,
            "gpus": gpus,
            "elapsed_hours": round(elapsed_hours, 2),
            "sus": round(sus, 1),
            "state": state,
        })

    # Sort jobs by SU cost
    jobs_data.sort(key=lambda x: x["sus"], reverse=True)
    top_jobs = jobs_data[:10]

    # Calculate totals
    total_sus = sum(j["sus"] for j in jobs_data)

    # Fill in missing dates
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    current = start_date
    daily_list = []

    while current <= end_date:
        date_key = current.strftime("%Y-%m-%d")
        daily_list.append({
            "date": date_key,
            "sus": round(daily_usage.get(date_key, 0), 1),
        })
        current += timedelta(days=1)

    # Calculate projections
    active_days = len([d for d in daily_list if d["sus"] > 0])
    if active_days > 0:
        daily_avg = total_sus / active_days
        # Project to end of month
        days_remaining = (end_date.replace(day=28) + timedelta(days=4)).replace(day=1) - end_date
        days_remaining = days_remaining.days
        projected_additional = daily_avg * days_remaining
        projected_total = total_sus + projected_additional
    else:
        daily_avg = 0
        projected_total = 0

    # Partition breakdown
    partition_list = [
        {"partition": p, "sus": round(s, 1)}
        for p, s in sorted(partition_usage.items(), key=lambda x: x[1], reverse=True)
    ]

    return {
        "total_sus": round(total_sus, 1),
        "daily_usage": daily_list,
        "daily_avg": round(daily_avg, 1),
        "top_jobs": top_jobs,
        "by_partition": partition_list,
        "projected_total": round(projected_total, 1),
        "days": days,
        "job_count": len(jobs_data),
    }


def get_heatmap_data(user: str, days: int = 90) -> dict:
    """
    Get aggregated job data for heatmap visualizations.

    Returns:
        - daily: Dict of date -> {total, completed, failed, cancelled, running}
        - hourly: Dict of day_hour (0-167) -> count (for day-of-week × hour grid)
        - success_rate: Dict of date -> success_rate percentage
    """
    from datetime import datetime, timedelta
    from collections import defaultdict

    # Fetch job history
    try:
        result = subprocess.run(
            [
                "sacct",
                "-u",
                user,
                f"--starttime=now-{days}days",
                "--format=JobID,Start,State",
                "--parsable2",
                "--noheader",
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )
    except (subprocess.TimeoutExpired, subprocess.SubprocessError):
        return {"error": "Could not fetch job history"}

    # Initialize data structures
    daily_data = defaultdict(lambda: {
        "total": 0,
        "completed": 0,
        "failed": 0,
        "cancelled": 0,
        "timeout": 0,
        "running": 0,
        "pending": 0,
    })

    # hourly_pattern[day_of_week * 24 + hour] = count
    hourly_pattern = defaultdict(int)

    # Parse job data
    for line in result.stdout.strip().split("\n"):
        if not line.strip():
            continue

        parts = line.split("|")
        if len(parts) < 3:
            continue

        job_id = parts[0].strip()
        # Skip step entries (e.g., "12345.batch", "12345.0")
        if "." in job_id:
            continue

        start_str = parts[1].strip()
        state = parts[2].strip().upper()

        # Parse start time
        if not start_str or start_str in ("Unknown", "None", "N/A"):
            continue

        try:
            # Format: 2024-01-15T10:30:00
            start_dt = datetime.strptime(start_str[:19], "%Y-%m-%dT%H:%M:%S")
        except ValueError:
            continue

        # Get date key (YYYY-MM-DD)
        date_key = start_dt.strftime("%Y-%m-%d")

        # Update daily counts
        daily_data[date_key]["total"] += 1

        if "COMPLETED" in state:
            daily_data[date_key]["completed"] += 1
        elif "FAILED" in state:
            daily_data[date_key]["failed"] += 1
        elif "CANCELLED" in state:
            daily_data[date_key]["cancelled"] += 1
        elif "TIMEOUT" in state:
            daily_data[date_key]["timeout"] += 1
        elif "RUNNING" in state:
            daily_data[date_key]["running"] += 1
        elif "PENDING" in state:
            daily_data[date_key]["pending"] += 1

        # Update hourly pattern (0=Monday, 6=Sunday)
        day_of_week = start_dt.weekday()
        hour = start_dt.hour
        pattern_key = day_of_week * 24 + hour
        hourly_pattern[pattern_key] += 1

    # Calculate success rates by date
    success_rates = {}
    for date_key, counts in daily_data.items():
        total = counts["total"]
        if total > 0:
            completed = counts["completed"]
            success_rates[date_key] = round((completed / total) * 100, 1)

    # Fill in missing dates for continuous display
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    current = start_date

    all_dates = []
    while current <= end_date:
        date_key = current.strftime("%Y-%m-%d")
        if date_key not in daily_data:
            daily_data[date_key] = {
                "total": 0,
                "completed": 0,
                "failed": 0,
                "cancelled": 0,
                "timeout": 0,
                "running": 0,
                "pending": 0,
            }
        all_dates.append(date_key)
        current += timedelta(days=1)

    # Convert to list format for frontend
    daily_list = []
    for date_key in sorted(all_dates):
        counts = daily_data[date_key]
        daily_list.append({
            "date": date_key,
            **counts,
            "success_rate": success_rates.get(date_key, 0),
        })

    # Convert hourly pattern to list (168 entries for 7 days × 24 hours)
    hourly_list = []
    for dow in range(7):  # Monday to Sunday
        for hour in range(24):
            key = dow * 24 + hour
            hourly_list.append({
                "day": dow,
                "hour": hour,
                "count": hourly_pattern[key],
            })

    # Calculate max values for scaling
    max_daily = max((d["total"] for d in daily_list), default=1)
    max_hourly = max((h["count"] for h in hourly_list), default=1)

    return {
        "daily": daily_list,
        "hourly": hourly_list,
        "max_daily": max_daily,
        "max_hourly": max_hourly,
        "days": days,
        "total_jobs": sum(d["total"] for d in daily_list),
    }

"""Aggregations over job records. Pure functions: no Slurm calls happen here."""

from __future__ import annotations

import re
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from slurm_dashboard.services.parsing import (
    array_parts,
    format_bytes,
    format_duration,
    parse_dependencies,
)

FAILED_CATEGORIES = {"failed", "timeout"}


def simple_category(job: dict) -> str:
    """Category for views that only colour four states (timeline, pipelines)."""
    category = job.get("state_category", "unknown")
    if category in {"cancelled", "timeout"}:
        return "failed"
    return category


def _local(iso_value: Optional[str]) -> Optional[datetime]:
    if not iso_value:
        return None
    try:
        return datetime.fromisoformat(iso_value)
    except ValueError:
        return None


def _median(values: List[float]) -> float:
    ordered = sorted(values)
    return ordered[len(ordered) // 2]


# ------------------------------------------------------------------- timeline


def history(jobs: List[dict], limit: int = 500) -> List[dict]:
    """Started jobs in the shape the timeline view draws."""
    rows = []
    for job in jobs:
        if not job.get("start"):
            continue
        rows.append(
            {
                "id": job["id"],
                "name": job.get("name", ""),
                "user": job.get("user", ""),
                "state": job.get("state", ""),
                "state_category": (
                    simple_category(job) if job.get("state_category") != "timeout" else "timeout"
                ),
                "start": job["start"],
                "end": job.get("end"),
                "elapsed": job.get("elapsed", ""),
                "partition": job.get("partition", ""),
                "log_key": job["id"],
            }
        )
        if len(rows) >= limit:
            break
    return rows


# ------------------------------------------------------------------ pipelines


def dependencies(queue: List[dict], recent: List[dict]) -> dict:
    """Dependency graph of queued jobs plus recently finished jobs.

    Array tasks are folded into one node per array so graphs stay readable.
    """
    members: Dict[str, List[dict]] = defaultdict(list)
    dep_strings: Dict[str, str] = {}
    for job in queue:
        base = array_parts(job["id"])[0].split("+")[0]
        members[base].append(job)
        if job.get("dependency"):
            dep_strings[base] = job["dependency"]
    queued = set(members)
    for job in recent:
        base = array_parts(job["id"])[0].split("+")[0]
        if base not in queued:
            members[base].append(job)

    nodes: Dict[str, dict] = {}
    for base, tasks in members.items():
        categories = {simple_category(t) for t in tasks}
        if "running" in categories:
            category = "running"
        elif "failed" in categories:
            category = "failed"
        elif categories == {"completed"}:
            category = "completed"
        else:
            category = "pending"
        first = tasks[0]
        nodes[base] = {
            "id": base,
            "name": first.get("name", ""),
            "state": first.get("state", "") if len(tasks) == 1 else category.upper(),
            "state_category": category,
            "dependency_str": dep_strings.get(base, ""),
            "start_time": first.get("start") or first.get("expected_start"),
            "end_time": first.get("end"),
            "partition": first.get("partition", ""),
            "tasks": len(tasks),
        }

    edges = []
    for base, dep_str in dep_strings.items():
        for dep_id, dep_type in parse_dependencies(dep_str):
            if dep_id in nodes and dep_id != base:
                edges.append({"from": dep_id, "to": base, "type": dep_type})

    pipelines = _connected_pipelines(nodes, edges)
    for pipeline in pipelines:
        done = sum(1 for j in pipeline["job_ids"] if nodes[j]["state_category"] == "completed")
        pipeline["completed"] = done
        pipeline["total"] = len(pipeline["job_ids"])
        pipeline["progress"] = round(done / pipeline["total"] * 100)
    return {"nodes": list(nodes.values()), "edges": edges, "pipelines": pipelines}


def _connected_pipelines(nodes: Dict[str, dict], edges: List[dict]) -> List[dict]:
    adjacency: Dict[str, set] = defaultdict(set)
    for edge in edges:
        adjacency[edge["from"]].add(edge["to"])
        adjacency[edge["to"]].add(edge["from"])

    seen: set = set()
    pipelines = []
    for start in sorted(adjacency):
        if start in seen:
            continue
        component, stack = [], [start]
        while stack:  # iterative: long chains must not hit the recursion limit
            node = stack.pop()
            if node in seen:
                continue
            seen.add(node)
            component.append(node)
            stack.extend(adjacency[node] - seen)
        if len(component) > 1:
            names = [nodes[j]["name"] for j in component]
            pipelines.append(
                {
                    "name": _common_prefix(names) or names[0] or "Pipeline",
                    "job_ids": sorted(component),
                }
            )
    return pipelines


def _common_prefix(strings: List[str]) -> str:
    if not strings:
        return ""
    prefix = strings[0]
    for text in strings[1:]:
        while prefix and not text.startswith(prefix):
            prefix = prefix[:-1]
    prefix = re.sub(r"[-_\d]+$", "", prefix)
    return prefix.strip() if len(prefix) >= 3 else ""


# ------------------------------------------------------------------- insights


def insights(jobs: List[dict]) -> dict:
    """Success statistics, efficiency and resource-request recommendations."""
    result = {
        "job_stats": None,
        "memory_insights": None,
        "time_insights": None,
        "failure_patterns": [],
        "efficiency_score": None,
        "by_name": [],
        "by_partition": [],
    }
    finished = [j for j in jobs if j.get("state_category") not in {"running", "pending", "unknown"}]
    if not finished:
        return result
    completed = [j for j in finished if j["state_category"] == "completed"]

    counts = Counter(j["state_category"] for j in finished)
    names = Counter(j["name"] for j in finished if j.get("name"))
    result["job_stats"] = {
        "total_jobs": len(finished),
        "completed": counts["completed"],
        "failed": counts["failed"],
        "cancelled": counts["cancelled"],
        "timeout": counts["timeout"],
        "success_rate": round(counts["completed"] / len(finished) * 100, 1),
        "job_names": [{"name": n, "count": c} for n, c in names.most_common(10)],
    }

    mem = [j for j in completed if j.get("mem_eff") is not None]
    if mem:
        avg = sum(j["mem_eff"] for j in mem) / len(mem)
        median_req = _median([j["req_mem_bytes"] for j in mem])
        median_used = _median([j["max_rss_bytes"] for j in mem])
        recommendation = None
        if avg < 50:
            recommendation = (
                f"Completed jobs use {avg:.0f}% of their requested memory on average. "
                f"Median request is {format_bytes(median_req)}, median peak use "
                f"{format_bytes(median_used)}; about {format_bytes(median_used * 1.3)} "
                "would leave 30% headroom."
            )
        result["memory_insights"] = {
            "avg_efficiency": round(avg, 1),
            "sample_count": len(mem),
            "median_requested": format_bytes(median_req),
            "median_used": format_bytes(median_used),
            "recommendation": recommendation,
        }

    timed = [j for j in completed if j["elapsed_seconds"] > 0 and j["timelimit_seconds"] > 0]
    if timed:
        usage = [min(100.0, j["elapsed_seconds"] / j["timelimit_seconds"] * 100) for j in timed]
        avg = sum(usage) / len(usage)
        elapsed = sorted(j["elapsed_seconds"] for j in timed)
        p90 = elapsed[min(len(elapsed) - 1, int(len(elapsed) * 0.9))]
        median_limit = _median([j["timelimit_seconds"] for j in timed])
        recommendation = None
        if avg < 30:
            recommendation = (
                f"90% of completed jobs finish within {format_duration(p90)}, while the median "
                f"time limit is {format_duration(median_limit)}. A limit near "
                f"{format_duration(p90 * 1.2)} can shorten queue waits."
            )
        result["time_insights"] = {
            "avg_efficiency": round(avg, 1),
            "sample_count": len(timed),
            "p90_runtime": format_duration(p90),
            "median_limit": format_duration(median_limit),
            "recommendation": recommendation,
        }

    cpu = [j["cpu_eff"] for j in completed if j.get("cpu_eff") is not None]
    result["efficiency_score"] = {
        "jobs_analyzed": len(completed),
        "cpu_efficiency": round(sum(cpu) / len(cpu), 1) if cpu else None,
        "memory_efficiency": result["memory_insights"]["avg_efficiency"] if mem else None,
        "time_efficiency": result["time_insights"]["avg_efficiency"] if timed else None,
    }
    result["failure_patterns"] = _failure_patterns(finished)
    result["by_name"] = _breakdown(finished, lambda j: j.get("name") or "(unnamed)")[:15]
    result["by_partition"] = _breakdown(finished, lambda j: j.get("partition") or "(none)")
    return result


def _breakdown(finished: List[dict], key_of) -> List[dict]:
    """Per-group run counts, outcomes, typical runtime, efficiency and hours."""
    groups: Dict[str, List[dict]] = defaultdict(list)
    for job in finished:
        groups[key_of(job)].append(job)

    rows = []
    for key, jobs in groups.items():
        counts = Counter(j["state_category"] for j in jobs)
        completed = [j for j in jobs if j["state_category"] == "completed"]
        runtimes = [j["elapsed_seconds"] for j in completed if j["elapsed_seconds"] > 0]
        mem = [j["mem_eff"] for j in completed if j.get("mem_eff") is not None]
        cpu = [j["cpu_eff"] for j in completed if j.get("cpu_eff") is not None]
        hours = [j.get("elapsed_seconds", 0) / 3600 for j in jobs]
        rows.append(
            {
                "key": key,
                "runs": len(jobs),
                "completed": counts["completed"],
                "failed": counts["failed"] + counts["timeout"],
                "cancelled": counts["cancelled"],
                "success_rate": round(counts["completed"] / len(jobs) * 100, 1),
                "median_runtime_seconds": int(_median(runtimes)) if runtimes else None,
                "cpu_eff": round(sum(cpu) / len(cpu), 1) if cpu else None,
                "mem_eff": round(sum(mem) / len(mem), 1) if mem else None,
                "cpu_hours": round(sum(h * j.get("cpus", 0) for h, j in zip(hours, jobs)), 1),
                "gpu_hours": round(sum(h * j.get("gpus", 0) for h, j in zip(hours, jobs)), 1),
            }
        )
    rows.sort(key=lambda r: (r["runs"], r["cpu_hours"]), reverse=True)
    return rows


def _failure_patterns(finished: List[dict]) -> List[dict]:
    """Partitions and job-name families with a high failure rate."""
    patterns = []

    def collect(kind: str, key_of, min_total: int, min_failed: int, min_rate: float) -> None:
        groups: Dict[str, List[int]] = defaultdict(lambda: [0, 0])
        for job in finished:
            key = key_of(job)
            if not key:
                continue
            groups[key][1] += 1
            if job["state_category"] in FAILED_CATEGORIES:
                groups[key][0] += 1
        for key, (failed, total) in groups.items():
            rate = failed / total * 100
            if total >= min_total and failed >= min_failed and rate > min_rate:
                patterns.append(
                    {
                        "type": kind,
                        "label": key,
                        "failed": failed,
                        "total": total,
                        "failure_rate": round(rate, 1),
                    }
                )

    collect("partition", lambda j: j.get("partition"), 5, 1, 20)
    collect(
        "job name",
        lambda j: (
            (re.sub(r"[-_]?\d+$", "", j.get("name", "")) + "*")
            if len(j.get("name", "")) >= 3
            else ""
        ),
        3,
        2,
        30,
    )
    patterns.sort(key=lambda p: (p["failure_rate"], p["total"]), reverse=True)
    return patterns


# ---------------------------------------------------------------------- usage


def usage(jobs: List[dict], days: int) -> dict:
    """CPU and GPU hours consumed, by day and by partition."""
    daily: Dict[str, Dict[str, float]] = defaultdict(lambda: {"cpu_hours": 0.0, "gpu_hours": 0.0})
    by_partition: Dict[str, Dict[str, float]] = defaultdict(
        lambda: {"cpu_hours": 0.0, "gpu_hours": 0.0}
    )
    rows = []
    for job in jobs:
        hours = job.get("elapsed_seconds", 0) / 3600
        cpu_hours = job.get("cpus", 0) * hours
        gpu_hours = job.get("gpus", 0) * hours
        if cpu_hours <= 0 and gpu_hours <= 0:
            continue
        start = _local(job.get("start"))
        if start:
            day = daily[start.strftime("%Y-%m-%d")]
            day["cpu_hours"] += cpu_hours
            day["gpu_hours"] += gpu_hours
        part = by_partition[job.get("partition") or "unknown"]
        part["cpu_hours"] += cpu_hours
        part["gpu_hours"] += gpu_hours
        rows.append(
            {
                "job_id": job["id"],
                "name": job.get("name", ""),
                "partition": job.get("partition", ""),
                "cpus": job.get("cpus", 0),
                "gpus": job.get("gpus", 0),
                "elapsed_hours": round(hours, 2),
                "cpu_hours": round(cpu_hours, 1),
                "gpu_hours": round(gpu_hours, 1),
                "state": job.get("state", ""),
            }
        )

    rows.sort(key=lambda r: r["cpu_hours"] + r["gpu_hours"], reverse=True)
    total_cpu = sum(d["cpu_hours"] for d in by_partition.values())
    total_gpu = sum(d["gpu_hours"] for d in by_partition.values())
    daily_list = [
        {
            "date": day,
            "cpu_hours": round(daily[day]["cpu_hours"], 1),
            "gpu_hours": round(daily[day]["gpu_hours"], 1),
        }
        for day in _date_range(days)
    ]
    active_days = sum(1 for d in daily_list if d["cpu_hours"] > 0 or d["gpu_hours"] > 0) or 1
    return {
        "total_cpu_hours": round(total_cpu, 1),
        "total_gpu_hours": round(total_gpu, 1),
        "daily_usage": daily_list,
        "daily_avg_cpu": round(total_cpu / active_days, 1),
        "daily_avg_gpu": round(total_gpu / active_days, 1),
        "top_jobs": rows[:10],
        "by_partition": [
            {
                "partition": name,
                "cpu_hours": round(v["cpu_hours"], 1),
                "gpu_hours": round(v["gpu_hours"], 1),
            }
            for name, v in sorted(
                by_partition.items(),
                key=lambda kv: kv[1]["cpu_hours"] + kv[1]["gpu_hours"],
                reverse=True,
            )
        ],
        "days": days,
        "job_count": len(rows),
    }


def _date_range(days: int) -> List[str]:
    today = datetime.now().date()
    return [(today - timedelta(days=offset)).strftime("%Y-%m-%d") for offset in range(days, -1, -1)]


# -------------------------------------------------------------------- heatmap


def heatmap(jobs: List[dict], days: int) -> dict:
    """Jobs per day (by state) and per weekday/hour of their start time."""
    states = ("completed", "failed", "cancelled", "timeout", "running", "pending")
    daily: Dict[str, Dict[str, int]] = {
        day: dict.fromkeys(("total",) + states, 0) for day in _date_range(days)
    }
    hourly: Dict[int, int] = defaultdict(int)

    for job in jobs:
        start = _local(job.get("start"))
        if not start:
            continue
        counts = daily.get(start.strftime("%Y-%m-%d"))
        if counts is None:
            continue
        counts["total"] += 1
        category = job.get("state_category")
        if category in counts:
            counts[category] += 1
        hourly[start.weekday() * 24 + start.hour] += 1

    daily_list = []
    for day in sorted(daily):
        counts = daily[day]
        rate = round(counts["completed"] / counts["total"] * 100, 1) if counts["total"] else 0
        daily_list.append(dict(counts, date=day, success_rate=rate))
    hourly_list = [
        {"day": dow, "hour": hour, "count": hourly[dow * 24 + hour]}
        for dow in range(7)
        for hour in range(24)
    ]
    return {
        "daily": daily_list,
        "hourly": hourly_list,
        "max_daily": max((d["total"] for d in daily_list), default=0) or 1,
        "max_hourly": max((h["count"] for h in hourly_list), default=0) or 1,
        "days": days,
        "total_jobs": sum(d["total"] for d in daily_list),
    }

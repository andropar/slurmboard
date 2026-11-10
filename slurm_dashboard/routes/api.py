"""REST API routes for Slurm Dashboard."""

from __future__ import annotations

import logging
import socket
from pathlib import Path
from typing import Any, Callable, List, Optional, Tuple

from flask import Blueprint, Response, current_app, jsonify, request, send_file

from slurm_dashboard.config import Config, get_config
from slurm_dashboard.services import analytics
from slurm_dashboard.services.logs import LogIndex, resolve_log, search_log
from slurm_dashboard.services.parsing import is_active_state
from slurm_dashboard.services.runner import SlurmError
from slurm_dashboard.services.slurm import Slurm, valid_job_id
from slurm_dashboard.services.templates import TemplateStore

logger = logging.getLogger(__name__)

api = Blueprint("api", __name__, url_prefix="/api")

MAX_RECENT = 1500
RECENT_FIELDS = (
    "id",
    "name",
    "user",
    "state",
    "state_category",
    "partition",
    "gres",
    "gpus",
    "cpus",
    "elapsed",
    "elapsed_seconds",
    "timelimit_seconds",
    "exit_code",
    "submit",
    "nodelist",
    "array_task_id",
    "start",
    "end",
    "cpu_eff",
    "mem_eff",
    "array_job_id",
    "log_key",
    "owned",
)


def slurm() -> Slurm:
    return current_app.extensions["slurm"]


def log_index() -> LogIndex:
    return current_app.extensions["log_index"]


def templates() -> TemplateStore:
    return current_app.extensions["templates"]


def _days(default: int, maximum: int) -> int:
    try:
        return max(1, min(maximum, int(request.args.get("days", default))))
    except ValueError:
        return default


def _error(message: str, status: int) -> Tuple[Response, int]:
    return jsonify({"error": message}), status


def _job_id_or_400(job_id: str) -> Optional[Tuple[Response, int]]:
    return None if valid_job_id(job_id) else _error("Invalid job ID", 400)


def locate_log(job_id: str, kind: str) -> Tuple[Optional[Path], str]:
    """Find a job's log file. Returns (path, message explaining a miss)."""
    if kind not in {"stdout", "stderr"}:
        return None, "Invalid log kind"
    if not valid_job_id(job_id):
        return None, "Invalid job ID"
    config = get_config()
    job = slurm().find_job(job_id, config.history_days)
    scontrol = {}
    if job is None:
        # Not in Slurm any more: it may still have log files under --log-root.
        entry = log_index().entries().get(job_id)
        if entry is None:
            return None, f"Job {job_id} is unknown to Slurm and has no log under {config.log_root}"
        job = {"id": job_id, "name": entry["name"]}
    elif job.get("active"):
        scontrol = slurm().scontrol_job(job["id"])
    path, tried = resolve_log(job, kind, scontrol, log_index())
    if path is None:
        looked = "\n".join(f"  {p}" for p in tried) or "  (Slurm reported no output path)"
        if job.get("state_category") == "pending":
            hint = "The job has not started yet."
        elif job.get("user") and not job.get("owned"):
            hint = f"The job belongs to {job['user']}; you may not be allowed to read its files."
        else:
            hint = "If your logs live elsewhere, set --log-root and --log-pattern."
        return None, f"No {kind} file found for job {job_id}. Looked for:\n{looked}\n{hint}"
    return path, ""


# ----------------------------------------------------------------------- jobs


@api.route("/jobs")
def jobs() -> Response:
    """Active jobs from the queue plus finished jobs from accounting.

    Query: days (1-90) of finished jobs; defaults to --history-days.
    """
    config = get_config()
    client = slurm()
    days = _days(config.history_days, 90)
    queue, queue_error = client.queue()
    history, history_error = client.accounting(days)

    running = [dict(job, start_time=job.get("start") or job.get("submit")) for job in queue]
    active_ids = {job["id"] for job in queue}
    recent = []
    for job in history:
        if job["id"] in active_ids or is_active_state(job["state"]):
            continue
        row = {key: job.get(key) for key in RECENT_FIELDS}
        row["updated"] = job.get("end") or job.get("start") or job.get("submit")
        row["start_time"] = job.get("start")
        recent.append(row)
        if len(recent) >= MAX_RECENT:
            break
    truncated = len(recent) >= MAX_RECENT

    if history_error and not history and not config.all_users:
        # Without accounting (no slurmdbd, or it is down) the log files under
        # --log-root are the only record of past jobs.
        for orphan in log_index().orphans(active_ids, limit=200):
            recent.append(dict(orphan, updated=orphan["end"]))

    errors = [e for e in (queue_error, history_error) if e]
    return jsonify(
        {
            "running": running,
            "recent": recent,
            "errors": errors,
            "meta": {
                "user": config.user,
                "os_user": client.os_user,
                "all_users": config.all_users,
                "history_days": days,
                "default_history_days": config.history_days,
                "truncated": truncated,
                "host": socket.gethostname().split(".")[0],
            },
        }
    )


@api.route("/job/<path:job_id>")
def job(job_id: str) -> Any:
    """One job in full: summary, how it was submitted, and where its logs are."""
    bad = _job_id_or_400(job_id)
    if bad:
        return bad
    config = get_config()
    client = slurm()
    record = client.find_job(job_id, config.history_days)
    scontrol: dict = {}
    submission = None
    if record is None:
        # Aged out of accounting, but its log files are still under --log-root.
        entry = log_index().entries().get(job_id)
        if entry is None:
            return _error(f"Job {job_id} is unknown to Slurm", 404)
        record = {"id": job_id, "name": entry["name"], "active": False}
    else:
        if record["active"]:
            scontrol = client.scontrol_job(record["id"])
        submission = client.submit_info(record["id"], config.history_days)

    logs: dict = {}
    for kind in ("stdout", "stderr"):
        path, tried = resolve_log(record, kind, scontrol, log_index())
        size = None
        if path is not None:
            try:
                size = path.stat().st_size
            except OSError:
                path = None
        logs[kind] = {"path": str(path) if path else None, "size": size, "tried": tried}
    logs["same_file"] = bool(
        logs["stdout"]["path"] and logs["stdout"]["path"] == logs["stderr"]["path"]
    )
    return jsonify({"job": client.summary(record), "submission": submission, "logs": logs})


@api.route("/log_file/<path:job_id>")
def log_file(job_id: str) -> Any:
    """Download a job's complete log. Query: kind (stdout|stderr)."""
    kind = request.args.get("kind", "stdout")
    path, message = locate_log(job_id, kind)
    if path is None:
        return _error(message.splitlines()[0], 404)
    suffix = "out" if kind == "stdout" else "err"
    return send_file(
        path,
        mimetype="text/plain",
        as_attachment=True,
        download_name=f"slurm-{job_id}.{suffix}.log",
        max_age=0,
    )


@api.route("/job_details/<path:job_id>")
def job_details(job_id: str) -> Any:
    """State, timing and efficiency for one job."""
    return _job_id_or_400(job_id) or jsonify(slurm().job_details(job_id, get_config().history_days))


@api.route("/job_resources/<path:job_id>")
def job_resources(job_id: str) -> Any:
    """Live resource usage of a running job."""
    return _job_id_or_400(job_id) or jsonify(slurm().job_resources(job_id))


@api.route("/job_submit_info/<path:job_id>")
def job_submit_info(job_id: str) -> Any:
    """Submission details (script, working directory, resources) of a job."""
    return _job_id_or_400(job_id) or jsonify(slurm().submit_info(job_id, get_config().history_days))


@api.route("/job_script/<path:job_id>")
def job_script(job_id: str) -> Any:
    """Batch script of a job. The script is looked up by job, not by path."""
    bad = _job_id_or_400(job_id)
    if bad:
        return bad
    script = slurm().job_script(job_id, get_config().history_days)
    return (jsonify(script), 404) if "error" in script else jsonify(script)


# -------------------------------------------------------------------- actions


def _action(run: Callable[[], dict]) -> Any:
    try:
        return jsonify(run())
    except SlurmError as exc:
        return _error(str(exc), 400)


@api.route("/cancel/<path:job_id>", methods=["POST"])
def cancel(job_id: str) -> Any:
    """Cancel one of your running or pending jobs."""
    bad = _job_id_or_400(job_id)
    if bad:
        return bad

    def run() -> dict:
        slurm().cancel(job_id)
        logger.info("Cancelled job %s", job_id)
        return {"success": True}

    return _action(run)


@api.route("/submit", methods=["POST"])
def submit() -> Any:
    """Submit a job.

    JSON body: script_path or script_content, plus optional job_name,
    partition, time_limit, memory, cpus, gpus, work_dir, output_file,
    error_file, dependency.
    """
    data = request.get_json(silent=True)
    if not isinstance(data, dict):
        return _error("No JSON body provided", 400)

    def run() -> dict:
        result = slurm().submit(
            data, script_path=data.get("script_path"), script_content=data.get("script_content")
        )
        logger.info("Submitted job %s", result["job_id"])
        return result

    return _action(run)


@api.route("/resubmit", methods=["POST"])
def resubmit() -> Any:
    """Resubmit one of your jobs.

    JSON body: job_id, plus optional overrides work_dir, partition,
    time_limit, memory, cpus.
    """
    data = request.get_json(silent=True)
    if not isinstance(data, dict):
        return _error("No JSON body provided", 400)
    job_id = str(data.get("job_id") or "")
    bad = _job_id_or_400(job_id)
    if bad:
        return bad
    overrides = {k: data.get(k) for k in ("work_dir", "partition", "time_limit", "memory", "cpus")}

    def run() -> dict:
        result = slurm().resubmit(job_id, overrides, get_config().history_days)
        logger.info("Resubmitted job %s as %s", job_id, result["job_id"])
        return result

    return _action(run)


# ----------------------------------------------------------------------- logs


@api.route("/search_log")
def search_log_endpoint() -> Any:
    """Search a job's log. Query: log_key (job ID), kind, q, context, regex."""
    pattern = request.args.get("q", "")
    if not pattern:
        return jsonify({"error": "Missing search pattern", "matches": [], "total_matches": 0}), 400
    try:
        context_lines = max(0, min(10, int(request.args.get("context", "3"))))
    except ValueError:
        context_lines = 3

    path, message = locate_log(request.args.get("log_key", ""), request.args.get("kind", "stdout"))
    if path is None:
        return jsonify({"error": message.splitlines()[0], "matches": [], "total_matches": 0}), 404

    use_regex = request.args.get("regex", "true").lower() == "true"
    result = search_log(path, pattern, context_lines=context_lines, use_regex=use_regex)
    return (jsonify(result), 400) if "error" in result else jsonify(result)


# ------------------------------------------------------------------ analytics


def _with_history(days: int, build: Callable[[List[dict]], dict]) -> Response:
    history, error = slurm().accounting(days)
    payload = build(history)
    if error:
        payload["error"] = error
    return jsonify(payload)


@api.route("/job_history")
def job_history() -> Response:
    """Started jobs for the timeline. Query: days (max 30), limit (max 2000)."""
    days = _days(7, 30)
    try:
        limit = max(1, min(2000, int(request.args.get("limit", "1000"))))
    except ValueError:
        limit = 1000
    return _with_history(days, lambda jobs: {"jobs": analytics.history(jobs, limit), "days": days})


@api.route("/job_dependencies")
def job_dependencies() -> Response:
    """Dependency graph: nodes, edges and connected pipelines."""
    queue, queue_error = slurm().queue()
    recent, history_error = slurm().accounting(2)
    payload = analytics.dependencies(queue, recent)
    errors = [e for e in (queue_error, history_error) if e]
    if errors:
        payload["error"] = "; ".join(errors)
    return jsonify(payload)


@api.route("/insights")
def insights() -> Response:
    """Success rate, efficiency and request recommendations. Query: days (max 90)."""
    days = _days(30, 90)

    def build(jobs: List[dict]) -> dict:
        payload = analytics.insights(jobs)
        totals = analytics.usage(jobs, days)
        payload["usage"] = {
            "cpu_hours": totals["total_cpu_hours"],
            "gpu_hours": totals["total_gpu_hours"],
        }
        payload["daily"] = analytics.heatmap(jobs, days)["daily"]
        payload["days"] = days
        return payload

    return _with_history(days, build)


@api.route("/usage")
def usage() -> Response:
    """CPU and GPU hours by day and partition. Query: days (max 90)."""
    days = _days(30, 90)
    return _with_history(days, lambda jobs: analytics.usage(jobs, days))


@api.route("/heatmap")
def heatmap() -> Response:
    """Job counts per day and per weekday/hour. Query: days (max 365)."""
    days = _days(90, 365)
    return _with_history(days, lambda jobs: analytics.heatmap(jobs, days))


@api.route("/partitions")
def partitions() -> Response:
    """Partitions of the cluster."""
    parts, error = slurm().partitions()
    return jsonify({"partitions": parts, "error": error})


# ------------------------------------------------------------------ templates


@api.route("/templates", methods=["GET"])
def list_templates() -> Response:
    """All saved job templates."""
    return jsonify({"templates": templates().list()})


@api.route("/templates", methods=["POST"])
def save_template() -> Any:
    """Save a job template. JSON body: name (required) and submit-form fields."""
    data = request.get_json(silent=True)
    if not isinstance(data, dict):
        return _error("No JSON body provided", 400)
    name = str(data.get("name") or "").strip()
    if not name:
        return _error("Template name is required", 400)
    try:
        return jsonify({"success": True, "template": templates().save(name, data)})
    except OSError as exc:
        return _error(f"Could not save template: {exc.strerror or exc}", 500)


@api.route("/templates/<template_id>", methods=["GET"])
def get_template(template_id: str) -> Any:
    """One template by ID."""
    template = templates().get(template_id)
    return jsonify(template) if template else _error("Template not found", 404)


@api.route("/templates/<template_id>", methods=["DELETE"])
def delete_template(template_id: str) -> Any:
    """Delete a template."""
    try:
        found = templates().delete(template_id)
    except OSError as exc:
        return _error(f"Could not delete template: {exc.strerror or exc}", 500)
    return jsonify({"success": True}) if found else _error("Template not found", 404)


def describe(config: Config) -> dict:
    """Settings the page needs before its first data request."""
    return {
        "user": config.user,
        "all_users": config.all_users,
        "history_days": config.history_days,
        "host": socket.gethostname().split(".")[0],
    }

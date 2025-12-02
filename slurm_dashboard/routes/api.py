"""REST API routes for Slurm Dashboard."""
from __future__ import annotations

import time
from functools import lru_cache
from typing import Dict

from flask import Blueprint, Response, jsonify, request

from slurm_dashboard.config import get_config
from slurm_dashboard.services.logs import collect_recent_jobs, safe_log_path, search_log
from slurm_dashboard.services.slurm import (
    cancel_job,
    get_available_partitions,
    get_cost_data,
    get_heatmap_data,
    get_job_dependencies,
    get_job_details,
    get_job_efficiency,
    get_job_history,
    get_job_insights,
    get_job_resources,
    get_job_submit_info,
    get_queue_info,
    get_running_jobs,
    get_script_content,
    predict_job_completion,
    resubmit_job,
    submit_job,
)

api = Blueprint("api", __name__, url_prefix="/api")


@lru_cache(maxsize=1)
def cached_recent(timestamp_bucket: int, pattern_hash: int) -> list[dict]:
    """Cache recent jobs with time-based invalidation."""
    config = get_config()
    return collect_recent_jobs(config.log_root, config.log_pattern)


@api.route("/jobs")
def jobs() -> Response:
    """Get running and recent jobs."""
    config = get_config()
    now_bucket = int(time.time() // config.refresh_cache)
    # Include pattern hash in cache key so pattern changes invalidate cache
    pattern_hash = hash(config.log_pattern.pattern)
    recent = cached_recent(now_bucket, pattern_hash)
    running = get_running_jobs(config.user)
    return jsonify({"running": running, "recent": recent})


@api.route("/job_details/<job_id>")
def job_details(job_id: str) -> Response:
    """Get detailed information about a specific job."""
    if not job_id.isdigit():
        return jsonify({"error": "Invalid job ID"}), 400
    config = get_config()
    details = get_job_details(job_id, config.user)
    return jsonify(details)


@api.route("/cancel/<job_id>", methods=["POST"])
def cancel(job_id: str) -> Response:
    """Cancel a running job."""
    if not job_id.isdigit():
        return jsonify({"error": "Invalid job ID"}), 400
    success, error = cancel_job(job_id)
    if success:
        return jsonify({"success": True})
    return jsonify({"error": error}), 500


@api.route("/search_log")
def search_log_endpoint() -> Response:
    """
    Search log file for a pattern.

    Query params:
        log_key: Job log key (name::job_id)
        kind: "stdout" or "stderr"
        q: Search pattern
        context: Context lines (default 3)
        regex: "true" or "false" (default "true")
    """
    log_key = request.args.get("log_key", "")
    kind = request.args.get("kind", "stdout")
    pattern = request.args.get("q", "")
    context = request.args.get("context", "3")
    use_regex = request.args.get("regex", "true").lower() == "true"

    if not pattern:
        return jsonify({"error": "Missing search pattern", "matches": [], "total_matches": 0}), 400

    if kind not in {"stdout", "stderr"}:
        return jsonify({"error": "Invalid kind", "matches": [], "total_matches": 0}), 400

    try:
        context_lines = int(context)
        context_lines = max(0, min(10, context_lines))  # Clamp to 0-10
    except ValueError:
        context_lines = 3

    config = get_config()
    path = safe_log_path(log_key, kind, config.log_root, config.log_pattern)

    if path is None:
        return jsonify({"error": "Log not found", "matches": [], "total_matches": 0}), 404

    result = search_log(path, pattern, context_lines=context_lines, use_regex=use_regex)

    if "error" in result:
        return jsonify(result), 400

    return jsonify(result)


@api.route("/job_history")
def job_history() -> Response:
    """
    Get job history for timeline visualization.

    Query params:
        days: Number of days of history (default 7, max 30)
        limit: Max jobs to return (default 500, max 1000)
    """
    config = get_config()

    try:
        days = int(request.args.get("days", "7"))
        days = max(1, min(30, days))
    except ValueError:
        days = 7

    try:
        limit = int(request.args.get("limit", "500"))
        limit = max(1, min(1000, limit))
    except ValueError:
        limit = 500

    jobs = get_job_history(config.user, days=days, limit=limit)
    return jsonify({"jobs": jobs, "days": days})


@api.route("/job_resources/<job_id>")
def job_resources(job_id: str) -> Response:
    """Get current resource usage for a running job."""
    if not job_id.isdigit():
        return jsonify({"error": "Invalid job ID"}), 400

    resources = get_job_resources(job_id)
    return jsonify(resources)


@api.route("/job_efficiency/<job_id>")
def job_efficiency(job_id: str) -> Response:
    """Get efficiency metrics for a completed job."""
    if not job_id.isdigit():
        return jsonify({"error": "Invalid job ID"}), 400

    config = get_config()
    efficiency = get_job_efficiency(job_id, config.user)
    return jsonify(efficiency)


@api.route("/job_submit_info/<job_id>")
def job_submit_info(job_id: str) -> Response:
    """Get submission information for a job."""
    if not job_id.isdigit():
        return jsonify({"error": "Invalid job ID"}), 400

    config = get_config()
    info = get_job_submit_info(job_id, config.user)
    return jsonify(info)


@api.route("/script_content")
def script_content() -> Response:
    """
    Get the content of a job submission script.

    Query params:
        path: Path to the script file
    """
    script_path = request.args.get("path", "")
    if not script_path:
        return jsonify({"error": "No script path provided"}), 400

    content = get_script_content(script_path)

    if "error" in content:
        return jsonify(content), 404

    return jsonify(content)


@api.route("/resubmit", methods=["POST"])
def resubmit() -> Response:
    """
    Resubmit a job with optional parameter overrides.

    JSON body:
        script_path: Required path to the script
        work_dir: Optional working directory
        partition: Optional partition override
        time_limit: Optional time limit override
        memory: Optional memory override
        cpus: Optional CPU count override
    """
    data = request.get_json()
    if not data:
        return jsonify({"error": "No JSON body provided"}), 400

    script_path = data.get("script_path")
    if not script_path:
        return jsonify({"error": "script_path is required"}), 400

    result = resubmit_job(
        script_path=script_path,
        work_dir=data.get("work_dir"),
        partition=data.get("partition"),
        time_limit=data.get("time_limit"),
        memory=data.get("memory"),
        cpus=data.get("cpus"),
    )

    if "error" in result:
        return jsonify(result), 500

    return jsonify(result)


@api.route("/queue_info")
def queue_info() -> Response:
    """
    Get queue position and wait time estimates for pending jobs.

    Returns information about pending jobs including:
        - Queue position within partition
        - Estimated wait time based on historical data
        - Confidence level (high/medium/low)
    """
    config = get_config()
    info = get_queue_info(config.user)
    return jsonify(info)


@api.route("/job_dependencies")
def job_dependencies() -> Response:
    """
    Get job dependency graph data for DAG visualization.

    Returns:
        - nodes: List of jobs with state and metadata
        - edges: Dependency relationships between jobs
        - pipelines: Grouped connected job pipelines with progress
    """
    config = get_config()
    deps = get_job_dependencies(config.user)
    return jsonify(deps)


@api.route("/insights")
def insights() -> Response:
    """
    Get job insights and resource recommendations.

    Query params:
        days: Number of days of history to analyze (default 30, max 90)

    Returns:
        - memory_insights: Memory usage patterns and recommendations
        - time_insights: Runtime patterns and recommendations
        - failure_patterns: Detected failure patterns
        - efficiency_score: Overall efficiency metrics
        - job_stats: Basic job statistics
    """
    try:
        days = int(request.args.get("days", "30"))
        days = max(1, min(90, days))
    except ValueError:
        days = 30

    config = get_config()
    data = get_job_insights(config.user, days=days)
    return jsonify(data)


@api.route("/predict/<job_id>")
def predict(job_id: str) -> Response:
    """
    Predict completion time for a running job.

    Returns estimated remaining time based on similar historical jobs.
    """
    if not job_id.isdigit():
        return jsonify({"error": "Invalid job ID"}), 400

    config = get_config()
    prediction = predict_job_completion(job_id, config.user)
    return jsonify(prediction)


@api.route("/partitions")
def partitions() -> Response:
    """Get available Slurm partitions."""
    parts = get_available_partitions()
    return jsonify({"partitions": parts})


@api.route("/submit", methods=["POST"])
def submit() -> Response:
    """
    Submit a new job to Slurm.

    JSON body:
        script_path: Path to existing script (optional if script_content provided)
        script_content: Script content to submit (optional if script_path provided)
        job_name: Job name (optional)
        partition: Partition to submit to (optional)
        time_limit: Time limit (optional, e.g., "1:00:00")
        memory: Memory request (optional, e.g., "4G")
        cpus: Number of CPUs (optional)
        gpus: Number of GPUs (optional)
        work_dir: Working directory (optional)
        output_file: Output file path (optional)
        error_file: Error file path (optional)
        dependency: Job dependency string (optional)
        environment: Environment variables dict (optional)
    """
    data = request.get_json()
    if not data:
        return jsonify({"error": "No JSON body provided"}), 400

    script_path = data.get("script_path")
    script_content = data.get("script_content")

    if not script_path and not script_content:
        return jsonify({"error": "Either script_path or script_content is required"}), 400

    result = submit_job(
        script_path=script_path,
        script_content=script_content,
        job_name=data.get("job_name"),
        partition=data.get("partition"),
        time_limit=data.get("time_limit"),
        memory=data.get("memory"),
        cpus=data.get("cpus"),
        gpus=data.get("gpus"),
        work_dir=data.get("work_dir"),
        output_file=data.get("output_file"),
        error_file=data.get("error_file"),
        dependency=data.get("dependency"),
        environment=data.get("environment"),
    )

    if "error" in result:
        return jsonify(result), 500

    return jsonify(result)


# In-memory template storage (for simplicity; could use file-based storage)
_job_templates: Dict[str, dict] = {}


@api.route("/templates", methods=["GET"])
def list_templates() -> Response:
    """Get all saved job templates."""
    return jsonify({"templates": list(_job_templates.values())})


@api.route("/templates", methods=["POST"])
def save_template() -> Response:
    """
    Save a job template.

    JSON body:
        name: Template name (required)
        description: Template description (optional)
        script_content: Script content (optional)
        job_name: Default job name (optional)
        partition: Default partition (optional)
        time_limit: Default time limit (optional)
        memory: Default memory (optional)
        cpus: Default CPU count (optional)
        gpus: Default GPU count (optional)
        work_dir: Default working directory (optional)
    """
    data = request.get_json()
    if not data:
        return jsonify({"error": "No JSON body provided"}), 400

    name = data.get("name")
    if not name:
        return jsonify({"error": "Template name is required"}), 400

    template = {
        "id": name.lower().replace(" ", "_"),
        "name": name,
        "description": data.get("description", ""),
        "script_content": data.get("script_content", ""),
        "job_name": data.get("job_name", ""),
        "partition": data.get("partition", ""),
        "time_limit": data.get("time_limit", ""),
        "memory": data.get("memory", ""),
        "cpus": data.get("cpus"),
        "gpus": data.get("gpus"),
        "work_dir": data.get("work_dir", ""),
        "created_at": time.time(),
    }

    _job_templates[template["id"]] = template
    return jsonify({"success": True, "template": template})


@api.route("/templates/<template_id>", methods=["GET"])
def get_template(template_id: str) -> Response:
    """Get a specific template by ID."""
    template = _job_templates.get(template_id)
    if not template:
        return jsonify({"error": "Template not found"}), 404
    return jsonify(template)


@api.route("/templates/<template_id>", methods=["DELETE"])
def delete_template(template_id: str) -> Response:
    """Delete a template."""
    if template_id not in _job_templates:
        return jsonify({"error": "Template not found"}), 404
    del _job_templates[template_id]
    return jsonify({"success": True})


@api.route("/heatmap")
def heatmap() -> Response:
    """
    Get aggregated job data for heatmap visualizations.

    Query params:
        days: Number of days of history (default 90, max 365)

    Returns:
        - daily: List of daily job counts and states
        - hourly: List of day×hour pattern data
        - max_daily: Maximum daily count (for scaling)
        - max_hourly: Maximum hourly count (for scaling)
        - total_jobs: Total number of jobs in the period
    """
    try:
        days = int(request.args.get("days", "90"))
        days = max(1, min(365, days))
    except ValueError:
        days = 90

    config = get_config()
    data = get_heatmap_data(config.user, days=days)
    return jsonify(data)


@api.route("/cost")
def cost() -> Response:
    """
    Get cost/allocation usage data.

    Query params:
        days: Number of days of history (default 30, max 90)

    Returns:
        - total_sus: Total service units consumed
        - daily_usage: List of daily SU usage
        - daily_avg: Average daily consumption
        - top_jobs: Most expensive jobs
        - by_partition: Usage breakdown by partition
        - projected_total: Projected usage by end of month
    """
    try:
        days = int(request.args.get("days", "30"))
        days = max(1, min(90, days))
    except ValueError:
        days = 30

    config = get_config()
    data = get_cost_data(config.user, days=days)
    return jsonify(data)

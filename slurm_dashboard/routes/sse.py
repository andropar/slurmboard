"""Server-Sent Events routes for log streaming."""

import json
import time
from pathlib import Path

from flask import Blueprint, Response, request, stream_with_context

from slurm_dashboard.config import get_config
from slurm_dashboard.services.logs import safe_log_path

sse = Blueprint("sse", __name__)


def event_stream(target: Path):
    """Generate SSE events for log file updates."""
    max_bytes = 200_000
    try:
        with target.open("r") as handle:
            handle.seek(0, 2)
            file_size = handle.tell()
            start = max(file_size - max_bytes, 0)
            handle.seek(start)
            if start > 0:
                handle.readline()
            snapshot = handle.read()
            position = handle.tell()
            yield f"data: {json.dumps({'snapshot': snapshot})}\n\n"
            while True:
                handle.seek(position)
                chunk = handle.readline()
                if chunk:
                    position = handle.tell()
                    yield f"data: {json.dumps({'append': chunk})}\n\n"
                else:
                    time.sleep(1)
    except GeneratorExit:
        return


@sse.route("/stream_log")
def stream_log() -> Response:
    """Stream log file contents via SSE."""
    log_key = request.args.get("log_key", "")
    kind = request.args.get("kind", "stdout")
    if kind not in {"stdout", "stderr"}:
        return Response("Invalid kind", status=400)

    config = get_config()
    path = safe_log_path(log_key, kind, config.log_root, config.log_pattern)
    if path is None:
        return Response("Log not found", status=404)

    headers = {"Content-Type": "text/event-stream", "Cache-Control": "no-cache"}
    return Response(stream_with_context(event_stream(path)), headers=headers)

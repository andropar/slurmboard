"""Server-Sent Events routes for log streaming."""

from __future__ import annotations

import json
from typing import Iterator

from flask import Blueprint, Response, request, stream_with_context

from slurm_dashboard.routes.api import locate_log
from slurm_dashboard.services.logs import FULL_SNAPSHOT_BYTES, SNAPSHOT_BYTES, tail_events

sse = Blueprint("sse", __name__)


def _frame(event: dict) -> str:
    # Pings are SSE comments: they keep the connection alive without reaching the page.
    return ": ping\n\n" if event.get("ping") else f"data: {json.dumps(event)}\n\n"


@sse.route("/stream_log")
def stream_log() -> Response:
    """Stream a job's log. Query: log_key (job ID), kind (stdout|stderr), full.

    The stream starts with the tail of the file; ``full=1`` raises that from
    200 KB to 8 MB.

    A missing log is reported as an ``error`` event rather than an HTTP error,
    because EventSource cannot read the body of a failed response.
    """
    path, message = locate_log(request.args.get("log_key", ""), request.args.get("kind", "stdout"))
    snapshot = FULL_SNAPSHOT_BYTES if request.args.get("full") == "1" else SNAPSHOT_BYTES

    def events() -> Iterator[str]:
        if path is None:
            yield _frame({"error": message})
            return
        yield _frame({"path": str(path)})
        for event in tail_events(path, snapshot_bytes=snapshot):
            yield _frame(event)

    headers = {"Cache-Control": "no-cache", "X-Accel-Buffering": "no"}
    return Response(stream_with_context(events()), mimetype="text/event-stream", headers=headers)

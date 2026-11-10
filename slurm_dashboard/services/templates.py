"""Job templates persisted as a JSON file in the user's config directory."""

from __future__ import annotations

import json
import logging
import os
import re
import threading
import time
from pathlib import Path
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

FIELDS = (
    "description",
    "script_content",
    "job_name",
    "partition",
    "time_limit",
    "memory",
    "cpus",
    "gpus",
    "work_dir",
)


class TemplateStore:
    """Load and save job templates; safe for concurrent requests."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._lock = threading.Lock()

    def _read(self) -> Dict[str, dict]:
        try:
            data = json.loads(self.path.read_text())
        except FileNotFoundError:
            return {}
        except (OSError, ValueError) as exc:
            logger.warning("Could not read templates from %s: %s", self.path, exc)
            return {}
        return data if isinstance(data, dict) else {}

    def _write(self, templates: Dict[str, dict]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(templates, indent=2))
        os.replace(str(tmp), str(self.path))

    def list(self) -> List[dict]:
        return sorted(self._read().values(), key=lambda t: t.get("name", "").lower())

    def get(self, template_id: str) -> Optional[dict]:
        return self._read().get(template_id)

    def save(self, name: str, data: dict) -> dict:
        """Create or replace the template with this name."""
        template_id = re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_") or "template"
        template = {"id": template_id, "name": name, "created_at": time.time()}
        for field in FIELDS:
            template[field] = (
                data.get(field) if field in ("cpus", "gpus") else (data.get(field) or "")
            )
        with self._lock:
            templates = self._read()
            templates[template_id] = template
            self._write(templates)
        return template

    def delete(self, template_id: str) -> bool:
        with self._lock:
            templates = self._read()
            if template_id not in templates:
                return False
            del templates[template_id]
            self._write(templates)
        return True

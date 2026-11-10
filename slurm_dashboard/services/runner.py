"""Subprocess wrapper and TTL cache shared by all Slurm queries."""

from __future__ import annotations

import logging
import subprocess
import threading
import time
from typing import Any, Callable, Dict, Hashable, List, Optional, Tuple

logger = logging.getLogger(__name__)


class SlurmError(Exception):
    """A Slurm command was missing, timed out or exited non-zero."""


def run_command(
    cmd: List[str], timeout: float = 10, stdin: Optional[str] = None, cwd: Optional[str] = None
) -> str:
    """Run a Slurm command and return its stdout.

    Raises:
        SlurmError: with a message that is safe to show in the UI.
    """
    name = cmd[0]
    started = time.monotonic()
    try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            errors="replace",
            timeout=timeout,
            input=stdin,
            cwd=cwd,
            check=False,
        )
    except FileNotFoundError as exc:
        if cwd and exc.filename == cwd:
            raise SlurmError(f"Working directory not found: {cwd}") from exc
        raise SlurmError(f"{name} not found on PATH (is Slurm available on this host?)") from exc
    except NotADirectoryError as exc:
        raise SlurmError(f"Working directory not found: {cwd}") from exc
    except subprocess.TimeoutExpired as exc:
        logger.warning("%s timed out after %ss", name, timeout)
        raise SlurmError(f"{name} timed out after {int(timeout)}s") from exc
    except OSError as exc:
        raise SlurmError(f"{name} could not be started: {exc}") from exc

    logger.debug(
        "%s finished in %.2fs (exit %s)", name, time.monotonic() - started, proc.returncode
    )
    if proc.returncode != 0:
        detail = (proc.stderr or proc.stdout).strip().splitlines()
        message = detail[-1] if detail else f"exit code {proc.returncode}"
        logger.warning("%s failed: %s", name, message)
        raise SlurmError(f"{name} failed: {message}")
    return proc.stdout


class TTLCache:
    """Thread-safe cache where concurrent callers share one in-flight load.

    Without this every browser tab and every view would start its own
    squeue/sacct process on each poll.
    """

    def __init__(self) -> None:
        self._values: Dict[Hashable, Tuple[float, Any]] = {}
        self._locks: Dict[Hashable, threading.Lock] = {}
        self._guard = threading.Lock()

    def _lock_for(self, key: Hashable) -> threading.Lock:
        with self._guard:
            return self._locks.setdefault(key, threading.Lock())

    def peek(self, key: Hashable, ttl: float) -> Optional[Any]:
        """Return a fresh cached value without loading, else None."""
        entry = self._values.get(key)
        if entry and time.monotonic() - entry[0] < ttl:
            return entry[1]
        return None

    def get(
        self, key: Hashable, ttl: float, loader: Callable[[], Any]
    ) -> Tuple[Any, Optional[str]]:
        """Return (value, error).

        On a failed load the previous value is returned together with the error
        message, so the UI can keep showing stale data and say why.
        """
        fresh = self.peek(key, ttl)
        if fresh is not None:
            return fresh, None
        with self._lock_for(key):
            fresh = self.peek(key, ttl)
            if fresh is not None:
                return fresh, None
            try:
                value = loader()
            except SlurmError as exc:
                stale = self._values.get(key)
                return (stale[1] if stale else None), str(exc)
            self._values[key] = (time.monotonic(), value)
            return value, None

    def invalidate(self, prefix: Optional[str] = None) -> None:
        """Drop all entries, or those whose key tuple starts with `prefix`."""
        with self._guard:
            if prefix is None:
                self._values.clear()
                return
            for key in [k for k in self._values if isinstance(k, tuple) and k[0] == prefix]:
                del self._values[key]

    def keys(self) -> List[Hashable]:
        return list(self._values)

"""Configuration management for Slurm Dashboard."""

from __future__ import annotations

import argparse
import getpass
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional

# Default pattern matches: {log_root}/{job_name}/job.{out|err}.{job_id}
DEFAULT_LOG_PATTERN = "{name}/job.{stream}.{id}"


@dataclass
class LogPattern:
    """
    Configurable log file path pattern.

    Template variables:
        {name}   - Job/script name
        {id}     - Job ID
        {stream} - "out" for stdout, "err" for stderr

    Examples:
        "{name}/job.{stream}.{id}"      - Default (subdir per job name)
        "slurm-{id}.{stream}"           - Slurm default style
        "{name}-{id}.{stream}"          - Flat with job name
        "{name}/{id}/std{stream}"       - Nested by job ID
    """

    pattern: str = DEFAULT_LOG_PATTERN

    def format_path(self, log_root: Path, name: str, job_id: str, stream: str) -> Path:
        """
        Format the pattern into a concrete file path.

        Args:
            log_root: Base directory for logs
            name: Job/script name
            job_id: Slurm job ID
            stream: "out" for stdout, "err" for stderr

        Returns:
            Resolved Path to the log file
        """
        formatted = self.pattern.format(name=name, id=job_id, stream=stream)
        return (log_root / formatted).resolve()

    def to_glob_pattern(self) -> str:
        """
        Convert the pattern to a glob pattern for discovering log files.

        Replaces template variables with wildcards.
        """
        # Replace known variables with wildcards
        glob_pat = self.pattern
        glob_pat = glob_pat.replace("{name}", "*")
        glob_pat = glob_pat.replace("{id}", "*")
        glob_pat = glob_pat.replace("{stream}", "*")
        return glob_pat

    def extract_job_info(self, log_root: Path, file_path: Path) -> Optional[dict]:
        """
        Extract job name and ID from a file path based on the pattern.

        Returns dict with 'name', 'id', 'stream' or None if no match.
        If pattern has no {name}, returns job_id as name.
        """
        try:
            rel_path = file_path.relative_to(log_root)
        except ValueError:
            return None

        match = self._regex().match(rel_path.as_posix())
        if match:
            result = match.groupdict()
            # If pattern has no {name}, use job_id as the name
            if "name" not in result:
                result["name"] = result.get("id", "unknown")
            return result
        return None

    def _regex(self) -> "re.Pattern[str]":
        """Compile the pattern once; array task IDs ("123_4") count as IDs."""
        compiled = getattr(self, "_compiled", None)
        if compiled is None:
            regex = re.escape(self.pattern)
            regex = regex.replace(r"\{name\}", r"(?P<name>[^/]+?)")
            regex = regex.replace(r"\{id\}", r"(?P<id>\d+(?:_\d+)?)")
            regex = regex.replace(r"\{stream\}", r"(?P<stream>out|err)")
            compiled = re.compile(f"^{regex}$")
            object.__setattr__(self, "_compiled", compiled)
        return compiled

    def validate(self) -> List[str]:
        """
        Validate the pattern has required variables.

        Returns list of error messages (empty if valid).
        """
        errors = []
        if "{id}" not in self.pattern:
            errors.append("Pattern must contain {id} placeholder")
        if "{stream}" not in self.pattern:
            errors.append("Pattern must contain {stream} placeholder")
        return errors


@dataclass
class Config:
    """Application configuration."""

    host: str = "127.0.0.1"
    port: int = 5000
    log_root: Path = field(default_factory=lambda: Path.home() / "slurm-logs")
    user: str = ""
    refresh_cache: int = 20
    log_pattern: LogPattern = field(default_factory=LogPattern)
    all_users: bool = False
    history_days: int = 7
    auth: bool = True
    token: Optional[str] = None
    state_dir: Path = field(default_factory=lambda: default_state_dir())

    def __post_init__(self) -> None:
        # Default user to current user if not specified
        if not self.user:
            self.user = getpass.getuser()
        self.history_days = max(1, min(90, self.history_days))

    @property
    def scope_user(self) -> Optional[str]:
        """User whose jobs are shown; None means every user."""
        return None if self.all_users else self.user

    @classmethod
    def from_args(cls, args: argparse.Namespace) -> "Config":
        """Create config from parsed arguments."""
        return cls(
            host=args.host,
            port=args.port,
            log_root=args.log_root.expanduser().resolve(),
            user=args.user,
            refresh_cache=args.refresh_cache,
            log_pattern=LogPattern(pattern=args.log_pattern),
            all_users=args.all_users,
            history_days=args.history_days,
            auth=not args.no_auth,
            token=args.token,
        )


def default_state_dir() -> Path:
    """Directory for the access token and saved templates."""
    base = os.environ.get("XDG_CONFIG_HOME") or str(Path.home() / ".config")
    return Path(base) / "slurm-dashboard"


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Slurm Dashboard - A lightweight web dashboard for monitoring Slurm jobs",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Log pattern examples:
  {name}/job.{stream}.{id}    Default (subdir per job name)
  slurm-{id}.{stream}         Slurm default style (flat)
  {name}-{id}.{stream}        Flat with job name prefix
  {name}/{id}/std{stream}     Nested by job ID

Template variables:
  {name}   - Job/script name
  {id}     - Job ID (digits only)
  {stream} - "out" for stdout, "err" for stderr
""",
    )
    parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="Host to bind to (default: 127.0.0.1)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=5000,
        help="Port to bind to (default: 5000)",
    )
    parser.add_argument(
        "--log-root",
        type=Path,
        default=Path.home() / "slurm-logs",
        help="Root directory for Slurm log files (default: ~/slurm-logs)",
    )
    parser.add_argument(
        "--log-pattern",
        default=DEFAULT_LOG_PATTERN,
        help=f"Log file path pattern relative to log-root (default: {DEFAULT_LOG_PATTERN})",
    )
    parser.add_argument(
        "--user",
        default=getpass.getuser(),
        help="Slurm username to filter jobs (default: current user)",
    )
    parser.add_argument(
        "--refresh-cache",
        type=int,
        default=20,
        help="Seconds to reuse accounting (sacct) results and the log index (default: 20)",
    )
    parser.add_argument(
        "--all-users",
        action="store_true",
        help="Show every user's jobs (read-only for jobs that are not yours)",
    )
    parser.add_argument(
        "--history-days",
        type=int,
        default=7,
        help="Days of accounting history shown in the job list (default: 7, max: 90)",
    )
    parser.add_argument(
        "--token",
        default=None,
        help="Access token to require instead of the generated one "
        "(or set SLURM_DASHBOARD_TOKEN)",
    )
    parser.add_argument(
        "--no-auth",
        action="store_true",
        help="Disable the access token. Anyone who can reach the port can then "
        "submit and cancel jobs as you",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="Log every Slurm command")
    return parser.parse_args(argv)


# Global config instance, set during app initialization
_config: Optional[Config] = None


def get_config() -> Config:
    """Get the current configuration."""
    if _config is None:
        raise RuntimeError("Configuration not initialized. Call set_config() first.")
    return _config


def set_config(config: Config) -> None:
    """Set the global configuration."""
    global _config
    _config = config

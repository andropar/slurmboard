"""Configuration management for Slurm Dashboard."""
from __future__ import annotations

import argparse
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

        # Convert pattern to regex
        regex_pattern = re.escape(self.pattern)
        regex_pattern = regex_pattern.replace(r"\{name\}", r"(?P<name>[^/]+)")
        regex_pattern = regex_pattern.replace(r"\{id\}", r"(?P<id>\d+)")
        regex_pattern = regex_pattern.replace(r"\{stream\}", r"(?P<stream>out|err)")
        regex_pattern = f"^{regex_pattern}$"

        match = re.match(regex_pattern, str(rel_path))
        if match:
            result = match.groupdict()
            # If pattern has no {name}, use job_id as the name
            if "name" not in result:
                result["name"] = result.get("id", "unknown")
            return result
        return None

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

    def __post_init__(self):
        # Default user to current user if not specified
        if not self.user:
            import getpass
            self.user = getpass.getuser()

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
        )


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    import getpass

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
        help="Cache refresh interval in seconds (default: 20)",
    )
    return parser.parse_args()


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

"""Entry point for running slurm_dashboard as a module."""

from __future__ import annotations

import sys

from slurm_dashboard.app import run_app
from slurm_dashboard.config import Config, parse_args


def main() -> None:
    """Main entry point."""
    args = parse_args()
    try:
        run_app(Config.from_args(args), verbose=args.verbose)
    except ValueError as exc:
        sys.exit(f"slurm-dashboard: {exc}")


if __name__ == "__main__":
    main()

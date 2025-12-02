"""Entry point for running slurm_dashboard as a module."""

from slurm_dashboard.app import run_app
from slurm_dashboard.config import Config, parse_args


def main() -> None:
    """Main entry point."""
    args = parse_args()
    config = Config.from_args(args)
    run_app(config)


if __name__ == "__main__":
    main()

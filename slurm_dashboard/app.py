"""Flask application factory for Slurm Dashboard."""

import sys
from pathlib import Path

from flask import Flask, render_template

from slurm_dashboard.config import Config, set_config
from slurm_dashboard.routes.api import api
from slurm_dashboard.routes.sse import sse


def validate_config(config: Config) -> list[str]:
    """
    Validate configuration and return list of warnings/errors.

    Returns list of warning messages. Fatal errors are raised as exceptions.
    """
    warnings = []

    # Validate log pattern
    pattern_errors = config.log_pattern.validate()
    if pattern_errors:
        raise ValueError(f"Invalid log pattern: {'; '.join(pattern_errors)}")

    # Check if log root exists
    if not config.log_root.exists():
        warnings.append(f"Log root directory does not exist: {config.log_root}")

    # Check if pattern matches any files
    if config.log_root.exists():
        glob_pattern = config.log_pattern.to_glob_pattern()
        matches = list(config.log_root.glob(glob_pattern))
        if not matches:
            warnings.append(
                f"No log files found matching pattern '{config.log_pattern.pattern}' "
                f"in {config.log_root}"
            )

    return warnings


def create_app(config: Config) -> Flask:
    """Create and configure the Flask application."""
    # Validate configuration
    warnings = validate_config(config)
    for warning in warnings:
        print(f"Warning: {warning}", file=sys.stderr)

    # Set global config
    set_config(config)

    # Ensure log root exists
    config.log_root.mkdir(parents=True, exist_ok=True)

    # Create Flask app with correct template and static paths
    package_dir = Path(__file__).parent
    app = Flask(
        __name__,
        template_folder=str(package_dir / "templates"),
        static_folder=str(package_dir / "static"),
    )

    # Register blueprints
    app.register_blueprint(api)
    app.register_blueprint(sse)

    # Index route
    @app.route("/")
    def index() -> str:
        return render_template("index.html")

    return app


def run_app(config: Config) -> None:
    """Create and run the application."""
    app = create_app(config)
    print(f"Starting Slurm Dashboard on http://{config.host}:{config.port}")
    print(f"Log root: {config.log_root}")
    print(f"Log pattern: {config.log_pattern.pattern}")
    print(f"User: {config.user}")
    app.run(host=config.host, port=config.port, debug=False)

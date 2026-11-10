"""Flask application factory for Slurm Dashboard."""

from __future__ import annotations

import logging
import shutil
from pathlib import Path
from typing import List, Optional

from flask import Flask, render_template

from slurm_dashboard import __version__
from slurm_dashboard.auth import install_auth, load_or_create_token
from slurm_dashboard.config import Config, set_config
from slurm_dashboard.routes.api import api, describe
from slurm_dashboard.routes.sse import sse
from slurm_dashboard.services.logs import LogIndex
from slurm_dashboard.services.slurm import Slurm
from slurm_dashboard.services.templates import TemplateStore

logger = logging.getLogger(__name__)


def validate_config(config: Config) -> List[str]:
    """Validate configuration and return a list of warnings.

    Raises:
        ValueError: if the log pattern is unusable.
    """
    pattern_errors = config.log_pattern.validate()
    if pattern_errors:
        raise ValueError(f"Invalid log pattern: {'; '.join(pattern_errors)}")

    warnings = []
    for tool in ("squeue", "sacct"):
        if shutil.which(tool) is None:
            warnings.append(f"'{tool}' is not on PATH; job data will be unavailable")
    if not config.log_root.is_dir():
        warnings.append(
            f"Log root {config.log_root} does not exist; logs are located from Slurm's "
            "own records only"
        )
    return warnings


def create_app(config: Config, token: Optional[str] = None) -> Flask:
    """Create and configure the Flask application.

    Args:
        config: Dashboard configuration.
        token: Access token to require; None with ``config.auth`` False disables auth.
    """
    for warning in validate_config(config):
        logger.warning(warning)
    set_config(config)

    package_dir = Path(__file__).parent
    app = Flask(
        __name__,
        template_folder=str(package_dir / "templates"),
        static_folder=str(package_dir / "static"),
    )
    app.config["MAX_CONTENT_LENGTH"] = 2 * 1024 * 1024
    app.extensions["slurm"] = Slurm(config.scope_user, accounting_ttl=config.refresh_cache)
    app.extensions["log_index"] = LogIndex(
        config.log_root, config.log_pattern, ttl=config.refresh_cache
    )
    app.extensions["templates"] = TemplateStore(config.state_dir / "templates.json")

    if config.auth and token is None:
        token = load_or_create_token(config.state_dir, config.token)
    install_auth(app, token if config.auth else None, config.state_dir / "token")
    app.extensions["token"] = token if config.auth else None

    app.register_blueprint(api)
    app.register_blueprint(sse)

    @app.route("/")
    def index() -> str:
        return render_template("index.html", settings=describe(config), version=__version__)

    return app


def run_app(config: Config, verbose: bool = False) -> None:
    """Create and run the application."""
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )
    if not verbose:
        # One line per poll request would bury everything else.
        logging.getLogger("werkzeug").setLevel(logging.WARNING)

    app = create_app(config)
    token = app.extensions["token"]
    url = f"http://{config.host}:{config.port}/"
    logger.info("Slurm Dashboard %s", __version__)
    logger.info("Jobs: %s", "all users (read-only for others)" if config.all_users else config.user)
    logger.info("Log root: %s (pattern %s)", config.log_root, config.log_pattern.pattern)
    if token:
        logger.info("Open: %s?token=%s", url, token)
    else:
        logger.warning("Authentication is OFF: anyone who can reach %s can act as you", url)
        logger.info("Open: %s", url)
    if config.host not in {"127.0.0.1", "localhost", "::1"}:
        logger.warning("Listening on %s: traffic is unencrypted HTTP", config.host)
    app.run(host=config.host, port=config.port, debug=False, threaded=True)

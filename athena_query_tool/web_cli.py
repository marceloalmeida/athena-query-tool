"""Command-line entry point for the Athena Query Tool web server."""

import argparse
import logging
import sys

from .exceptions import ConfigurationError
from .web import create_app

logger = logging.getLogger(__name__)


def main():
    """Entry point: parse --config argument, load config, start Flask server."""
    parser = argparse.ArgumentParser(
        description="Start the Athena Query Tool web server"
    )
    parser.add_argument(
        "--config",
        default="config.yaml",
        help="Path to configuration file (default: config.yaml)",
    )
    args = parser.parse_args()

    try:
        app = create_app(args.config)
    except ConfigurationError as exc:
        logger.error("Configuration error: %s", exc)
        sys.exit(1)

    app.run(debug=True)


if __name__ == "__main__":
    main()

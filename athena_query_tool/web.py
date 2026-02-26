"""Flask web application for Athena Query Tool."""

import os

from flask import Flask, jsonify, request, send_from_directory

from .auth import AuthenticationManager
from .cache import CacheManager
from .config import ConfigurationManager
from .exceptions import AuthenticationError, QueryExecutionError
from .executor import QueryExecutor
from .formatter import ResultFormatter
from .retry import RetryHandler


def create_app(config_path: str) -> Flask:
    """Create and configure the Flask application.

    Args:
        config_path: Path to YAML configuration file

    Returns:
        Configured Flask app instance
    """
    static_folder = os.path.join(os.path.dirname(__file__), "static")
    app = Flask(__name__, static_folder=static_folder, static_url_path="")

    # Load configuration
    config = ConfigurationManager.load_config(config_path)

    # Authenticate and create AWS clients
    auth_manager = AuthenticationManager()
    session = auth_manager.get_session(
        profile=config.aws.profile,
        region=config.aws.region,
    )
    athena_client = session.client("athena")
    s3_client = session.client("s3")

    # Set up optional cache manager
    cache_manager = None
    if config.cache.enabled:
        cache_manager = CacheManager(config.cache, s3_client)

    # Set up query executor
    retry_handler = RetryHandler()
    executor = QueryExecutor(
        athena_client=athena_client,
        config=config.athena,
        retry_handler=retry_handler,
        s3_client=s3_client,
        cache_manager=cache_manager,
        query_prefix_config=config.query_prefix,
    )

    # Store instances on app for use in route handlers
    app.config["APP_CONFIG"] = config
    app.config["QUERY_EXECUTOR"] = executor
    app.config["RESULT_FORMATTER"] = ResultFormatter()

    # Catch-all route to serve the SPA
    @app.route("/")
    def index():
        return send_from_directory(app.static_folder, "index.html")

    @app.route("/api/config")
    def get_config():
        cfg = app.config["APP_CONFIG"]
        return jsonify({
            "success": True,
            "data": {
                "region": cfg.aws.region,
                "database": cfg.athena.database,
                "workgroup": cfg.athena.workgroup,
            },
        })

    @app.route("/api/query", methods=["POST"])
    def execute_query():
        if not request.is_json:
            return jsonify({"success": False, "error": "Request must be JSON"}), 400

        data = request.get_json()
        sql = data.get("sql", "")
        if not isinstance(sql, str) or not sql.strip():
            return jsonify({"success": False, "error": "Missing or empty 'sql' field"}), 400

        executor = app.config["QUERY_EXECUTOR"]

        # Determine from_cache before execution
        from_cache = False
        if executor.cache_manager:
            cached = executor.cache_manager.get_cached_execution(sql)
            if cached is not None:
                from_cache = True

        result = executor.execute_query(sql)

        return jsonify({
            "success": True,
            "data": {
                "columns": [{"name": col.name, "type": col.type} for col in result.columns],
                "rows": result.rows,
                "row_count": result.row_count,
                "from_cache": from_cache,
            },
        })

    @app.errorhandler(AuthenticationError)
    def handle_auth_error(e):
        return jsonify({"success": False, "error": str(e)}), 401

    @app.errorhandler(QueryExecutionError)
    def handle_query_error(e):
        return jsonify({"success": False, "error": str(e)}), 500

    @app.errorhandler(Exception)
    def handle_unexpected_error(e):
        return jsonify({"success": False, "error": "Internal server error"}), 500

    return app

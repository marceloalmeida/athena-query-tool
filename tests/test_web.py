"""Unit tests for the Flask web backend API."""

import sys
from unittest.mock import Mock, patch, MagicMock

import pytest

from athena_query_tool.config import (
    Config,
    AWSConfig,
    AthenaConfig,
    CacheConfig,
    OutputConfig,
    QueryConfig,
)
from athena_query_tool.exceptions import (
    AuthenticationError,
    ConfigurationError,
    QueryExecutionError,
)
from athena_query_tool.executor import Column, QueryResult


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_config():
    """Create a sample Config used by the app."""
    return Config(
        aws=AWSConfig(profile=None, region="us-west-2"),
        athena=AthenaConfig(
            database="test_db",
            workgroup="primary",
            output_location="s3://bucket/results/",
        ),
        cache=CacheConfig(enabled=False),
        output=OutputConfig(format="table", file=None),
        queries=[QueryConfig(name="q1", sql="SELECT 1")],
    )


@pytest.fixture
def sample_result():
    """Create a sample QueryResult."""
    return QueryResult(
        columns=[Column(name="id", type="integer"), Column(name="name", type="varchar")],
        rows=[[1, "Alice"], [2, "Bob"]],
        row_count=2,
    )


@pytest.fixture
def app(sample_config):
    """Create a Flask app with mocked AWS dependencies.

    Patches ConfigurationManager, AuthenticationManager, and related
    constructors so that ``create_app`` never touches real AWS resources.
    """
    with patch("athena_query_tool.web.ConfigurationManager") as mock_cfg_mgr, \
         patch("athena_query_tool.web.AuthenticationManager") as mock_auth_mgr, \
         patch("athena_query_tool.web.RetryHandler"), \
         patch("athena_query_tool.web.QueryExecutor") as mock_executor_cls:

        mock_cfg_mgr.load_config.return_value = sample_config

        mock_session = Mock()
        mock_auth_mgr.return_value.get_session.return_value = mock_session
        mock_session.client.return_value = Mock()

        mock_executor = Mock()
        mock_executor.cache_manager = None
        mock_executor_cls.return_value = mock_executor

        from athena_query_tool.web import create_app
        flask_app = create_app("dummy.yaml")

        # Expose the mock executor so individual tests can configure it
        flask_app._mock_executor = mock_executor
        flask_app.config["TESTING"] = True
        yield flask_app


@pytest.fixture
def client(app):
    """Flask test client."""
    return app.test_client()


# ---------------------------------------------------------------------------
# GET / — serves HTML
# ---------------------------------------------------------------------------

def test_get_root_serves_html(client):
    """GET / should return the index.html page."""
    resp = client.get("/")
    assert resp.status_code == 200
    assert b"<!DOCTYPE html>" in resp.data or b"<html" in resp.data


# ---------------------------------------------------------------------------
# GET /api/config — returns region, database, workgroup
# ---------------------------------------------------------------------------

def test_get_config_returns_correct_values(client):
    """GET /api/config should return region, database, workgroup from config."""
    resp = client.get("/api/config")
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["success"] is True
    assert body["data"]["region"] == "us-west-2"
    assert body["data"]["database"] == "test_db"
    assert body["data"]["workgroup"] == "primary"


# ---------------------------------------------------------------------------
# POST /api/query — valid SQL
# ---------------------------------------------------------------------------

def test_post_query_valid_sql(client, app, sample_result):
    """POST /api/query with valid SQL returns columns, rows, row_count, from_cache."""
    app._mock_executor.execute_query.return_value = sample_result

    resp = client.post("/api/query", json={"sql": "SELECT * FROM t"})
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["success"] is True
    data = body["data"]
    assert data["columns"] == [
        {"name": "id", "type": "integer"},
        {"name": "name", "type": "varchar"},
    ]
    assert data["rows"] == [[1, "Alice"], [2, "Bob"]]
    assert data["row_count"] == 2
    assert data["from_cache"] is False


# ---------------------------------------------------------------------------
# POST /api/query — missing sql field → 400
# ---------------------------------------------------------------------------

def test_post_query_missing_sql_field(client):
    """POST /api/query with missing 'sql' field returns 400."""
    resp = client.post("/api/query", json={"query": "SELECT 1"})
    assert resp.status_code == 400
    body = resp.get_json()
    assert body["success"] is False
    assert "sql" in body["error"].lower()


# ---------------------------------------------------------------------------
# POST /api/query — empty sql → 400
# ---------------------------------------------------------------------------

def test_post_query_empty_sql(client):
    """POST /api/query with empty sql returns 400."""
    resp = client.post("/api/query", json={"sql": ""})
    assert resp.status_code == 400
    body = resp.get_json()
    assert body["success"] is False


def test_post_query_whitespace_only_sql(client):
    """POST /api/query with whitespace-only sql returns 400."""
    resp = client.post("/api/query", json={"sql": "   "})
    assert resp.status_code == 400
    body = resp.get_json()
    assert body["success"] is False


# ---------------------------------------------------------------------------
# POST /api/query — non-JSON body → 400
# ---------------------------------------------------------------------------

def test_post_query_non_json_body(client):
    """POST /api/query with non-JSON body returns 400."""
    resp = client.post("/api/query", data="not json", content_type="text/plain")
    assert resp.status_code == 400
    body = resp.get_json()
    assert body["success"] is False
    assert "json" in body["error"].lower()


# ---------------------------------------------------------------------------
# Authentication error → 401
# ---------------------------------------------------------------------------

def test_authentication_error_returns_401(client, app):
    """AuthenticationError raised during query execution returns 401."""
    app._mock_executor.execute_query.side_effect = AuthenticationError("Bad creds")

    resp = client.post("/api/query", json={"sql": "SELECT 1"})
    assert resp.status_code == 401
    body = resp.get_json()
    assert body["success"] is False
    assert "Bad creds" in body["error"]


# ---------------------------------------------------------------------------
# Query execution error → 500
# ---------------------------------------------------------------------------

def test_query_execution_error_returns_500(client, app):
    """QueryExecutionError raised during query execution returns 500."""
    app._mock_executor.execute_query.side_effect = QueryExecutionError("Syntax error")

    resp = client.post("/api/query", json={"sql": "BAD SQL"})
    assert resp.status_code == 500
    body = resp.get_json()
    assert body["success"] is False
    assert "Syntax error" in body["error"]


# ---------------------------------------------------------------------------
# Cache integration — from_cache is true on cache hit
# ---------------------------------------------------------------------------

def test_cache_hit_sets_from_cache_true(sample_config, sample_result):
    """When CacheManager returns a cached execution, from_cache should be True."""
    sample_config.cache.enabled = True

    with patch("athena_query_tool.web.ConfigurationManager") as mock_cfg_mgr, \
         patch("athena_query_tool.web.AuthenticationManager") as mock_auth_mgr, \
         patch("athena_query_tool.web.RetryHandler"), \
         patch("athena_query_tool.web.CacheManager") as mock_cache_cls, \
         patch("athena_query_tool.web.QueryExecutor") as mock_executor_cls:

        mock_cfg_mgr.load_config.return_value = sample_config

        mock_session = Mock()
        mock_auth_mgr.return_value.get_session.return_value = mock_session
        mock_session.client.return_value = Mock()

        # Set up cache manager mock
        mock_cache = Mock()
        mock_cache.get_cached_execution.return_value = Mock()  # non-None → cache hit
        mock_cache_cls.return_value = mock_cache

        mock_executor = Mock()
        mock_executor.cache_manager = mock_cache
        mock_executor.execute_query.return_value = sample_result
        mock_executor_cls.return_value = mock_executor

        from athena_query_tool.web import create_app
        flask_app = create_app("dummy.yaml")
        flask_app.config["TESTING"] = True
        client = flask_app.test_client()

        resp = client.post("/api/query", json={"sql": "SELECT 1"})
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["data"]["from_cache"] is True


# ---------------------------------------------------------------------------
# Startup exits with non-zero code on bad config
# ---------------------------------------------------------------------------

def test_startup_exits_on_bad_config():
    """web_cli.main() should sys.exit(1) when config loading fails."""
    with patch("sys.argv", ["web-server", "--config", "nonexistent.yaml"]), \
         patch(
             "athena_query_tool.web_cli.create_app",
             side_effect=ConfigurationError("File not found"),
         ):
        with pytest.raises(SystemExit) as exc_info:
            from athena_query_tool.web_cli import main
            main()
        assert exc_info.value.code == 1

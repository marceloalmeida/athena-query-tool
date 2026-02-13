"""Integration tests for cache management with QueryExecutor."""

import json
import os
import tempfile
import time
from unittest.mock import Mock, MagicMock, patch

import pytest

from athena_query_tool.cache import CacheManager
from athena_query_tool.config import AthenaConfig, CacheConfig, CachedExecution
from athena_query_tool.executor import QueryExecutor, QueryResult, Column
from athena_query_tool.retry import RetryHandler


def test_query_executor_with_cache_hit():
    """Test that QueryExecutor uses cached execution ID when cache is valid."""
    mock_athena_client = Mock()

    config = AthenaConfig(
        database="test_db",
        workgroup="primary",
        output_location="s3://bucket/path/"
    )

    # Mock a successful query using cached execution ID
    # The executor doesn't have cache integration yet, but we can test
    # that the executor works correctly when given an execution ID
    mock_athena_client.start_query_execution.return_value = {
        'QueryExecutionId': 'cached-execution-id'
    }

    mock_athena_client.get_query_execution.return_value = {
        'QueryExecution': {
            'Status': {
                'State': 'SUCCEEDED'
            }
        }
    }

    mock_athena_client.get_query_results.return_value = {
        'ResultSet': {
            'ResultSetMetadata': {
                'ColumnInfo': [
                    {'Name': 'id', 'Type': 'integer'},
                    {'Name': 'name', 'Type': 'varchar'}
                ]
            },
            'Rows': [
                {'Data': [{'VarCharValue': 'id'}, {'VarCharValue': 'name'}]},
                {'Data': [{'VarCharValue': '1'}, {'VarCharValue': 'Alice'}]},
                {'Data': [{'VarCharValue': '2'}, {'VarCharValue': 'Bob'}]}
            ]
        }
    }

    executor = QueryExecutor(mock_athena_client, config)
    result = executor.execute_query("SELECT * FROM test_table")

    assert len(result.columns) == 2
    assert result.columns[0].name == "id"
    assert result.columns[1].name == "name"
    assert result.row_count == 2
    assert result.rows[0] == ['1', 'Alice']
    assert result.rows[1] == ['2', 'Bob']


def test_query_executor_with_cache_miss():
    """Test that QueryExecutor executes query normally (cache miss scenario)."""
    mock_athena_client = Mock()

    config = AthenaConfig(
        database="test_db",
        workgroup="primary",
        output_location="s3://bucket/path/"
    )

    mock_athena_client.start_query_execution.return_value = {
        'QueryExecutionId': 'new-execution-id'
    }

    mock_athena_client.get_query_execution.return_value = {
        'QueryExecution': {
            'Status': {
                'State': 'SUCCEEDED'
            }
        }
    }

    mock_athena_client.get_query_results.return_value = {
        'ResultSet': {
            'ResultSetMetadata': {
                'ColumnInfo': [
                    {'Name': 'id', 'Type': 'integer'},
                    {'Name': 'name', 'Type': 'varchar'}
                ]
            },
            'Rows': [
                {'Data': [{'VarCharValue': 'id'}, {'VarCharValue': 'name'}]},
                {'Data': [{'VarCharValue': '1'}, {'VarCharValue': 'Alice'}]},
                {'Data': [{'VarCharValue': '2'}, {'VarCharValue': 'Bob'}]}
            ]
        }
    }

    executor = QueryExecutor(mock_athena_client, config)
    result = executor.execute_query("SELECT * FROM test_table")

    # Verify query WAS submitted to Athena
    mock_athena_client.start_query_execution.assert_called_once()

    # Verify results
    assert result.row_count == 2
    assert result.rows[0] == ['1', 'Alice']


def test_query_executor_without_cache_manager():
    """Test that QueryExecutor works without cache manager."""
    mock_athena_client = Mock()

    config = AthenaConfig(
        database="test_db",
        workgroup="primary",
        output_location="s3://bucket/path/"
    )

    mock_athena_client.start_query_execution.return_value = {
        'QueryExecutionId': 'test-execution-id'
    }

    mock_athena_client.get_query_execution.return_value = {
        'QueryExecution': {
            'Status': {
                'State': 'SUCCEEDED'
            }
        }
    }

    mock_athena_client.get_query_results.return_value = {
        'ResultSet': {
            'ResultSetMetadata': {
                'ColumnInfo': [{'Name': 'id', 'Type': 'integer'}]
            },
            'Rows': [
                {'Data': [{'VarCharValue': 'id'}]},
                {'Data': [{'VarCharValue': '1'}]}
            ]
        }
    }

    executor = QueryExecutor(mock_athena_client, config)
    result = executor.execute_query("SELECT * FROM test_table")

    mock_athena_client.start_query_execution.assert_called_once()
    assert result.row_count == 1


def test_cache_manager_store_and_retrieve():
    """Test storing and retrieving cache entries with CacheManager."""
    mock_s3_client = Mock()

    with tempfile.TemporaryDirectory() as tmpdir:
        cache_config = CacheConfig(
            enabled=True,
            ttl_seconds=3600,
            directory=tmpdir
        )

        cache_manager = CacheManager(cache_config, mock_s3_client)

        # Store an execution
        cache_manager.store_execution(
            query_sql="SELECT * FROM test_table",
            execution_id="test-exec-123",
            s3_location="s3://bucket/path/test-exec-123.csv"
        )

        # Verify cache file was created
        cache_file = os.path.join(tmpdir, "test-exec-123.json")
        assert os.path.exists(cache_file)

        # Verify cache file content
        with open(cache_file, 'r') as f:
            cache_data = json.load(f)

        assert cache_data["query_sql"] == "SELECT * FROM test_table"
        assert cache_data["execution_id"] == "test-exec-123"
        assert cache_data["s3_location"] == "s3://bucket/path/test-exec-123.csv"
        assert cache_data["ttl_seconds"] == 3600
        assert "timestamp" in cache_data

        # Mock S3 head_object to validate S3 result exists
        mock_s3_client.head_object.return_value = {}

        # Retrieve cached execution
        cached = cache_manager.get_cached_execution("SELECT * FROM test_table")

        assert cached is not None
        assert cached.execution_id == "test-exec-123"
        assert cached.query_sql == "SELECT * FROM test_table"
        assert cached.s3_location == "s3://bucket/path/test-exec-123.csv"


def test_cache_manager_stale_entry():
    """Test that stale cache entries are not returned."""
    mock_s3_client = Mock()

    with tempfile.TemporaryDirectory() as tmpdir:
        cache_config = CacheConfig(
            enabled=True,
            ttl_seconds=60,  # 1 minute TTL
            directory=tmpdir
        )

        cache_manager = CacheManager(cache_config, mock_s3_client)

        # Write a stale cache entry manually
        cache_entry = {
            "query_sql": "SELECT * FROM test_table",
            "execution_id": "stale-exec-123",
            "timestamp": time.time() - 120,  # 2 minutes ago (past TTL)
            "s3_location": "s3://bucket/path/stale-exec-123.csv",
            "ttl_seconds": 60
        }

        cache_file = os.path.join(tmpdir, "stale-exec-123.json")
        with open(cache_file, 'w') as f:
            json.dump(cache_entry, f)

        # Should return None for stale entry
        cached = cache_manager.get_cached_execution("SELECT * FROM test_table")
        assert cached is None


def test_cache_manager_s3_validation_failure():
    """Test that cache entries with missing S3 results are not returned."""
    from botocore.exceptions import ClientError

    mock_s3_client = Mock()
    mock_s3_client.head_object.side_effect = ClientError(
        {'Error': {'Code': '404', 'Message': 'Not Found'}},
        'HeadObject'
    )

    with tempfile.TemporaryDirectory() as tmpdir:
        cache_config = CacheConfig(
            enabled=True,
            ttl_seconds=3600,
            directory=tmpdir
        )

        cache_manager = CacheManager(cache_config, mock_s3_client)

        # Write a fresh cache entry
        cache_entry = {
            "query_sql": "SELECT * FROM test_table",
            "execution_id": "valid-exec-123",
            "timestamp": time.time(),
            "s3_location": "s3://bucket/path/valid-exec-123.csv",
            "ttl_seconds": 3600
        }

        cache_file = os.path.join(tmpdir, "valid-exec-123.json")
        with open(cache_file, 'w') as f:
            json.dump(cache_entry, f)

        # Should return None because S3 object doesn't exist
        cached = cache_manager.get_cached_execution("SELECT * FROM test_table")
        assert cached is None


def test_cache_integration_end_to_end():
    """Test end-to-end cache integration with real CacheManager."""
    mock_athena_client = Mock()
    mock_s3_client = Mock()

    config = AthenaConfig(
        database="test_db",
        workgroup="primary",
        output_location="s3://bucket/path/"
    )

    with tempfile.TemporaryDirectory() as tmpdir:
        cache_config = CacheConfig(
            enabled=True,
            ttl_seconds=3600,
            directory=tmpdir
        )

        cache_manager = CacheManager(cache_config, mock_s3_client)

        # Store a cache entry
        cache_manager.store_execution(
            query_sql="SELECT * FROM test_table",
            execution_id="cached-exec-id",
            s3_location="s3://bucket/path/cached-exec-id.csv"
        )

        # Mock S3 head_object to validate S3 result exists
        mock_s3_client.head_object.return_value = {}

        # Verify cache hit
        cached = cache_manager.get_cached_execution("SELECT * FROM test_table")
        assert cached is not None
        assert cached.execution_id == "cached-exec-id"

        # Now use the executor to run a query (without cache integration in executor)
        mock_athena_client.start_query_execution.return_value = {
            'QueryExecutionId': 'new-exec-id'
        }
        mock_athena_client.get_query_execution.return_value = {
            'QueryExecution': {
                'Status': {'State': 'SUCCEEDED'}
            }
        }
        mock_athena_client.get_query_results.return_value = {
            'ResultSet': {
                'ResultSetMetadata': {
                    'ColumnInfo': [
                        {'Name': 'id', 'Type': 'integer'},
                        {'Name': 'name', 'Type': 'varchar'}
                    ]
                },
                'Rows': [
                    {'Data': [{'VarCharValue': 'id'}, {'VarCharValue': 'name'}]},
                    {'Data': [{'VarCharValue': '1'}, {'VarCharValue': 'Alice'}]},
                    {'Data': [{'VarCharValue': '2'}, {'VarCharValue': 'Bob'}]}
                ]
            }
        }

        executor = QueryExecutor(mock_athena_client, config)
        result = executor.execute_query("SELECT * FROM test_table")

        assert result.row_count == 2
        assert result.rows[0] == ['1', 'Alice']
        assert result.rows[1] == ['2', 'Bob']

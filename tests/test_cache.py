"""Unit tests for cache management."""

import json
import os
import tempfile
import time
from unittest.mock import MagicMock, Mock

import pytest
from botocore.exceptions import ClientError

from athena_query_tool.cache import CacheManager
from athena_query_tool.config import CacheConfig, CachedExecution


def test_cache_manager_initialization():
    """Test CacheManager initialization."""
    s3_client = MagicMock()
    cache_config = CacheConfig(enabled=True, ttl_seconds=3600, directory=".test_cache/")
    
    cache_manager = CacheManager(cache_config, s3_client)
    
    assert cache_manager.cache_config == cache_config
    assert cache_manager.s3_client == s3_client


def test_ensure_cache_directory_creates_directory():
    """Test that cache directory is created if it doesn't exist."""
    with tempfile.TemporaryDirectory() as tmpdir:
        cache_dir = os.path.join(tmpdir, "test_cache")
        cache_config = CacheConfig(enabled=True, directory=cache_dir)
        s3_client = MagicMock()
        
        # Directory should not exist yet
        assert not os.path.exists(cache_dir)
        
        cache_manager = CacheManager(cache_config, s3_client)
        
        # Directory should now exist
        assert os.path.exists(cache_dir)
        assert os.path.isdir(cache_dir)


def test_ensure_cache_directory_disabled():
    """Test that cache directory is not created when caching is disabled."""
    with tempfile.TemporaryDirectory() as tmpdir:
        cache_dir = os.path.join(tmpdir, "test_cache")
        cache_config = CacheConfig(enabled=False, directory=cache_dir)
        s3_client = MagicMock()
        
        cache_manager = CacheManager(cache_config, s3_client)
        
        # Directory should not be created when caching is disabled
        assert not os.path.exists(cache_dir)


def test_get_cache_filename():
    """Test cache filename generation from execution ID."""
    cache_config = CacheConfig(enabled=True, directory=".test_cache/")
    s3_client = MagicMock()
    cache_manager = CacheManager(cache_config, s3_client)
    
    execution_id = "abc123-def456-ghi789"
    filename = cache_manager._get_cache_filename(execution_id)
    
    assert filename == ".test_cache/abc123-def456-ghi789.json"


def test_store_execution():
    """Test storing cache entry."""
    with tempfile.TemporaryDirectory() as tmpdir:
        cache_config = CacheConfig(enabled=True, ttl_seconds=3600, directory=tmpdir)
        s3_client = MagicMock()
        cache_manager = CacheManager(cache_config, s3_client)
        
        query_sql = "SELECT * FROM table WHERE id = 1"
        execution_id = "test-execution-id"
        s3_location = "s3://bucket/path/result.csv"
        
        cache_manager.store_execution(query_sql, execution_id, s3_location)
        
        # Verify cache file was created
        cache_file = os.path.join(tmpdir, f"{execution_id}.json")
        assert os.path.exists(cache_file)
        
        # Verify cache file content
        with open(cache_file, 'r') as f:
            cache_entry = json.load(f)
        
        assert cache_entry['query_sql'] == query_sql
        assert cache_entry['execution_id'] == execution_id
        assert cache_entry['s3_location'] == s3_location
        assert cache_entry['ttl_seconds'] == 3600
        assert 'timestamp' in cache_entry
        assert isinstance(cache_entry['timestamp'], (int, float))


def test_store_execution_disabled():
    """Test that store_execution does nothing when caching is disabled."""
    with tempfile.TemporaryDirectory() as tmpdir:
        cache_config = CacheConfig(enabled=False, directory=tmpdir)
        s3_client = MagicMock()
        cache_manager = CacheManager(cache_config, s3_client)
        
        cache_manager.store_execution("SELECT 1", "exec-id", "s3://bucket/path")
        
        # No cache file should be created
        cache_file = os.path.join(tmpdir, "exec-id.json")
        assert not os.path.exists(cache_file)


def test_get_cached_execution_disabled():
    """Test that get_cached_execution returns None when caching is disabled."""
    cache_config = CacheConfig(enabled=False)
    s3_client = MagicMock()
    cache_manager = CacheManager(cache_config, s3_client)
    
    result = cache_manager.get_cached_execution("SELECT 1")
    
    assert result is None


def test_get_cached_execution_no_cache_directory():
    """Test that get_cached_execution returns None when cache directory doesn't exist."""
    with tempfile.TemporaryDirectory() as tmpdir:
        cache_dir = os.path.join(tmpdir, "nonexistent")
        cache_config = CacheConfig(enabled=True, directory=cache_dir)
        s3_client = MagicMock()
        cache_manager = CacheManager(cache_config, s3_client)
        
        # Remove the directory if it was created
        if os.path.exists(cache_dir):
            os.rmdir(cache_dir)
        
        result = cache_manager.get_cached_execution("SELECT 1")
        
        assert result is None


def test_get_cached_execution_no_matching_query():
    """Test cache miss when no matching query is found."""
    with tempfile.TemporaryDirectory() as tmpdir:
        cache_config = CacheConfig(enabled=True, ttl_seconds=3600, directory=tmpdir)
        s3_client = MagicMock()
        cache_manager = CacheManager(cache_config, s3_client)
        
        # Store a cache entry
        cache_manager.store_execution("SELECT 1", "exec-1", "s3://bucket/path1")
        
        # Try to get a different query
        result = cache_manager.get_cached_execution("SELECT 2")
        
        assert result is None


def test_get_cached_execution_valid_cache():
    """Test cache hit with valid and fresh cache entry."""
    with tempfile.TemporaryDirectory() as tmpdir:
        cache_config = CacheConfig(enabled=True, ttl_seconds=3600, directory=tmpdir)
        s3_client = MagicMock()
        
        # Mock S3 head_object to indicate object exists
        s3_client.head_object.return_value = {}
        
        cache_manager = CacheManager(cache_config, s3_client)
        
        query_sql = "SELECT * FROM table"
        execution_id = "test-exec-id"
        s3_location = "s3://bucket/path/result.csv"
        
        # Store cache entry
        cache_manager.store_execution(query_sql, execution_id, s3_location)
        
        # Retrieve cache entry
        result = cache_manager.get_cached_execution(query_sql)
        
        assert result is not None
        assert isinstance(result, CachedExecution)
        assert result.query_sql == query_sql
        assert result.execution_id == execution_id
        assert result.s3_location == s3_location
        assert result.ttl_seconds == 3600


def test_get_cached_execution_stale_cache():
    """Test cache miss when cache entry is stale."""
    with tempfile.TemporaryDirectory() as tmpdir:
        cache_config = CacheConfig(enabled=True, ttl_seconds=1, directory=tmpdir)
        s3_client = MagicMock()
        cache_manager = CacheManager(cache_config, s3_client)
        
        query_sql = "SELECT * FROM table"
        
        # Store cache entry
        cache_manager.store_execution(query_sql, "exec-id", "s3://bucket/path")
        
        # Wait for cache to become stale
        time.sleep(1.1)
        
        # Try to retrieve - should be None due to staleness
        result = cache_manager.get_cached_execution(query_sql)
        
        assert result is None


def test_get_cached_execution_s3_object_not_found():
    """Test cache miss when S3 object no longer exists."""
    with tempfile.TemporaryDirectory() as tmpdir:
        cache_config = CacheConfig(enabled=True, ttl_seconds=3600, directory=tmpdir)
        s3_client = MagicMock()
        
        # Mock S3 head_object to raise 404 error
        s3_client.head_object.side_effect = ClientError(
            {'Error': {'Code': '404', 'Message': 'Not Found'}},
            'HeadObject'
        )
        
        cache_manager = CacheManager(cache_config, s3_client)
        
        query_sql = "SELECT * FROM table"
        
        # Store cache entry
        cache_manager.store_execution(query_sql, "exec-id", "s3://bucket/path/result.csv")
        
        # Try to retrieve - should be None because S3 object doesn't exist
        result = cache_manager.get_cached_execution(query_sql)
        
        assert result is None


def test_get_cached_execution_invalid_cache_file():
    """Test that invalid cache files are skipped gracefully."""
    with tempfile.TemporaryDirectory() as tmpdir:
        cache_config = CacheConfig(enabled=True, directory=tmpdir)
        s3_client = MagicMock()
        cache_manager = CacheManager(cache_config, s3_client)
        
        # Create an invalid cache file
        invalid_file = os.path.join(tmpdir, "invalid.json")
        with open(invalid_file, 'w') as f:
            f.write("not valid json{")
        
        # Should return None and not raise exception
        result = cache_manager.get_cached_execution("SELECT 1")
        
        assert result is None


def test_get_cached_execution_missing_required_fields():
    """Test that cache entries with missing required fields are skipped."""
    with tempfile.TemporaryDirectory() as tmpdir:
        cache_config = CacheConfig(enabled=True, directory=tmpdir)
        s3_client = MagicMock()
        cache_manager = CacheManager(cache_config, s3_client)
        
        # Create cache file with missing fields
        cache_file = os.path.join(tmpdir, "incomplete.json")
        with open(cache_file, 'w') as f:
            json.dump({"query_sql": "SELECT 1"}, f)  # Missing execution_id, timestamp, s3_location
        
        result = cache_manager.get_cached_execution("SELECT 1")
        
        assert result is None


def test_is_cache_fresh_within_ttl():
    """Test freshness check for cache within TTL."""
    cache_config = CacheConfig(enabled=True, ttl_seconds=3600)
    s3_client = MagicMock()
    cache_manager = CacheManager(cache_config, s3_client)
    
    # Timestamp from 30 minutes ago
    timestamp = time.time() - 1800
    
    assert cache_manager._is_cache_fresh(timestamp, 3600) is True


def test_is_cache_fresh_outside_ttl():
    """Test freshness check for cache outside TTL."""
    cache_config = CacheConfig(enabled=True, ttl_seconds=3600)
    s3_client = MagicMock()
    cache_manager = CacheManager(cache_config, s3_client)
    
    # Timestamp from 2 hours ago
    timestamp = time.time() - 7200
    
    assert cache_manager._is_cache_fresh(timestamp, 3600) is False


def test_is_cache_fresh_exactly_at_boundary():
    """Test freshness check at exact TTL boundary."""
    from unittest.mock import patch
    
    cache_config = CacheConfig(enabled=True, ttl_seconds=3600)
    s3_client = MagicMock()
    cache_manager = CacheManager(cache_config, s3_client)
    
    # Use a fixed current time to avoid timing issues
    fixed_now = 1000000.0
    timestamp = fixed_now - 3600  # Exactly at TTL boundary
    
    with patch('athena_query_tool.cache.time') as mock_time:
        mock_time.time.return_value = fixed_now
        # Should be fresh (age <= ttl)
        assert cache_manager._is_cache_fresh(timestamp, 3600) is True


def test_validate_s3_result_exists_valid():
    """Test S3 validation for existing object."""
    cache_config = CacheConfig(enabled=True)
    s3_client = MagicMock()
    
    # Mock successful head_object call
    s3_client.head_object.return_value = {}
    
    cache_manager = CacheManager(cache_config, s3_client)
    
    result = cache_manager._validate_s3_result_exists("s3://bucket/path/result.csv")
    
    assert result is True
    s3_client.head_object.assert_called_once_with(Bucket="bucket", Key="path/result.csv")


def test_validate_s3_result_exists_not_found():
    """Test S3 validation for non-existent object."""
    cache_config = CacheConfig(enabled=True)
    s3_client = MagicMock()
    
    # Mock 404 error
    s3_client.head_object.side_effect = ClientError(
        {'Error': {'Code': '404', 'Message': 'Not Found'}},
        'HeadObject'
    )
    
    cache_manager = CacheManager(cache_config, s3_client)
    
    result = cache_manager._validate_s3_result_exists("s3://bucket/path/result.csv")
    
    assert result is False


def test_validate_s3_result_exists_invalid_format():
    """Test S3 validation with invalid S3 URI format."""
    cache_config = CacheConfig(enabled=True)
    s3_client = MagicMock()
    cache_manager = CacheManager(cache_config, s3_client)
    
    # Invalid format (not s3://)
    result = cache_manager._validate_s3_result_exists("http://bucket/path/result.csv")
    assert result is False
    
    # Invalid format (no key)
    result = cache_manager._validate_s3_result_exists("s3://bucket")
    assert result is False


def test_validate_s3_result_exists_s3_error():
    """Test S3 validation handles S3 errors gracefully."""
    cache_config = CacheConfig(enabled=True)
    s3_client = MagicMock()
    
    # Mock S3 error
    s3_client.head_object.side_effect = ClientError(
        {'Error': {'Code': 'AccessDenied', 'Message': 'Access Denied'}},
        'HeadObject'
    )
    
    cache_manager = CacheManager(cache_config, s3_client)
    
    result = cache_manager._validate_s3_result_exists("s3://bucket/path/result.csv")
    
    assert result is False


def test_store_execution_handles_write_errors():
    """Test that store_execution handles file write errors gracefully."""
    cache_config = CacheConfig(enabled=True, directory="/invalid/path/that/cannot/be/created/")
    s3_client = MagicMock()
    
    # This should not raise an exception
    cache_manager = CacheManager(cache_config, s3_client)
    cache_manager.store_execution("SELECT 1", "exec-id", "s3://bucket/path")
    
    # No exception should be raised - errors are logged


def test_get_cached_execution_skips_non_json_files():
    """Test that non-JSON files are skipped during cache lookup."""
    with tempfile.TemporaryDirectory() as tmpdir:
        cache_config = CacheConfig(enabled=True, directory=tmpdir)
        s3_client = MagicMock()
        cache_manager = CacheManager(cache_config, s3_client)
        
        # Create a non-JSON file
        non_json_file = os.path.join(tmpdir, "not_json.txt")
        with open(non_json_file, 'w') as f:
            f.write("some text")
        
        result = cache_manager.get_cached_execution("SELECT 1")
        
        assert result is None

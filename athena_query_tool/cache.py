"""Cache management for Athena Query Tool."""

import json
import logging
import os
import time
from typing import Optional

from botocore.exceptions import ClientError

from .config import CacheConfig, CachedExecution


logger = logging.getLogger(__name__)


class CacheManager:
    """Manages local caching of query execution IDs and results."""
    
    def __init__(self, cache_config: CacheConfig, s3_client):
        """
        Initialize cache manager.
        
        Args:
            cache_config: Cache configuration (directory, TTL, enabled flag)
            s3_client: boto3 S3 client for validating cached results
        """
        self.cache_config = cache_config
        self.s3_client = s3_client
        self._ensure_cache_directory()
    
    def _ensure_cache_directory(self) -> None:
        """Create cache directory if it doesn't exist."""
        if not self.cache_config.enabled:
            return
        
        try:
            os.makedirs(self.cache_config.directory, exist_ok=True)
            logger.debug(f"Cache directory ensured: {self.cache_config.directory}")
        except OSError as e:
            logger.warning(f"Failed to create cache directory: {e}")
    
    def _get_cache_filename(self, execution_id: str) -> str:
        """
        Get cache filename from execution ID.
        
        Args:
            execution_id: Athena execution ID
            
        Returns:
            Full path to cache file
        """
        return os.path.join(self.cache_config.directory, f"{execution_id}.json")
    
    def store_execution(self, query_sql: str, execution_id: str, s3_location: str) -> None:
        """
        Store query execution in cache.
        
        Args:
            query_sql: SQL query string
            execution_id: Athena execution ID (used as cache key)
            s3_location: S3 location of query results
        """
        print("DEBUG")
        if not self.cache_config.enabled:
            return
        
        cache_entry = {
            "query_sql": query_sql,
            "execution_id": execution_id,
            "timestamp": time.time(),
            "s3_location": s3_location,
            "ttl_seconds": self.cache_config.ttl_seconds
        }
        
        cache_file = self._get_cache_filename(execution_id)
        
        try:
            with open(cache_file, 'w', encoding='utf-8') as f:
                json.dump(cache_entry, f, indent=2)
            logger.debug(f"Stored cache entry: {cache_file}")
        except (OSError, IOError) as e:
            logger.warning(f"Failed to write cache file {cache_file}: {e}")
        except Exception as e:
            logger.warning(f"Unexpected error writing cache file {cache_file}: {e}")
    
    def get_cached_execution(self, query_sql: str) -> Optional[CachedExecution]:
        """
        Get cached execution for a query if valid and fresh.
        
        Args:
            query_sql: SQL query string (used to look up cache)
            
        Returns:
            CachedExecution if valid cache exists, None otherwise
        """
        if not self.cache_config.enabled:
            return None
        
        # Look through all cache files to find matching query
        try:
            if not os.path.exists(self.cache_config.directory):
                return None
            
            for filename in os.listdir(self.cache_config.directory):
                if not filename.endswith('.json'):
                    continue
                
                cache_file = os.path.join(self.cache_config.directory, filename)
                
                try:
                    with open(cache_file, 'r', encoding='utf-8') as f:
                        cache_entry = json.load(f)
                    
                    # Check if query SQL matches
                    if cache_entry.get('query_sql') != query_sql:
                        continue
                    
                    # Extract fields
                    execution_id = cache_entry.get('execution_id')
                    timestamp = cache_entry.get('timestamp')
                    s3_location = cache_entry.get('s3_location')
                    ttl_seconds = cache_entry.get('ttl_seconds', self.cache_config.ttl_seconds)
                    
                    # Validate required fields
                    if not all([execution_id, timestamp, s3_location]):
                        logger.warning(f"Invalid cache entry in {cache_file}: missing required fields")
                        continue
                    
                    # Check if cache is fresh
                    if not self._is_cache_fresh(timestamp, ttl_seconds):
                        logger.debug(f"Cache entry in {cache_file} is stale")
                        continue
                    
                    # Validate S3 result exists
                    if not self._validate_s3_result_exists(s3_location):
                        logger.debug(f"S3 result for cache entry in {cache_file} no longer exists")
                        continue
                    
                    # Valid cache found
                    logger.info(f"Found valid cached execution: {execution_id}")
                    return CachedExecution(
                        query_sql=query_sql,
                        execution_id=execution_id,
                        timestamp=timestamp,
                        s3_location=s3_location,
                        ttl_seconds=ttl_seconds
                    )
                
                except (json.JSONDecodeError, OSError, IOError) as e:
                    logger.warning(f"Failed to read cache file {cache_file}: {e}")
                    continue
                except Exception as e:
                    logger.warning(f"Unexpected error reading cache file {cache_file}: {e}")
                    continue
            
            # No valid cache found
            return None
        
        except OSError as e:
            logger.warning(f"Failed to list cache directory: {e}")
            return None
        except Exception as e:
            logger.warning(f"Unexpected error during cache lookup: {e}")
            return None
    
    def _is_cache_fresh(self, timestamp: float, ttl_seconds: int) -> bool:
        """
        Check if cached entry is within TTL.
        
        Args:
            timestamp: Unix timestamp when cache entry was created
            ttl_seconds: Time-to-live in seconds
            
        Returns:
            True if cache is fresh, False otherwise
        """
        current_time = time.time()
        age_seconds = current_time - timestamp
        return age_seconds <= ttl_seconds
    
    def _validate_s3_result_exists(self, s3_location: str) -> bool:
        """
        Validate that S3 result file still exists.
        
        Args:
            s3_location: S3 URI (e.g., s3://bucket/path/to/file)
            
        Returns:
            True if S3 object exists, False otherwise
        """
        try:
            # Parse S3 location
            if not s3_location.startswith('s3://'):
                logger.warning(f"Invalid S3 location format: {s3_location}")
                return False
            
            s3_path = s3_location[5:]  # Remove 's3://' prefix
            parts = s3_path.split('/', 1)
            
            if len(parts) != 2:
                logger.warning(f"Invalid S3 location format: {s3_location}")
                return False
            
            bucket = parts[0]
            key = parts[1]
            
            # Check if object exists
            self.s3_client.head_object(Bucket=bucket, Key=key)
            return True
        
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            if error_code == '404':
                logger.debug(f"S3 object not found: {s3_location}")
            else:
                logger.warning(f"S3 error validating cache ({error_code}): {e}")
            return False
        except Exception as e:
            logger.warning(f"Unexpected error validating S3 result: {e}")
            return False



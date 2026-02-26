"""Query execution against AWS Athena."""

import logging
import time
from dataclasses import dataclass
from typing import Any, List, Optional

from .config import AthenaConfig, QueryPrefixConfig
from .exceptions import QueryExecutionError
from .retry import RetryHandler

logger = logging.getLogger(__name__)


@dataclass
class Column:
    """Represents a column in a query result."""
    name: str
    type: str


@dataclass
class QueryResult:
    """Represents the result of a query execution."""
    columns: List[Column]
    rows: List[List[Any]]
    row_count: int


class QueryExecutor:
    """Executes queries against AWS Athena and retrieves results."""
    
    def __init__(self, athena_client, config: AthenaConfig, retry_handler: RetryHandler = None,
                 s3_client=None, cache_manager=None,
                 query_prefix_config: QueryPrefixConfig = None):
        """
        Initialize QueryExecutor.
        
        Args:
            athena_client: Boto3 Athena client
            config: Athena configuration settings
            retry_handler: Optional RetryHandler instance (creates default if not provided)
            s3_client: Optional boto3 S3 client (needed for cache integration)
            cache_manager: Optional CacheManager instance for query result caching
            query_prefix_config: Optional QueryPrefixConfig for SQL comment prefix
        """
        self.athena_client = athena_client
        self.config = config
        self.retry_handler = retry_handler or RetryHandler()
        self.s3_client = s3_client
        self.cache_manager = cache_manager
        self.query_prefix_config = query_prefix_config or QueryPrefixConfig()
        self.poll_interval = 1.0  # Poll every 1 second
    
    def _build_prefix(self, query_name: Optional[str] = None) -> str:
        """
        Build the SQL comment prefix string.

        Args:
            query_name: Optional query name to include in the prefix

        Returns:
            SQL comment prefix string
        """
        if query_name:
            return f"-- [{self.query_prefix_config.tool_name}] query_name={query_name}\n"
        return f"-- [{self.query_prefix_config.tool_name}]\n"

    def execute_query(self, sql: str, query_name: Optional[str] = None) -> QueryResult:
        """
        Execute SQL query and return results, using cache when available.
        
        Args:
            sql: SQL query string
            query_name: Optional query name to include in the comment prefix
            
        Returns:
            QueryResult with columns and rows
            
        Raises:
            QueryExecutionError: If query fails
        """
        # Check cache first (using original SQL without prefix)
        if self.cache_manager:
            cached = self.cache_manager.get_cached_execution(sql)
            if cached:
                logger.info(f"Cache hit for query, reusing execution ID: {cached.execution_id}")
                return self._get_results(cached.execution_id)
        
        # Build prefixed SQL for submission
        prefixed_sql = self._build_prefix(query_name) + sql

        # Submit the query with prefix
        execution_id = self._submit_query(prefixed_sql)

        # Wait for completion
        final_status = self._wait_for_completion(execution_id)
        
        # Check if query succeeded
        if final_status != 'SUCCEEDED':
            # Get error information
            error_message = self._get_error_message(execution_id)
            raise QueryExecutionError(f"Query failed: {error_message}")
        
        # Store in cache after successful execution
        if self.cache_manager:
            s3_location = f"{self.config.output_location.rstrip('/')}/{execution_id}.csv"
            self.cache_manager.store_execution(sql, execution_id, s3_location)
            logger.info(f"Cached execution ID: {execution_id}")
        
        # Retrieve and return results
        return self._get_results(execution_id)
    
    def _submit_query(self, sql: str) -> str:
        """
        Submit query to Athena.
        
        Args:
            sql: SQL query string
            
        Returns:
            Query execution ID
            
        Raises:
            QueryExecutionError: If submission fails
        """
        try:
            response = self.retry_handler.execute_with_retry(
                self.athena_client.start_query_execution,
                QueryString=sql,
                QueryExecutionContext={
                    'Database': self.config.database
                },
                ResultConfiguration={
                    'OutputLocation': self.config.output_location
                },
                WorkGroup=self.config.workgroup
            )
            return response['QueryExecutionId']
        except Exception as e:
            raise QueryExecutionError(f"Failed to submit query: {str(e)}") from e
    
    def _wait_for_completion(self, execution_id: str) -> str:
        """
        Poll query status until it reaches a terminal state.
        
        Args:
            execution_id: Query execution ID
            
        Returns:
            Final query status (SUCCEEDED, FAILED, or CANCELLED)
            
        Raises:
            QueryExecutionError: If status check fails
        """
        terminal_states = {'SUCCEEDED', 'FAILED', 'CANCELLED'}
        
        while True:
            try:
                response = self.retry_handler.execute_with_retry(
                    self.athena_client.get_query_execution,
                    QueryExecutionId=execution_id
                )
                
                status = response['QueryExecution']['Status']['State']
                
                if status in terminal_states:
                    return status
                
                # Sleep before next poll
                time.sleep(self.poll_interval)
                
            except Exception as e:
                raise QueryExecutionError(f"Failed to check query status: {str(e)}") from e
    
    def _get_error_message(self, execution_id: str) -> str:
        """
        Get error message from failed query.
        
        Args:
            execution_id: Query execution ID
            
        Returns:
            Error message from Athena
        """
        try:
            response = self.retry_handler.execute_with_retry(
                self.athena_client.get_query_execution,
                QueryExecutionId=execution_id
            )
            
            status_info = response['QueryExecution']['Status']
            state_change_reason = status_info.get('StateChangeReason', 'Unknown error')
            
            return state_change_reason
            
        except Exception as e:
            return f"Failed to retrieve error message: {str(e)}"
    
    def _get_results(self, execution_id: str) -> QueryResult:
        """
        Retrieve query results.
        
        Args:
            execution_id: Query execution ID
            
        Returns:
            QueryResult with columns and rows
            
        Raises:
            QueryExecutionError: If result retrieval fails
        """
        try:
            # Get first page of results
            response = self.retry_handler.execute_with_retry(
                self.athena_client.get_query_results,
                QueryExecutionId=execution_id
            )
            
            # Extract column information from the first row (header row)
            result_set = response['ResultSet']
            column_info = result_set['ResultSetMetadata']['ColumnInfo']
            
            columns = [
                Column(name=col['Name'], type=col['Type'])
                for col in column_info
            ]
            
            # Extract data rows (skip the first row which is the header)
            rows = []
            result_rows = result_set.get('Rows', [])
            
            # Skip header row if present
            data_rows = result_rows[1:] if len(result_rows) > 0 else []
            
            for row in data_rows:
                row_data = []
                for field in row.get('Data', []):
                    # Handle NULL values - VarCharValue key is absent for NULL
                    value = field.get('VarCharValue')
                    row_data.append(value)
                rows.append(row_data)
            
            # Handle pagination if there are more results
            while 'NextToken' in response:
                response = self.retry_handler.execute_with_retry(
                    self.athena_client.get_query_results,
                    QueryExecutionId=execution_id,
                    NextToken=response['NextToken']
                )
                
                for row in response['ResultSet'].get('Rows', []):
                    row_data = []
                    for field in row.get('Data', []):
                        value = field.get('VarCharValue')
                        row_data.append(value)
                    rows.append(row_data)
            
            return QueryResult(
                columns=columns,
                rows=rows,
                row_count=len(rows)
            )
            
        except Exception as e:
            raise QueryExecutionError(f"Failed to retrieve query results: {str(e)}") from e

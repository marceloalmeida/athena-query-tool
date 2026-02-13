"""Unit tests for query execution."""

from unittest.mock import Mock, MagicMock
import pytest

from athena_query_tool.executor import Column, QueryResult, QueryExecutor
from athena_query_tool.config import AthenaConfig
from athena_query_tool.exceptions import QueryExecutionError
from athena_query_tool.retry import RetryHandler


def test_column_creation():
    """Test Column dataclass creation."""
    column = Column(name="id", type="integer")
    assert column.name == "id"
    assert column.type == "integer"


def test_query_result_creation():
    """Test QueryResult dataclass creation."""
    columns = [
        Column(name="id", type="integer"),
        Column(name="name", type="varchar")
    ]
    rows = [
        [1, "Alice"],
        [2, "Bob"]
    ]
    result = QueryResult(columns=columns, rows=rows, row_count=2)
    
    assert len(result.columns) == 2
    assert result.columns[0].name == "id"
    assert result.columns[1].name == "name"
    assert len(result.rows) == 2
    assert result.rows[0] == [1, "Alice"]
    assert result.row_count == 2


def test_query_result_empty():
    """Test QueryResult with zero rows."""
    columns = [Column(name="id", type="integer")]
    result = QueryResult(columns=columns, rows=[], row_count=0)
    
    assert len(result.columns) == 1
    assert len(result.rows) == 0
    assert result.row_count == 0


def test_query_result_with_null_values():
    """Test QueryResult with NULL values."""
    columns = [
        Column(name="id", type="integer"),
        Column(name="optional_field", type="varchar")
    ]
    rows = [
        [1, "value"],
        [2, None]
    ]
    result = QueryResult(columns=columns, rows=rows, row_count=2)
    
    assert result.rows[0][1] == "value"
    assert result.rows[1][1] is None


def test_query_executor_initialization():
    """Test QueryExecutor initialization."""
    mock_client = Mock()
    config = AthenaConfig(
        database="test_db",
        workgroup="primary",
        output_location="s3://bucket/path/"
    )
    
    executor = QueryExecutor(mock_client, config)
    
    assert executor.athena_client == mock_client
    assert executor.config == config
    assert isinstance(executor.retry_handler, RetryHandler)


def test_query_executor_initialization_with_retry_handler():
    """Test QueryExecutor initialization with custom retry handler."""
    mock_client = Mock()
    config = AthenaConfig(
        database="test_db",
        workgroup="primary",
        output_location="s3://bucket/path/"
    )
    custom_retry = RetryHandler(max_attempts=5, base_delay=2.0)
    
    executor = QueryExecutor(mock_client, config, custom_retry)
    
    assert executor.retry_handler == custom_retry


def test_successful_query_execution():
    """Test successful query execution."""
    mock_client = Mock()
    config = AthenaConfig(
        database="test_db",
        workgroup="primary",
        output_location="s3://bucket/path/"
    )
    
    # Mock start_query_execution
    mock_client.start_query_execution.return_value = {
        'QueryExecutionId': 'test-execution-id'
    }
    
    # Mock get_query_execution (status check)
    mock_client.get_query_execution.return_value = {
        'QueryExecution': {
            'Status': {
                'State': 'SUCCEEDED'
            }
        }
    }
    
    # Mock get_query_results
    mock_client.get_query_results.return_value = {
        'ResultSet': {
            'ResultSetMetadata': {
                'ColumnInfo': [
                    {'Name': 'id', 'Type': 'integer'},
                    {'Name': 'name', 'Type': 'varchar'}
                ]
            },
            'Rows': [
                # Header row
                {'Data': [{'VarCharValue': 'id'}, {'VarCharValue': 'name'}]},
                # Data rows
                {'Data': [{'VarCharValue': '1'}, {'VarCharValue': 'Alice'}]},
                {'Data': [{'VarCharValue': '2'}, {'VarCharValue': 'Bob'}]}
            ]
        }
    }
    
    executor = QueryExecutor(mock_client, config)
    result = executor.execute_query("SELECT * FROM test_table")
    
    assert len(result.columns) == 2
    assert result.columns[0].name == "id"
    assert result.columns[1].name == "name"
    assert result.row_count == 2
    assert result.rows[0] == ['1', 'Alice']
    assert result.rows[1] == ['2', 'Bob']


def test_query_execution_with_zero_rows():
    """Test query execution that returns zero rows."""
    mock_client = Mock()
    config = AthenaConfig(
        database="test_db",
        workgroup="primary",
        output_location="s3://bucket/path/"
    )
    
    mock_client.start_query_execution.return_value = {
        'QueryExecutionId': 'test-execution-id'
    }
    
    mock_client.get_query_execution.return_value = {
        'QueryExecution': {
            'Status': {
                'State': 'SUCCEEDED'
            }
        }
    }
    
    # Only header row, no data rows
    mock_client.get_query_results.return_value = {
        'ResultSet': {
            'ResultSetMetadata': {
                'ColumnInfo': [
                    {'Name': 'id', 'Type': 'integer'}
                ]
            },
            'Rows': [
                {'Data': [{'VarCharValue': 'id'}]}
            ]
        }
    }
    
    executor = QueryExecutor(mock_client, config)
    result = executor.execute_query("SELECT * FROM empty_table")
    
    assert len(result.columns) == 1
    assert result.row_count == 0
    assert len(result.rows) == 0


def test_query_execution_with_null_values():
    """Test query execution with NULL values in results."""
    mock_client = Mock()
    config = AthenaConfig(
        database="test_db",
        workgroup="primary",
        output_location="s3://bucket/path/"
    )
    
    mock_client.start_query_execution.return_value = {
        'QueryExecutionId': 'test-execution-id'
    }
    
    mock_client.get_query_execution.return_value = {
        'QueryExecution': {
            'Status': {
                'State': 'SUCCEEDED'
            }
        }
    }
    
    # Row with NULL value (VarCharValue key is absent)
    mock_client.get_query_results.return_value = {
        'ResultSet': {
            'ResultSetMetadata': {
                'ColumnInfo': [
                    {'Name': 'id', 'Type': 'integer'},
                    {'Name': 'optional', 'Type': 'varchar'}
                ]
            },
            'Rows': [
                {'Data': [{'VarCharValue': 'id'}, {'VarCharValue': 'optional'}]},
                {'Data': [{'VarCharValue': '1'}, {'VarCharValue': 'value'}]},
                {'Data': [{'VarCharValue': '2'}, {}]}  # NULL value
            ]
        }
    }
    
    executor = QueryExecutor(mock_client, config)
    result = executor.execute_query("SELECT * FROM test_table")
    
    assert result.rows[0] == ['1', 'value']
    assert result.rows[1] == ['2', None]


def test_query_execution_failure():
    """Test query execution that fails."""
    mock_client = Mock()
    config = AthenaConfig(
        database="test_db",
        workgroup="primary",
        output_location="s3://bucket/path/"
    )
    
    mock_client.start_query_execution.return_value = {
        'QueryExecutionId': 'test-execution-id'
    }
    
    # Query failed
    mock_client.get_query_execution.return_value = {
        'QueryExecution': {
            'Status': {
                'State': 'FAILED',
                'StateChangeReason': 'SYNTAX_ERROR: line 1:8: Table does not exist'
            }
        }
    }
    
    executor = QueryExecutor(mock_client, config)
    
    with pytest.raises(QueryExecutionError) as exc_info:
        executor.execute_query("SELECT * FROM nonexistent_table")
    
    assert "Query failed" in str(exc_info.value)
    assert "SYNTAX_ERROR" in str(exc_info.value)


def test_query_submission_uses_config():
    """Test that query submission uses configuration values."""
    mock_client = Mock()
    config = AthenaConfig(
        database="my_database",
        workgroup="my_workgroup",
        output_location="s3://my-bucket/results/"
    )
    
    mock_client.start_query_execution.return_value = {
        'QueryExecutionId': 'test-execution-id'
    }
    
    mock_client.get_query_execution.return_value = {
        'QueryExecution': {
            'Status': {
                'State': 'SUCCEEDED'
            }
        }
    }
    
    mock_client.get_query_results.return_value = {
        'ResultSet': {
            'ResultSetMetadata': {
                'ColumnInfo': [{'Name': 'col', 'Type': 'varchar'}]
            },
            'Rows': [
                {'Data': [{'VarCharValue': 'col'}]}
            ]
        }
    }
    
    executor = QueryExecutor(mock_client, config)
    executor.execute_query("SELECT 1")
    
    # Verify start_query_execution was called with correct parameters
    mock_client.start_query_execution.assert_called_once()
    call_kwargs = mock_client.start_query_execution.call_args[1]
    
    assert call_kwargs['QueryExecutionContext']['Database'] == 'my_database'
    assert call_kwargs['WorkGroup'] == 'my_workgroup'
    assert call_kwargs['ResultConfiguration']['OutputLocation'] == 's3://my-bucket/results/'


def test_query_execution_with_pagination():
    """Test query execution with paginated results."""
    mock_client = Mock()
    config = AthenaConfig(
        database="test_db",
        workgroup="primary",
        output_location="s3://bucket/path/"
    )
    
    mock_client.start_query_execution.return_value = {
        'QueryExecutionId': 'test-execution-id'
    }
    
    mock_client.get_query_execution.return_value = {
        'QueryExecution': {
            'Status': {
                'State': 'SUCCEEDED'
            }
        }
    }
    
    # First page with NextToken
    first_page = {
        'ResultSet': {
            'ResultSetMetadata': {
                'ColumnInfo': [{'Name': 'id', 'Type': 'integer'}]
            },
            'Rows': [
                {'Data': [{'VarCharValue': 'id'}]},
                {'Data': [{'VarCharValue': '1'}]}
            ]
        },
        'NextToken': 'token123'
    }
    
    # Second page without NextToken
    second_page = {
        'ResultSet': {
            'Rows': [
                {'Data': [{'VarCharValue': '2'}]},
                {'Data': [{'VarCharValue': '3'}]}
            ]
        }
    }
    
    mock_client.get_query_results.side_effect = [first_page, second_page]
    
    executor = QueryExecutor(mock_client, config)
    result = executor.execute_query("SELECT * FROM large_table")
    
    assert result.row_count == 3
    assert result.rows == [['1'], ['2'], ['3']]
    assert mock_client.get_query_results.call_count == 2


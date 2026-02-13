"""Unit tests for CLI module."""

import sys
from io import StringIO
from unittest.mock import Mock, patch, MagicMock
import pytest

from athena_query_tool.cli import main
from athena_query_tool.config import Config, AWSConfig, AthenaConfig, CacheConfig, OutputConfig, QueryConfig
from athena_query_tool.executor import QueryResult, Column
from athena_query_tool.exceptions import (
    ConfigurationError,
    AuthenticationError,
    QueryExecutionError,
    FileOutputError
)


@pytest.fixture
def sample_config():
    """Create a sample configuration for testing."""
    return Config(
        aws=AWSConfig(profile=None, region='us-east-1'),
        athena=AthenaConfig(
            database='test_db',
            workgroup='primary',
            output_location='s3://test-bucket/results/'
        ),
        cache=CacheConfig(),
        output=OutputConfig(format='table', file=None),
        queries=[
            QueryConfig(name='test_query', sql='SELECT * FROM test_table')
        ]
    )


@pytest.fixture
def sample_result():
    """Create a sample query result for testing."""
    return QueryResult(
        columns=[
            Column(name='id', type='integer'),
            Column(name='name', type='varchar')
        ],
        rows=[
            [1, 'Alice'],
            [2, 'Bob']
        ],
        row_count=2
    )


def test_main_success_table_output(sample_config, sample_result):
    """Test successful execution with table output."""
    with patch('sys.argv', ['athena-query', 'config.yaml']):
        with patch('athena_query_tool.config.ConfigurationManager') as mock_config_mgr:
            with patch('athena_query_tool.auth.AuthenticationManager') as mock_auth_mgr:
                with patch('athena_query_tool.executor.QueryExecutor') as mock_executor_cls:
                    with patch('athena_query_tool.formatter.ResultFormatter') as mock_formatter_cls:
                        # Setup mocks
                        mock_config_mgr.load_config.return_value = sample_config
                        
                        mock_session = Mock()
                        mock_auth_mgr.return_value.get_session.return_value = mock_session
                        mock_session.client.return_value = Mock()
                        
                        mock_executor = Mock()
                        mock_executor.execute_query.return_value = sample_result
                        mock_executor_cls.return_value = mock_executor
                        
                        mock_formatter = Mock()
                        mock_formatter.format_as_table.return_value = "| id | name |\n|  1 | Alice |\n|  2 | Bob |"
                        mock_formatter_cls.return_value = mock_formatter
                        
                        # Capture stdout
                        captured_output = StringIO()
                        with patch('sys.stdout', captured_output):
                            exit_code = main()
                        
                        # Verify
                        assert exit_code == 0
                        assert 'test_query' in captured_output.getvalue()
                        mock_executor.execute_query.assert_called_once_with('SELECT * FROM test_table')


def test_main_success_csv_output(sample_config, sample_result, tmp_path):
    """Test successful execution with CSV output."""
    # Modify config for CSV output
    csv_file = tmp_path / "output.csv"
    sample_config.output.format = 'csv'
    sample_config.output.file = str(csv_file)
    
    with patch('sys.argv', ['athena-query', 'config.yaml']):
        with patch('athena_query_tool.config.ConfigurationManager') as mock_config_mgr:
            with patch('athena_query_tool.auth.AuthenticationManager') as mock_auth_mgr:
                with patch('athena_query_tool.executor.QueryExecutor') as mock_executor_cls:
                    with patch('athena_query_tool.formatter.ResultFormatter') as mock_formatter_cls:
                        # Setup mocks
                        mock_config_mgr.load_config.return_value = sample_config
                        
                        mock_session = Mock()
                        mock_auth_mgr.return_value.get_session.return_value = mock_session
                        mock_session.client.return_value = Mock()
                        
                        mock_executor = Mock()
                        mock_executor.execute_query.return_value = sample_result
                        mock_executor_cls.return_value = mock_executor
                        
                        mock_formatter = Mock()
                        mock_formatter_cls.return_value = mock_formatter
                        
                        # Execute
                        exit_code = main()
                        
                        # Verify
                        assert exit_code == 0
                        mock_formatter.write_to_csv.assert_called_once()


def test_main_configuration_error():
    """Test handling of configuration errors."""
    with patch('sys.argv', ['athena-query', 'config.yaml']):
        with patch('athena_query_tool.config.ConfigurationManager') as mock_config_mgr:
            # Setup mock to raise ConfigurationError
            mock_config_mgr.load_config.side_effect = ConfigurationError("Missing required field: database")
            
            # Capture stderr
            captured_error = StringIO()
            with patch('sys.stderr', captured_error):
                exit_code = main()
            
            # Verify
            assert exit_code == 1
            assert 'Configuration Error' in captured_error.getvalue()


def test_main_authentication_error(sample_config):
    """Test handling of authentication errors."""
    with patch('sys.argv', ['athena-query', 'config.yaml']):
        with patch('athena_query_tool.config.ConfigurationManager') as mock_config_mgr:
            with patch('athena_query_tool.auth.AuthenticationManager') as mock_auth_mgr:
                # Setup mocks
                mock_config_mgr.load_config.return_value = sample_config
                mock_auth_mgr.return_value.get_session.side_effect = AuthenticationError("No credentials found")
                
                # Capture stderr
                captured_error = StringIO()
                with patch('sys.stderr', captured_error):
                    exit_code = main()
                
                # Verify
                assert exit_code == 2
                assert 'Authentication Error' in captured_error.getvalue()


def test_main_query_execution_error(sample_config):
    """Test handling of query execution errors."""
    with patch('sys.argv', ['athena-query', 'config.yaml']):
        with patch('athena_query_tool.config.ConfigurationManager') as mock_config_mgr:
            with patch('athena_query_tool.auth.AuthenticationManager') as mock_auth_mgr:
                with patch('athena_query_tool.executor.QueryExecutor') as mock_executor_cls:
                    # Setup mocks
                    mock_config_mgr.load_config.return_value = sample_config
                    
                    mock_session = Mock()
                    mock_auth_mgr.return_value.get_session.return_value = mock_session
                    mock_session.client.return_value = Mock()
                    
                    mock_executor = Mock()
                    mock_executor.execute_query.side_effect = QueryExecutionError("Query failed: Syntax error")
                    mock_executor_cls.return_value = mock_executor
                    
                    # Capture stderr
                    captured_error = StringIO()
                    with patch('sys.stderr', captured_error):
                        exit_code = main()
                    
                    # Verify
                    assert exit_code == 3
                    assert 'Query failed' in captured_error.getvalue()


def test_main_file_output_error(sample_config, sample_result):
    """Test handling of file output errors."""
    # Modify config for CSV output
    sample_config.output.format = 'csv'
    sample_config.output.file = '/invalid/path/output.csv'
    
    with patch('sys.argv', ['athena-query', 'config.yaml']):
        with patch('athena_query_tool.config.ConfigurationManager') as mock_config_mgr:
            with patch('athena_query_tool.auth.AuthenticationManager') as mock_auth_mgr:
                with patch('athena_query_tool.executor.QueryExecutor') as mock_executor_cls:
                    with patch('athena_query_tool.formatter.ResultFormatter') as mock_formatter_cls:
                        # Setup mocks
                        mock_config_mgr.load_config.return_value = sample_config
                        
                        mock_session = Mock()
                        mock_auth_mgr.return_value.get_session.return_value = mock_session
                        mock_session.client.return_value = Mock()
                        
                        mock_executor = Mock()
                        mock_executor.execute_query.return_value = sample_result
                        mock_executor_cls.return_value = mock_executor
                        
                        mock_formatter = Mock()
                        mock_formatter.write_to_csv.side_effect = FileOutputError("Permission denied")
                        mock_formatter_cls.return_value = mock_formatter
                        
                        # Capture stderr
                        captured_error = StringIO()
                        with patch('sys.stderr', captured_error):
                            exit_code = main()
                        
                        # Verify
                        assert exit_code == 5
                        assert 'Error writing output file' in captured_error.getvalue()


def test_main_csv_without_file_path(sample_config):
    """Test CSV output format without file path specified."""
    # Modify config for CSV output without file path
    sample_config.output.format = 'csv'
    sample_config.output.file = None
    
    with patch('sys.argv', ['athena-query', 'config.yaml']):
        with patch('athena_query_tool.config.ConfigurationManager') as mock_config_mgr:
            with patch('athena_query_tool.auth.AuthenticationManager') as mock_auth_mgr:
                with patch('athena_query_tool.executor.QueryExecutor') as mock_executor_cls:
                    # Setup mocks
                    mock_config_mgr.load_config.return_value = sample_config
                    
                    mock_session = Mock()
                    mock_auth_mgr.return_value.get_session.return_value = mock_session
                    mock_session.client.return_value = Mock()
                    
                    mock_executor = Mock()
                    mock_executor.execute_query.return_value = QueryResult(
                        columns=[Column(name='id', type='integer')],
                        rows=[[1]],
                        row_count=1
                    )
                    mock_executor_cls.return_value = mock_executor
                    
                    # Execute
                    exit_code = main()
                    
                    # Verify
                    assert exit_code == 1


def test_main_multiple_queries(sample_config, sample_result, tmp_path):
    """Test execution of multiple queries with CSV output."""
    # Add multiple queries
    sample_config.queries = [
        QueryConfig(name='query1', sql='SELECT * FROM table1'),
        QueryConfig(name='query2', sql='SELECT * FROM table2')
    ]
    
    csv_file = tmp_path / "output.csv"
    sample_config.output.format = 'csv'
    sample_config.output.file = str(csv_file)
    
    with patch('sys.argv', ['athena-query', 'config.yaml']):
        with patch('athena_query_tool.config.ConfigurationManager') as mock_config_mgr:
            with patch('athena_query_tool.auth.AuthenticationManager') as mock_auth_mgr:
                with patch('athena_query_tool.executor.QueryExecutor') as mock_executor_cls:
                    with patch('athena_query_tool.formatter.ResultFormatter') as mock_formatter_cls:
                        # Setup mocks
                        mock_config_mgr.load_config.return_value = sample_config
                        
                        mock_session = Mock()
                        mock_auth_mgr.return_value.get_session.return_value = mock_session
                        mock_session.client.return_value = Mock()
                        
                        mock_executor = Mock()
                        mock_executor.execute_query.return_value = sample_result
                        mock_executor_cls.return_value = mock_executor
                        
                        mock_formatter = Mock()
                        mock_formatter_cls.return_value = mock_formatter
                        
                        # Execute
                        exit_code = main()
                        
                        # Verify
                        assert exit_code == 0
                        assert mock_executor.execute_query.call_count == 2
                        assert mock_formatter.write_to_csv.call_count == 2
                        
                        # Verify filenames include query names
                        calls = mock_formatter.write_to_csv.call_args_list
                        assert 'query1' in calls[0][0][1]
                        assert 'query2' in calls[1][0][1]

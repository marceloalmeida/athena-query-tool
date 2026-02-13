"""Unit tests for retry logic."""

import time
import pytest
from unittest.mock import Mock
from botocore.exceptions import ClientError, ConnectTimeoutError, ReadTimeoutError

from athena_query_tool.retry import RetryHandler


def test_retry_handler_initialization():
    """Test RetryHandler initialization with default values."""
    handler = RetryHandler()
    assert handler.max_attempts == 3
    assert handler.base_delay == 1.0


def test_retry_handler_custom_initialization():
    """Test RetryHandler initialization with custom values."""
    handler = RetryHandler(max_attempts=5, base_delay=2.0)
    assert handler.max_attempts == 5
    assert handler.base_delay == 2.0


def test_execute_with_retry_success_first_attempt():
    """Test successful execution on first attempt."""
    handler = RetryHandler()
    mock_func = Mock(return_value="success")
    
    result = handler.execute_with_retry(mock_func, "arg1", kwarg1="value1")
    
    assert result == "success"
    assert mock_func.call_count == 1
    mock_func.assert_called_once_with("arg1", kwarg1="value1")


def test_execute_with_retry_success_after_transient_error():
    """Test successful execution after transient error."""
    handler = RetryHandler(base_delay=0.01)  # Small delay for faster tests
    
    # Mock function that fails once with throttling, then succeeds
    mock_func = Mock(side_effect=[
        ClientError(
            {'Error': {'Code': 'ThrottlingException', 'Message': 'Rate exceeded'}},
            'StartQueryExecution'
        ),
        "success"
    ])
    
    result = handler.execute_with_retry(mock_func)
    
    assert result == "success"
    assert mock_func.call_count == 2


def test_execute_with_retry_non_transient_error_immediate_failure():
    """Test immediate failure on non-transient error."""
    handler = RetryHandler()
    
    # Mock function that raises a non-transient error
    error = ClientError(
        {'Error': {'Code': 'InvalidRequestException', 'Message': 'Invalid request'}},
        'StartQueryExecution'
    )
    mock_func = Mock(side_effect=error)
    
    with pytest.raises(ClientError) as exc_info:
        handler.execute_with_retry(mock_func)
    
    assert exc_info.value == error
    assert mock_func.call_count == 1  # Should not retry


def test_execute_with_retry_exhausts_max_attempts():
    """Test that retry exhausts max attempts and raises last exception."""
    handler = RetryHandler(max_attempts=3, base_delay=0.01)
    
    # Mock function that always fails with transient error
    error = ClientError(
        {'Error': {'Code': 'ThrottlingException', 'Message': 'Rate exceeded'}},
        'StartQueryExecution'
    )
    mock_func = Mock(side_effect=error)
    
    with pytest.raises(ClientError) as exc_info:
        handler.execute_with_retry(mock_func)
    
    assert exc_info.value == error
    assert mock_func.call_count == 3  # Should try max_attempts times


def test_execute_with_retry_exponential_backoff():
    """Test that exponential backoff is applied correctly."""
    handler = RetryHandler(max_attempts=3, base_delay=0.1)
    
    # Mock function that always fails with transient error
    error = ClientError(
        {'Error': {'Code': 'ThrottlingException', 'Message': 'Rate exceeded'}},
        'StartQueryExecution'
    )
    mock_func = Mock(side_effect=error)
    
    start_time = time.time()
    
    with pytest.raises(ClientError):
        handler.execute_with_retry(mock_func)
    
    elapsed_time = time.time() - start_time
    
    # Expected delays: 0.1 * (2^0) + 0.1 * (2^1) = 0.1 + 0.2 = 0.3 seconds
    # Allow some tolerance for execution time
    assert elapsed_time >= 0.25  # At least 0.3 - tolerance
    assert elapsed_time < 0.5   # Not too much more


def test_is_transient_error_throttling_exception():
    """Test that ThrottlingException is classified as transient."""
    handler = RetryHandler()
    
    error = ClientError(
        {'Error': {'Code': 'ThrottlingException', 'Message': 'Rate exceeded'}},
        'StartQueryExecution'
    )
    
    assert handler._is_transient_error(error) is True


def test_is_transient_error_too_many_requests():
    """Test that TooManyRequestsException is classified as transient."""
    handler = RetryHandler()
    
    error = ClientError(
        {'Error': {'Code': 'TooManyRequestsException', 'Message': 'Too many requests'}},
        'StartQueryExecution'
    )
    
    assert handler._is_transient_error(error) is True


def test_is_transient_error_provisioned_throughput_exceeded():
    """Test that ProvisionedThroughputExceededException is classified as transient."""
    handler = RetryHandler()
    
    error = ClientError(
        {'Error': {'Code': 'ProvisionedThroughputExceededException', 'Message': 'Throughput exceeded'}},
        'StartQueryExecution'
    )
    
    assert handler._is_transient_error(error) is True


def test_is_transient_error_request_limit_exceeded():
    """Test that RequestLimitExceeded is classified as transient."""
    handler = RetryHandler()
    
    error = ClientError(
        {'Error': {'Code': 'RequestLimitExceeded', 'Message': 'Request limit exceeded'}},
        'StartQueryExecution'
    )
    
    assert handler._is_transient_error(error) is True


def test_is_transient_error_5xx_http_status():
    """Test that 5xx HTTP status codes are classified as transient."""
    handler = RetryHandler()
    
    # Test 500 Internal Server Error
    error_500 = ClientError(
        {
            'Error': {'Code': 'InternalError', 'Message': 'Internal error'},
            'ResponseMetadata': {'HTTPStatusCode': 500}
        },
        'StartQueryExecution'
    )
    assert handler._is_transient_error(error_500) is True
    
    # Test 503 Service Unavailable
    error_503 = ClientError(
        {
            'Error': {'Code': 'ServiceUnavailable', 'Message': 'Service unavailable'},
            'ResponseMetadata': {'HTTPStatusCode': 503}
        },
        'StartQueryExecution'
    )
    assert handler._is_transient_error(error_503) is True
    
    # Test 599 (edge of 5xx range)
    error_599 = ClientError(
        {
            'Error': {'Code': 'UnknownError', 'Message': 'Unknown error'},
            'ResponseMetadata': {'HTTPStatusCode': 599}
        },
        'StartQueryExecution'
    )
    assert handler._is_transient_error(error_599) is True


def test_is_transient_error_service_unavailable():
    """Test that ServiceUnavailable error code is classified as transient."""
    handler = RetryHandler()
    
    error = ClientError(
        {'Error': {'Code': 'ServiceUnavailable', 'Message': 'Service unavailable'}},
        'StartQueryExecution'
    )
    
    assert handler._is_transient_error(error) is True


def test_is_transient_error_internal_server_error():
    """Test that InternalServerError error code is classified as transient."""
    handler = RetryHandler()
    
    error = ClientError(
        {'Error': {'Code': 'InternalServerError', 'Message': 'Internal server error'}},
        'StartQueryExecution'
    )
    
    assert handler._is_transient_error(error) is True


def test_is_transient_error_connect_timeout():
    """Test that ConnectTimeoutError is classified as transient."""
    handler = RetryHandler()
    
    error = ConnectTimeoutError(endpoint_url="https://athena.us-east-1.amazonaws.com")
    
    assert handler._is_transient_error(error) is True


def test_is_transient_error_read_timeout():
    """Test that ReadTimeoutError is classified as transient."""
    handler = RetryHandler()
    
    error = ReadTimeoutError(endpoint_url="https://athena.us-east-1.amazonaws.com")
    
    assert handler._is_transient_error(error) is True


def test_is_transient_error_non_transient_client_error():
    """Test that non-transient ClientErrors are classified correctly."""
    handler = RetryHandler()
    
    # Test 4xx errors (client errors)
    error_400 = ClientError(
        {
            'Error': {'Code': 'InvalidRequestException', 'Message': 'Invalid request'},
            'ResponseMetadata': {'HTTPStatusCode': 400}
        },
        'StartQueryExecution'
    )
    assert handler._is_transient_error(error_400) is False
    
    error_404 = ClientError(
        {
            'Error': {'Code': 'ResourceNotFoundException', 'Message': 'Resource not found'},
            'ResponseMetadata': {'HTTPStatusCode': 404}
        },
        'StartQueryExecution'
    )
    assert handler._is_transient_error(error_404) is False


def test_is_transient_error_generic_exception():
    """Test that generic exceptions are not classified as transient."""
    handler = RetryHandler()
    
    error = ValueError("Some value error")
    
    assert handler._is_transient_error(error) is False


def test_is_transient_error_authentication_error():
    """Test that authentication errors are not classified as transient."""
    handler = RetryHandler()
    
    error = ClientError(
        {'Error': {'Code': 'UnrecognizedClientException', 'Message': 'Invalid credentials'}},
        'StartQueryExecution'
    )
    
    assert handler._is_transient_error(error) is False


def test_execute_with_retry_with_args_and_kwargs():
    """Test that args and kwargs are properly passed to the function."""
    handler = RetryHandler()
    
    mock_func = Mock(return_value="result")
    
    result = handler.execute_with_retry(
        mock_func,
        "arg1",
        "arg2",
        kwarg1="value1",
        kwarg2="value2"
    )
    
    assert result == "result"
    mock_func.assert_called_once_with("arg1", "arg2", kwarg1="value1", kwarg2="value2")


def test_execute_with_retry_multiple_transient_errors():
    """Test retry with multiple different transient errors."""
    handler = RetryHandler(base_delay=0.01)
    
    # Mock function that fails with different transient errors, then succeeds
    mock_func = Mock(side_effect=[
        ClientError(
            {'Error': {'Code': 'ThrottlingException', 'Message': 'Rate exceeded'}},
            'StartQueryExecution'
        ),
        ConnectTimeoutError(endpoint_url="https://athena.us-east-1.amazonaws.com"),
        "success"
    ])
    
    result = handler.execute_with_retry(mock_func)
    
    assert result == "success"
    assert mock_func.call_count == 3

"""Retry logic with exponential backoff."""

import time
from typing import Any, Callable
from botocore.exceptions import ClientError, ConnectTimeoutError, ReadTimeoutError


class RetryHandler:
    """Handles retry logic with exponential backoff for transient failures."""
    
    def __init__(self, max_attempts: int = 3, base_delay: float = 1.0):
        """
        Initialize retry handler.
        
        Args:
            max_attempts: Maximum number of retry attempts
            base_delay: Base delay in seconds for exponential backoff
        """
        self.max_attempts = max_attempts
        self.base_delay = base_delay
    
    def execute_with_retry(self, func: Callable, *args, **kwargs) -> Any:
        """
        Execute function with retry logic.
        
        Args:
            func: Function to execute
            *args, **kwargs: Arguments to pass to function
            
        Returns:
            Result from successful function execution
            
        Raises:
            Last exception if all retries exhausted
        """
        last_exception = None
        
        for attempt in range(self.max_attempts):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                last_exception = e
                
                # If this is not a transient error, fail immediately
                if not self._is_transient_error(e):
                    raise
                
                # If we've exhausted all attempts, raise the last exception
                if attempt == self.max_attempts - 1:
                    raise
                
                # Calculate exponential backoff delay
                delay = self.base_delay * (2 ** attempt)
                time.sleep(delay)
        
        # This should never be reached, but just in case
        if last_exception:
            raise last_exception
    
    def _is_transient_error(self, error: Exception) -> bool:
        """
        Determine if error is transient and should be retried.
        
        Args:
            error: Exception to classify
            
        Returns:
            True if error is transient, False otherwise
        """
        # Handle botocore timeout errors
        if isinstance(error, (ConnectTimeoutError, ReadTimeoutError)):
            return True
        
        # Handle boto3 ClientError
        if isinstance(error, ClientError):
            error_code = error.response.get('Error', {}).get('Code', '')
            http_status = error.response.get('ResponseMetadata', {}).get('HTTPStatusCode', 0)
            
            # Throttling errors
            if error_code in ['ThrottlingException', 'TooManyRequestsException', 
                             'ProvisionedThroughputExceededException', 'RequestLimitExceeded']:
                return True
            
            # 5xx HTTP errors (server errors)
            if 500 <= http_status < 600:
                return True
            
            # Service unavailable
            if error_code in ['ServiceUnavailable', 'InternalServerError']:
                return True
        
        return False

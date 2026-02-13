"""Custom exceptions for Athena Query Tool."""


class AthenaQueryToolError(Exception):
    """Base exception for Athena Query Tool."""
    pass


class ConfigurationError(AthenaQueryToolError):
    """Configuration-related errors."""
    pass


class AuthenticationError(AthenaQueryToolError):
    """Authentication-related errors."""
    pass


class QueryExecutionError(AthenaQueryToolError):
    """Query execution errors."""
    pass


class FileOutputError(AthenaQueryToolError):
    """File output errors."""
    pass

# Design Document: AWS Athena Query Tool

## Overview

The AWS Athena Query Tool is a Python command-line application that executes SQL queries against AWS Athena and displays results in an ASCII table format or writes them to CSV/JSON files. The tool follows a modular architecture with clear separation of concerns between configuration management, AWS authentication, query execution, retry logic, and result formatting.

The application will use the boto3 library for AWS SDK interactions, a table formatting library (such as tabulate or prettytable) for ASCII table generation, and Python's built-in csv and json modules for file output.

## Architecture

The system follows a layered architecture:

```
┌─────────────────────────────────────┐
│         CLI Entry Point             │
└──────────────┬──────────────────────┘
               │
┌──────────────▼──────────────────────┐
│     Configuration Manager           │
│  - Parse config file (YAML)         │
│  - Validate required fields         │
└──────────────┬──────────────────────┘
               │
┌──────────────▼──────────────────────┐
│    Authentication Manager           │
│  - Resolve AWS credentials          │
│  - Follow credential chain          │
└──────────────┬──────────────────────┘
               │
┌──────────────▼──────────────────────┐
│       Query Executor                │
│  - Submit queries to Athena         │
│  - Poll for completion              │
│  - Retrieve results                 │
└──────────────┬──────────────────────┘
               │
┌──────────────▼──────────────────────┐
│      Retry Handler                  │
│  - Wrap API calls                   │
│  - Exponential backoff              │
│  - Transient error detection        │
└──────────────┬──────────────────────┘
               │
┌──────────────▼──────────────────────┐
│      Result Formatter               │
│  - Format as ASCII table            │
│  - Handle NULL values               │
│  - Truncate long values             │
│  - Write to CSV/JSON files          │
└──────────────┬──────────────────────┘
               │
               ▼
     [Output to stdout or file]
```

## Components and Interfaces

### 1. Configuration Manager

**Responsibility:** Parse and validate the configuration file.

**Interface:**
```python
class ConfigurationManager:
    def load_config(file_path: str) -> Config:
        """
        Load and parse configuration file.
        
        Args:
            file_path: Path to YAML configuration file
            
        Returns:
            Config object with validated settings
            
        Raises:
            ConfigurationError: If file is missing, invalid, or incomplete
        """
        pass
```

**Configuration Schema:**
```yaml
aws:
  profile: optional_profile_name
  region: us-east-1

athena:
  database: my_database
  workgroup: primary
  output_location: s3://my-bucket/athena-results/

output:
  format: table  # Options: table, csv, json
  file: optional_output_file_path  # Only used when format is csv or json

queries:
  - name: sample_query
    sql: SELECT * FROM my_table LIMIT 10
```

### 2. Authentication Manager

**Responsibility:** Resolve AWS credentials using the standard credential provider chain.

**Interface:**
```python
class AuthenticationManager:
    def get_session(profile: Optional[str] = None, region: Optional[str] = None) -> boto3.Session:
        """
        Create boto3 session with resolved credentials.
        
        Args:
            profile: Optional AWS profile name
            region: Optional AWS region
            
        Returns:
            Configured boto3 Session
            
        Raises:
            AuthenticationError: If no valid credentials found
        """
        pass
```

**Credential Resolution Order:**
1. Environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
2. Profile specified in configuration
3. AWS credentials file (~/.aws/credentials)
4. IAM role (when running on AWS infrastructure)

### 3. Query Executor

**Responsibility:** Execute queries against Athena and retrieve results.

**Interface:**
```python
class QueryExecutor:
    def __init__(self, athena_client, config: AthenaConfig):
        """Initialize with boto3 Athena client and configuration."""
        pass
    
    def execute_query(self, sql: str) -> QueryResult:
        """
        Execute SQL query and return results.
        
        Args:
            sql: SQL query string
            
        Returns:
            QueryResult with columns and rows
            
        Raises:
            QueryExecutionError: If query fails
        """
        pass
    
    def _submit_query(self, sql: str) -> str:
        """Submit query and return execution ID."""
        pass
    
    def _wait_for_completion(self, execution_id: str) -> str:
        """Poll until query completes, return final status."""
        pass
    
    def _get_results(self, execution_id: str) -> QueryResult:
        """Retrieve query results."""
        pass
```

### 4. Retry Handler

**Responsibility:** Implement retry logic with exponential backoff for transient failures.

**Interface:**
```python
class RetryHandler:
    def __init__(self, max_attempts: int = 3, base_delay: float = 1.0):
        """
        Initialize retry handler.
        
        Args:
            max_attempts: Maximum number of retry attempts
            base_delay: Base delay in seconds for exponential backoff
        """
        pass
    
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
        pass
    
    def _is_transient_error(self, error: Exception) -> bool:
        """Determine if error is transient and should be retried."""
        pass
```

**Transient Error Detection:**
- Throttling errors (ThrottlingException)
- Network timeouts (ConnectTimeout, ReadTimeout)
- HTTP 5xx errors
- Service unavailable errors

**Backoff Strategy:**
- Exponential backoff: delay = base_delay * (2 ** attempt)
- Example: 1s, 2s, 4s for 3 attempts with base_delay=1.0

### 5. Result Formatter

**Responsibility:** Format query results as ASCII table or write to CSV/JSON files.

**Interface:**
```python
class ResultFormatter:
    def format_as_table(self, result: QueryResult, max_width: int = 50) -> str:
        """
        Format query result as ASCII table.
        
        Args:
            result: QueryResult with columns and rows
            max_width: Maximum width for column values before truncation
            
        Returns:
            Formatted ASCII table string
        """
        pass
    
    def write_to_csv(self, result: QueryResult, file_path: str) -> None:
        """
        Write query result to CSV file.
        
        Args:
            result: QueryResult with columns and rows
            file_path: Path to output CSV file
            
        Raises:
            IOError: If file cannot be written
        """
        pass
    
    def write_to_json(self, result: QueryResult, file_path: str) -> None:
        """
        Write query result to JSON file.
        
        Args:
            result: QueryResult with columns and rows
            file_path: Path to output JSON file
            
        Raises:
            IOError: If file cannot be written
        """
        pass
    
    def _truncate_value(self, value: str, max_width: int) -> str:
        """Truncate value if longer than max_width."""
        pass
    
    def _format_value(self, value: Any) -> str:
        """Format value for display (handle NULL, types)."""
        pass
```

**File Format Specifications:**

**CSV Format:**
- First row contains column headers
- NULL values represented as empty strings
- Values properly escaped according to CSV standards
- UTF-8 encoding

**JSON Format:**
```json
{
  "columns": [
    {"name": "col1", "type": "varchar"},
    {"name": "col2", "type": "integer"}
  ],
  "rows": [
    {"col1": "value1", "col2": 123},
    {"col1": "value2", "col2": null}
  ],
  "row_count": 2
}
```

## Data Models

### Config
```python
@dataclass
class AWSConfig:
    profile: Optional[str]
    region: str

@dataclass
class AthenaConfig:
    database: str
    workgroup: str
    output_location: str

@dataclass
class OutputConfig:
    format: str = "table"  # Options: "table", "csv", "json"
    file: Optional[str] = None  # File path for csv/json output

@dataclass
class QueryConfig:
    name: str
    sql: str

@dataclass
class Config:
    aws: AWSConfig
    athena: AthenaConfig
    output: OutputConfig
    queries: List[QueryConfig]
```

### QueryResult
```python
@dataclass
class Column:
    name: str
    type: str

@dataclass
class QueryResult:
    columns: List[Column]
    rows: List[List[Any]]
    row_count: int
```

## Correctness Properties


A property is a characteristic or behavior that should hold true across all valid executions of a system—essentially, a formal statement about what the system should do. Properties serve as the bridge between human-readable specifications and machine-verifiable correctness guarantees.

### Property 1: Configuration Parsing Completeness

*For any* valid YAML configuration file containing AWS settings, Athena settings, and queries, parsing the file should extract all specified fields correctly, including database name, workgroup name, S3 output location, and all query definitions with their names and SQL statements.

**Validates: Requirements 1.1, 1.4, 1.5**

### Property 2: Required Field Validation

*For any* configuration file missing one or more required fields (database, workgroup, output_location, or queries), the configuration parser should reject it with a validation error indicating which required field is missing.

**Validates: Requirements 1.6, 6.3**

### Property 3: Credential Provider Chain Order

*For any* authentication attempt, the Authentication Manager should try credential sources in the correct order: environment variables first, then specified profile, then credentials file, then IAM role.

**Validates: Requirements 2.6**

### Property 4: Query Submission with Configuration

*For any* valid SQL query and configuration, when submitting the query to Athena, the Query Executor should use the database, workgroup, and S3 output location specified in the configuration.

**Validates: Requirements 3.1, 3.2, 3.3**

### Property 5: Query Polling Until Completion

*For any* submitted query, the Query Executor should continue polling the query status until it reaches a terminal state (SUCCEEDED, FAILED, or CANCELLED).

**Validates: Requirements 3.4, 3.5**

### Property 6: Transient Error Retry

*For any* transient error (throttling, timeout, 5xx HTTP error) occurring during any AWS API call, the Retry Handler should retry the operation up to the maximum number of attempts.

**Validates: Requirements 4.1, 4.2**

### Property 7: Exponential Backoff

*For any* retry sequence, the delay between attempts should increase exponentially, where delay(n) = base_delay * (2^n) for attempt n.

**Validates: Requirements 4.3**

### Property 8: Non-Transient Error Immediate Failure

*For any* non-transient error (authentication failure, invalid query syntax, resource not found), the Retry Handler should not retry and should return the error immediately without delay.

**Validates: Requirements 4.4**

### Property 9: Retry Exhaustion

*For any* operation that fails with transient errors exceeding the maximum retry attempts, the Retry Handler should return the last error encountered.

**Validates: Requirements 4.5**

### Property 10: Transient Error Classification

*For any* error, the Retry Handler should correctly classify it as transient if and only if it is a throttling error, network timeout, or 5xx HTTP error.

**Validates: Requirements 4.6**

### Property 11: Result Formatting with Headers

*For any* query result with columns and rows, the Result Formatter should produce an ASCII table that includes all column headers and all row data.

**Validates: Requirements 5.1, 5.2**

### Property 12: NULL Value Display

*For any* query result containing NULL values, the Result Formatter should display each NULL as the string "NULL" in the formatted output.

**Validates: Requirements 5.4**

### Property 13: Value Truncation

*For any* column value exceeding the maximum width, the Result Formatter should truncate it and append an ellipsis indicator ("...").

**Validates: Requirements 5.6**

### Property 14: Data Type Formatting

*For any* query result containing various data types (strings, integers, floats, dates, booleans), the Result Formatter should format each type appropriately as a string representation.

**Validates: Requirements 5.7**

### Property 15: AWS Error Message Propagation

*For any* AWS API error, the error message displayed to the user should include both the AWS error code and the error message from AWS.

**Validates: Requirements 6.2**

### Property 16: Athena Query Error Propagation

*For any* query that fails in Athena with a syntax or execution error, the error message displayed should include the error message from Athena.

**Validates: Requirements 6.4**

### Property 17: Exit Code Behavior

*For any* execution, the tool should exit with status code 0 if and only if all queries complete successfully; otherwise it should exit with a non-zero status code.

**Validates: Requirements 6.5, 6.6**

### Property 18: Optional Field Defaults

*For any* configuration file with optional fields omitted (such as AWS profile or region), the configuration parser should apply sensible default values.

**Validates: Requirements 7.5**

### Property 19: CSV File Output Format

*For any* query result, when writing to CSV format, the output file should contain column headers in the first row, followed by data rows with NULL values represented as empty strings, and all values properly escaped according to CSV standards.

**Validates: Requirements 5.8, 5.9**

### Property 20: JSON File Output Format

*For any* query result, when writing to JSON format, the output file should contain a valid JSON object with "columns", "rows", and "row_count" fields, where NULL values are represented as JSON null.

**Validates: Requirements 5.8, 5.9**

### Property 21: File Output Success

*For any* valid file path and query result, when writing to CSV or JSON format, the operation should complete successfully and create a readable file at the specified path.

**Validates: Requirements 5.8**

## Error Handling

### Error Categories

1. **Configuration Errors**
   - Missing configuration file
   - Invalid YAML syntax
   - Missing required fields
   - Invalid field values
   - Invalid output format specification

2. **Authentication Errors**
   - No valid credentials found
   - Invalid credentials
   - Insufficient permissions

3. **Query Execution Errors**
   - Invalid SQL syntax
   - Table or database not found
   - Query timeout
   - Insufficient permissions

4. **AWS Service Errors**
   - Throttling (transient)
   - Service unavailable (transient)
   - Network timeouts (transient)
   - Internal server errors (transient)

5. **File Output Errors**
   - Invalid file path
   - Permission denied when writing file
   - Disk full or I/O errors
   - Invalid output format specified

### Error Handling Strategy

- All errors should be caught at the appropriate layer
- Error messages should be descriptive and actionable
- Stack traces should only be shown in debug mode
- Exit codes should indicate error category:
  - 0: Success
  - 1: Configuration error
  - 2: Authentication error
  - 3: Query execution error
  - 4: AWS service error
  - 5: File output error

## Testing Strategy

### Dual Testing Approach

The testing strategy employs both unit tests and property-based tests to ensure comprehensive coverage:

- **Unit tests**: Verify specific examples, edge cases, and error conditions
- **Property tests**: Verify universal properties across all inputs

Both approaches are complementary and necessary. Unit tests catch concrete bugs in specific scenarios, while property tests verify general correctness across a wide range of inputs.

### Property-Based Testing

We will use the **Hypothesis** library for Python to implement property-based tests. Each correctness property defined above will be implemented as a property-based test.

**Configuration:**
- Each property test will run a minimum of 100 iterations
- Each test will be tagged with a comment referencing the design property
- Tag format: `# Feature: athena-query-tool, Property N: [property text]`

**Example Property Test:**
```python
from hypothesis import given, strategies as st

# Feature: athena-query-tool, Property 1: Configuration Parsing Completeness
@given(
    database=st.text(min_size=1),
    workgroup=st.text(min_size=1),
    output_location=st.text(min_size=1),
    queries=st.lists(st.tuples(st.text(min_size=1), st.text(min_size=1)), min_size=1)
)
@settings(max_examples=100)
def test_config_parsing_completeness(database, workgroup, output_location, queries):
    # Generate config, parse it, verify all fields extracted correctly
    pass
```

### Unit Testing

Unit tests will focus on:

1. **Edge Cases**
   - Missing configuration file
   - Invalid YAML syntax
   - Empty query results
   - Query failures
   - Zero-row results
   - Invalid file paths for output
   - File write permission errors

2. **Integration Points**
   - Boto3 client initialization
   - Athena API interactions (mocked)
   - File I/O operations (CSV and JSON writing)

3. **Specific Examples**
   - Environment variable authentication
   - Profile-based authentication
   - Specific error message formats
   - CSV output with special characters
   - JSON output with NULL values
   - Table output to stdout

### Test Coverage Goals

- Minimum 90% code coverage
- All error paths tested
- All edge cases covered
- All properties verified through property-based tests

### Mocking Strategy

- Mock boto3 Athena client for unit tests
- Use real YAML parsing (no mocking)
- Mock file system operations where appropriate
- Use dependency injection to facilitate testing

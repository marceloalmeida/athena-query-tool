# Requirements Document

## Introduction

This document specifies the requirements for an AWS Athena Query Tool that executes SQL queries against AWS Athena and displays results in an ASCII table format. The tool reads configuration from a file, supports multiple AWS authentication methods, and includes retry logic for transient failures.

## Glossary

- **Athena_Query_Tool**: The Python application that executes SQL queries against AWS Athena
- **Configuration_File**: A file containing SQL queries and Athena configuration parameters
- **Query_Executor**: The component responsible for submitting queries to AWS Athena
- **Result_Formatter**: The component that formats query results into ASCII table format
- **Authentication_Manager**: The component that handles AWS credential resolution
- **Retry_Handler**: The component that implements retry logic for transient failures
- **Cache_Manager**: The component that manages local caching of query execution IDs and results

## Requirements

### Requirement 1: Configuration File Management

**User Story:** As a user, I want to define SQL queries and Athena settings in a configuration file, so that I can easily manage and version control my queries.

#### Acceptance Criteria

1. WHEN a configuration file path is provided, THE Athena_Query_Tool SHALL parse the file and extract SQL queries and Athena settings
2. WHEN the configuration file is missing, THE Athena_Query_Tool SHALL return a descriptive error message
3. WHEN the configuration file contains invalid syntax, THE Athena_Query_Tool SHALL return a descriptive error indicating the location of the syntax error
4. THE Configuration_File SHALL support specifying database name, workgroup name, and S3 output location
5. THE Configuration_File SHALL support specifying one or more SQL queries
6. WHEN parsing the configuration file, THE Athena_Query_Tool SHALL validate that all required fields are present

### Requirement 2: AWS Authentication

**User Story:** As a user, I want flexible authentication options, so that I can use the tool in different environments (local development, EC2, Lambda).

#### Acceptance Criteria

1. WHEN AWS credentials are available in environment variables, THE Authentication_Manager SHALL use those credentials
2. WHEN AWS credentials are not in environment variables, THE Authentication_Manager SHALL attempt to use the AWS credentials file
3. WHEN running on AWS infrastructure with an IAM role, THE Authentication_Manager SHALL use the IAM role credentials
4. WHEN an AWS profile name is specified in the configuration, THE Authentication_Manager SHALL use that profile
5. WHEN no valid credentials are found, THE Authentication_Manager SHALL return a descriptive error message
6. THE Authentication_Manager SHALL follow the standard AWS credential provider chain

### Requirement 3: Query Execution

**User Story:** As a user, I want to execute SQL queries against Athena, so that I can retrieve data from my data lake.

#### Acceptance Criteria

1. WHEN a valid SQL query is provided, THE Query_Executor SHALL submit the query to AWS Athena
2. WHEN a query is submitted, THE Query_Executor SHALL use the database and workgroup specified in the configuration
3. WHEN a query is submitted, THE Query_Executor SHALL specify the S3 output location from the configuration
4. WHEN a query is executing, THE Query_Executor SHALL poll for query completion status
5. WHEN a query completes successfully, THE Query_Executor SHALL retrieve the result set
6. WHEN a query fails, THE Query_Executor SHALL return the error message from Athena
7. THE Query_Executor SHALL support queries that return zero rows

### Requirement 4: Retry Logic

**User Story:** As a user, I want automatic retry for transient failures, so that temporary network issues don't cause my queries to fail unnecessarily.

#### Acceptance Criteria

1. WHEN a transient error occurs during query submission, THE Retry_Handler SHALL retry the operation up to a maximum number of attempts
2. WHEN a transient error occurs during result retrieval, THE Retry_Handler SHALL retry the operation up to a maximum number of attempts
3. WHEN retrying, THE Retry_Handler SHALL use exponential backoff between attempts
4. WHEN a non-transient error occurs, THE Retry_Handler SHALL not retry and SHALL return the error immediately
5. WHEN maximum retry attempts are exhausted, THE Retry_Handler SHALL return the last error encountered
6. THE Retry_Handler SHALL consider throttling errors, network timeouts, and 5xx HTTP errors as transient

### Requirement 5: Result Formatting

**User Story:** As a user, I want query results displayed in a readable ASCII table format, so that I can easily view the data in my terminal. It also should be possible to write the output (json os csv format) to file

#### Acceptance Criteria

1. WHEN query results are retrieved, THE Result_Formatter SHALL format them as an ASCII table
2. WHEN formatting results, THE Result_Formatter SHALL include column headers
3. WHEN formatting results, THE Result_Formatter SHALL align columns appropriately
4. WHEN a column value is NULL, THE Result_Formatter SHALL display it as "NULL"
5. WHEN results contain zero rows, THE Result_Formatter SHALL display the column headers with a message indicating no rows returned
6. WHEN column values are very long, THE Result_Formatter SHALL truncate them with an ellipsis indicator
7. THE Result_Formatter SHALL handle various data types including strings, numbers, dates, and booleans
8. WHEN Is asked to output the content to file
9. THE result should be write to CSV or JSON files

### Requirement 6: Error Handling

**User Story:** As a user, I want clear error messages, so that I can quickly understand and fix issues.

#### Acceptance Criteria

1. WHEN an error occurs, THE Athena_Query_Tool SHALL display a descriptive error message to the user
2. WHEN an AWS API error occurs, THE Athena_Query_Tool SHALL include the AWS error code and message
3. WHEN a configuration error occurs, THE Athena_Query_Tool SHALL indicate which configuration field is invalid
4. WHEN a query syntax error occurs, THE Athena_Query_Tool SHALL display the error message from Athena
5. THE Athena_Query_Tool SHALL exit with a non-zero status code when an error occurs
6. THE Athena_Query_Tool SHALL exit with status code 0 when execution completes successfully

### Requirement 7: Configuration File Format

**User Story:** As a user, I want a simple and readable configuration file format, so that I can easily create and modify configurations.

#### Acceptance Criteria

1. THE Configuration_File SHALL use JSON or YAML format
2. THE Configuration_File SHALL support comments for documentation
3. WHEN multiple queries are specified, THE Configuration_File SHALL allow naming each query
4. THE Configuration_File SHALL have a clear schema that is easy to understand
5. WHEN optional fields are omitted, THE Athena_Query_Tool SHALL use sensible defaults

### Requirement 8: Query Result Caching

**User Story:** As a user, I want query results to be cached locally, so that I can avoid re-executing expensive queries when the results are still fresh.

#### Acceptance Criteria

1. WHEN caching is enabled and a query is executed, THE Cache_Manager SHALL store the execution ID, query SQL, timestamp, and S3 result location locally
2. WHEN a query is submitted and caching is enabled, THE Cache_Manager SHALL check if a cached entry exists for that query SQL
3. WHEN a cached entry exists, THE Cache_Manager SHALL validate if the execution ID still exists on the S3 results bucket
4. WHEN a cached entry exists, THE Cache_Manager SHALL validate if the cached data is fresh based on the configured TTL value
5. WHEN a cached entry is valid and fresh, THE Query_Executor SHALL reuse the cached execution ID instead of executing a new query
6. WHEN a cached entry is invalid or stale, THE Query_Executor SHALL execute a new query and update the cache
7. WHEN no cached entry exists, THE Query_Executor SHALL execute a new query and create a cache entry
8. THE Configuration_File SHALL support cache_enabled boolean to enable or disable caching
9. THE Configuration_File SHALL support cache_ttl_seconds to configure freshness duration with a default of 3600 seconds
10. THE Configuration_File SHALL support cache_directory to specify local cache storage location with a default of .athena_cache/
11. THE cache directory SHALL be excluded from version control by adding it to .gitignore
12. WHEN the cache directory does not exist, THE Cache_Manager SHALL create it automatically
# Implementation Plan: AWS Athena Query Tool

## Overview

This implementation plan breaks down the AWS Athena Query Tool into discrete coding tasks. The tool will execute SQL queries against AWS Athena and display results in ASCII table format or write them to CSV/JSON files. The implementation follows a modular architecture with clear separation between configuration, authentication, query execution, retry logic, and result formatting.

## Tasks

- [x] 1. Set up project structure and dependencies
  - Create Python package structure with appropriate directories
  - Set up requirements.txt with dependencies: boto3, PyYAML, tabulate (or prettytable), hypothesis (for testing)
  - Create main entry point script
  - Set up basic logging configuration
  - _Requirements: All_

- [ ] 2. Implement configuration management
  - [x] 2.1 Create Config data models
    - Define AWSConfig, AthenaConfig, OutputConfig, QueryConfig, and Config dataclasses
    - _Requirements: 1.4, 1.5, 7.1, 7.4_
  
  - [x] 2.2 Implement ConfigurationManager class
    - Implement load_config() method to parse YAML files
    - Implement validation for required fields (database, workgroup, output_location, queries)
    - Implement default value handling for optional fields (profile, region, output format)
    - Handle file not found errors with descriptive messages
    - Handle YAML syntax errors with location information
    - _Requirements: 1.1, 1.2, 1.3, 1.6, 6.3, 7.5_
  
  - [ ]* 2.3 Write property test for configuration parsing completeness
    - **Property 1: Configuration Parsing Completeness**
    - **Validates: Requirements 1.1, 1.4, 1.5**
  
  - [ ]* 2.4 Write property test for required field validation
    - **Property 2: Required Field Validation**
    - **Validates: Requirements 1.6, 6.3**
  
  - [ ]* 2.5 Write property test for optional field defaults
    - **Property 18: Optional Field Defaults**
    - **Validates: Requirements 7.5**
  
  - [ ]* 2.6 Write unit tests for configuration edge cases
    - Test missing configuration file
    - Test invalid YAML syntax
    - Test invalid output format values
    - _Requirements: 1.2, 1.3_

- [ ] 3. Implement AWS authentication
  - [x] 3.1 Create AuthenticationManager class
    - Implement get_session() method using boto3.Session
    - Implement credential resolution following AWS credential provider chain
    - Support environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
    - Support profile-based authentication
    - Support IAM role credentials
    - Handle authentication errors with descriptive messages
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 6.2_
  
  - [ ]* 3.2 Write property test for credential provider chain order
    - **Property 3: Credential Provider Chain Order**
    - **Validates: Requirements 2.6**
  
  - [ ]* 3.3 Write unit tests for authentication scenarios
    - Test environment variable authentication
    - Test profile-based authentication
    - Test missing credentials error handling
    - _Requirements: 2.1, 2.2, 2.4, 2.5_

- [x] 4. Checkpoint - Ensure configuration and authentication work
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 5. Implement retry logic
  - [x] 5.1 Create RetryHandler class
    - Implement execute_with_retry() method with exponential backoff
    - Implement _is_transient_error() to classify errors
    - Support configurable max_attempts and base_delay
    - Classify throttling, timeouts, and 5xx errors as transient
    - Implement exponential backoff: delay = base_delay * (2 ** attempt)
    - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.5, 4.6_
  
  - [ ]* 5.2 Write property test for transient error retry
    - **Property 6: Transient Error Retry**
    - **Validates: Requirements 4.1, 4.2**
  
  - [ ]* 5.3 Write property test for exponential backoff
    - **Property 7: Exponential Backoff**
    - **Validates: Requirements 4.3**
  
  - [ ]* 5.4 Write property test for non-transient error immediate failure
    - **Property 8: Non-Transient Error Immediate Failure**
    - **Validates: Requirements 4.4**
  
  - [ ]* 5.5 Write property test for retry exhaustion
    - **Property 9: Retry Exhaustion**
    - **Validates: Requirements 4.5**
  
  - [ ]* 5.6 Write property test for transient error classification
    - **Property 10: Transient Error Classification**
    - **Validates: Requirements 4.6**
  
  - [ ]* 5.7 Write unit tests for retry edge cases
    - Test maximum retry attempts exhaustion
    - Test immediate failure on non-transient errors
    - _Requirements: 4.4, 4.5_

- [ ] 6. Implement query execution
  - [x] 6.1 Create QueryResult data model
    - Define Column and QueryResult dataclasses
    - _Requirements: 3.5_
  
  - [x] 6.2 Create QueryExecutor class
    - Implement __init__() to accept Athena client and config
    - Implement _submit_query() to submit queries with database, workgroup, and S3 output location
    - Implement _wait_for_completion() to poll query status until terminal state
    - Implement _get_results() to retrieve result set
    - Implement execute_query() to orchestrate submission, polling, and retrieval
    - Integrate RetryHandler for all AWS API calls
    - Handle query failures with Athena error messages
    - Support zero-row results
    - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.7, 6.4_
  
  - [ ]* 6.3 Write property test for query submission with configuration
    - **Property 4: Query Submission with Configuration**
    - **Validates: Requirements 3.1, 3.2, 3.3**
  
  - [ ]* 6.4 Write property test for query polling until completion
    - **Property 5: Query Polling Until Completion**
    - **Validates: Requirements 3.4, 3.5**
  
  - [ ]* 6.5 Write property test for Athena query error propagation
    - **Property 16: Athena Query Error Propagation**
    - **Validates: Requirements 6.4**
  
  - [ ]* 6.6 Write unit tests for query execution scenarios
    - Test successful query execution
    - Test query failure handling
    - Test zero-row results
    - _Requirements: 3.6, 3.7_

- [x] 7. Checkpoint - Ensure query execution works
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 8. Implement result formatting
  - [x] 8.1 Create ResultFormatter class
    - Implement format_as_table() for ASCII table output
    - Implement _format_value() to handle NULL values and various data types
    - Implement _truncate_value() for long values with ellipsis
    - Handle zero-row results with headers and message
    - Use tabulate or prettytable library for table formatting
    - _Requirements: 5.1, 5.2, 5.3, 5.4, 5.5, 5.6, 5.7_
  
  - [x] 8.2 Implement CSV file output
    - Implement write_to_csv() method using Python's csv module
    - Write column headers in first row
    - Represent NULL values as empty strings
    - Properly escape values according to CSV standards
    - Use UTF-8 encoding
    - Handle file write errors with descriptive messages
    - _Requirements: 5.8, 5.9_
  
  - [x] 8.3 Implement JSON file output
    - Implement write_to_json() method using Python's json module
    - Create JSON object with "columns", "rows", and "row_count" fields
    - Represent NULL values as JSON null
    - Use proper indentation for readability
    - Handle file write errors with descriptive messages
    - _Requirements: 5.8, 5.9_
  
  - [ ]* 8.4 Write property test for result formatting with headers
    - **Property 11: Result Formatting with Headers**
    - **Validates: Requirements 5.1, 5.2**
  
  - [ ]* 8.5 Write property test for NULL value display
    - **Property 12: NULL Value Display**
    - **Validates: Requirements 5.4**
  
  - [ ]* 8.6 Write property test for value truncation
    - **Property 13: Value Truncation**
    - **Validates: Requirements 5.6**
  
  - [ ]* 8.7 Write property test for data type formatting
    - **Property 14: Data Type Formatting**
    - **Validates: Requirements 5.7**
  
  - [ ]* 8.8 Write property test for CSV file output format
    - **Property 19: CSV File Output Format**
    - **Validates: Requirements 5.8, 5.9**
  
  - [ ]* 8.9 Write property test for JSON file output format
    - **Property 20: JSON File Output Format**
    - **Validates: Requirements 5.8, 5.9**
  
  - [ ]* 8.10 Write property test for file output success
    - **Property 21: File Output Success**
    - **Validates: Requirements 5.8**
  
  - [ ]* 8.11 Write unit tests for result formatting edge cases
    - Test zero-row results display
    - Test very long column values
    - Test CSV output with special characters
    - Test JSON output with NULL values
    - Test file write permission errors
    - _Requirements: 5.5, 5.6_

- [ ] 9. Implement CLI entry point and main orchestration
  - [x] 9.1 Create main CLI script
    - Parse command-line arguments (config file path, optional debug flag)
    - Initialize ConfigurationManager and load configuration
    - Initialize AuthenticationManager and create boto3 session
    - Create Athena client from session
    - Initialize QueryExecutor with client and configuration
    - Initialize ResultFormatter
    - Loop through queries in configuration and execute each
    - Format results based on output configuration (table, csv, or json)
    - Display results to stdout or write to file based on configuration
    - Handle all error categories with appropriate exit codes
    - _Requirements: 6.1, 6.2, 6.5, 6.6_
  
  - [ ]* 9.2 Write property test for AWS error message propagation
    - **Property 15: AWS Error Message Propagation**
    - **Validates: Requirements 6.2**
  
  - [ ]* 9.3 Write property test for exit code behavior
    - **Property 17: Exit Code Behavior**
    - **Validates: Requirements 6.5, 6.6**
  
  - [ ]* 9.4 Write integration tests
    - Test end-to-end flow with mocked Athena client
    - Test multiple queries execution
    - Test different output formats (table, CSV, JSON)
    - Test error handling and exit codes
    - _Requirements: All_

- [x] 10. Final checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP
- Each task references specific requirements for traceability
- Checkpoints ensure incremental validation
- Property tests validate universal correctness properties using Hypothesis library
- Unit tests validate specific examples and edge cases
- All AWS API calls should be wrapped with RetryHandler
- Use dependency injection to facilitate testing (pass clients and handlers as parameters)

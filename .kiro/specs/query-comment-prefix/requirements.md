# Requirements Document

## Introduction

This feature adds a SQL comment prefix to every query submitted by the Athena Query Tool, making queries identifiable and trackable in Athena's query history. The comment contains the tool name (and optionally the query name from configuration) so that operators can filter and attribute queries back to this tool.

## Glossary

- **Query_Prefix**: A SQL line comment (`-- <text>\n`) prepended to the original SQL query string before submission to Athena.
- **Tool_Name**: The identifier for this application, defaulting to `athena-query-tool`.
- **Query_Executor**: The component (`executor.py`) responsible for submitting SQL queries to the Athena API.
- **Configuration_Manager**: The component (`config.py`) responsible for loading and validating YAML configuration.
- **CLI**: The command-line interface entry point (`cli.py`).
- **Web_Interface**: The Flask web application (`web.py`) that accepts ad-hoc SQL queries via HTTP.

## Requirements

### Requirement 1: Prefix Queries with Tool Name Comment

**User Story:** As a platform engineer, I want every SQL query submitted through the tool to be prefixed with a comment containing the tool name, so that I can identify and track queries originating from this tool in Athena's query history.

#### Acceptance Criteria

1. WHEN a SQL query is submitted for execution, THE Query_Executor SHALL prepend a SQL comment line in the format `-- [athena-query-tool]\n` to the query string before sending it to the Athena API.
2. THE Query_Executor SHALL preserve the original SQL query text unchanged after the comment prefix.
3. WHEN a query has a configured name, THE Query_Executor SHALL include the query name in the comment in the format `-- [athena-query-tool] query_name=<name>\n`.
4. WHEN a query is submitted via the Web_Interface without a configured name, THE Query_Executor SHALL prepend the comment with only the tool name and no query name.

### Requirement 2: Configurable Comment Prefix

**User Story:** As a developer, I want to optionally customize the tool name used in the comment prefix, so that I can distinguish between different deployments or instances of the tool.

#### Acceptance Criteria

1. THE Configuration_Manager SHALL support an optional `tool_name` field under a `query_prefix` section in the YAML configuration.
2. WHEN the `tool_name` field is provided in configuration, THE Query_Executor SHALL use the configured value in the comment prefix instead of the default.
3. WHEN the `tool_name` field is omitted from configuration, THE Query_Executor SHALL use `athena-query-tool` as the default tool name.
4. IF the `tool_name` field is provided as an empty string, THEN THE Configuration_Manager SHALL raise a configuration error indicating that `tool_name` must be a non-empty string.

### Requirement 3: Comment Prefix Does Not Affect Query Caching

**User Story:** As a user, I want the comment prefix to not break query caching, so that repeated queries still benefit from cached results.

#### Acceptance Criteria

1. WHEN checking the cache for a previously executed query, THE Query_Executor SHALL use the original SQL text (without the comment prefix) as the cache key.
2. WHEN storing a query execution in the cache, THE Query_Executor SHALL use the original SQL text (without the comment prefix) as the cache key.

### Requirement 4: Comment Prefix Applied Across All Execution Paths

**User Story:** As a platform engineer, I want the comment prefix applied regardless of how the query is triggered, so that all queries from this tool are consistently identifiable.

#### Acceptance Criteria

1. WHEN a query is executed via the CLI, THE Query_Executor SHALL prepend the comment prefix to the query.
2. WHEN a query is executed via the Web_Interface, THE Query_Executor SHALL prepend the comment prefix to the query.

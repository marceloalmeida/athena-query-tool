# Implementation Plan: Query Comment Prefix

## Overview

Add a SQL comment prefix to every query submitted by the Athena Query Tool. The implementation touches two modules: `config.py` for parsing the new `query_prefix` configuration section, and `executor.py` for building and prepending the comment. CLI and Web callers pass through the executor, so the prefix is applied consistently. Cache keys remain based on the original un-prefixed SQL.

## Tasks

- [x] 1. Add `QueryPrefixConfig` dataclass and config parsing
  - [x] 1.1 Add `QueryPrefixConfig` dataclass to `athena_query_tool/config.py`
    - Create `QueryPrefixConfig` dataclass with `tool_name: str` field defaulting to `"athena-query-tool"`
    - Add `query_prefix: QueryPrefixConfig` field to the existing `Config` dataclass
    - _Requirements: 2.1, 2.3_

  - [x] 1.2 Implement `_parse_query_prefix_config` static method in `ConfigurationManager`
    - Parse the optional `query_prefix` section from the YAML dict
    - Return default `QueryPrefixConfig()` when section is absent
    - Return default tool name when section is present but `tool_name` key is absent
    - Raise `ConfigurationError` when `tool_name` is an empty string
    - Raise `ConfigurationError` when `tool_name` is not a string (e.g. integer, list)
    - Wire the new parser into `load_config` so `Config` includes `query_prefix`
    - _Requirements: 2.1, 2.2, 2.3, 2.4_

  - [ ]* 1.3 Write unit tests for `_parse_query_prefix_config`
    - Test default when `query_prefix` section is absent
    - Test custom `tool_name` is used when provided
    - Test default tool name when section present but `tool_name` key absent
    - Test `ConfigurationError` raised for empty string `tool_name`
    - Test `ConfigurationError` raised for non-string `tool_name`
    - _Requirements: 2.1, 2.2, 2.3, 2.4_

- [x] 2. Implement prefix building and query prefixing in `QueryExecutor`
  - [x] 2.1 Add `_build_prefix` method to `QueryExecutor` in `athena_query_tool/executor.py`
    - Accept `QueryPrefixConfig` in `__init__` (with a default) and store it
    - Implement `_build_prefix(query_name: Optional[str] = None) -> str` that returns `-- [<tool_name>]\n` or `-- [<tool_name>] query_name=<name>\n`
    - _Requirements: 1.1, 1.2, 1.3, 1.4_

  - [x] 2.2 Modify `execute_query` to prepend the prefix before submission
    - Add optional `query_name: Optional[str] = None` parameter to `execute_query`
    - Keep using the original `sql` for cache lookup (`get_cached_execution`) and cache storage (`store_execution`)
    - Build the prefixed SQL via `_build_prefix(query_name)` and pass it to `_submit_query`
    - _Requirements: 1.1, 1.2, 3.1, 3.2_

  - [ ]* 2.3 Write property test for prefix format correctness
    - **Property 1: Prefix format correctness**
    - Generate random SQL strings, non-empty tool names, and optional query names using Hypothesis
    - Assert the combined output starts with `-- [<tool_name>]`, contains `query_name=<name>` iff a query name was provided, and ends with the original SQL byte-for-byte unchanged
    - **Validates: Requirements 1.1, 1.2, 1.3, 1.4**

  - [ ]* 2.4 Write property test for custom tool name in prefix
    - **Property 2: Custom tool name appears in prefix**
    - Generate random non-empty tool name strings using Hypothesis
    - Assert the prefix contains the exact tool name between square brackets and does not contain the default unless it equals the default
    - **Validates: Requirements 2.1, 2.2, 2.3**

  - [ ]* 2.5 Write unit tests for `_build_prefix` and `execute_query` prefixing
    - Test `_build_prefix` output with no query name
    - Test `_build_prefix` output with a query name
    - Test `_build_prefix` output with a custom tool name
    - Test that `_submit_query` receives the prefixed SQL (mock Athena client, inspect `QueryString` argument)
    - Test that cache methods receive the original un-prefixed SQL (mock `CacheManager`)
    - _Requirements: 1.1, 1.2, 1.3, 3.1, 3.2_

- [x] 3. Checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 4. Wire prefix through CLI and Web execution paths
  - [x] 4.1 Pass `query_prefix` config and `query_name` through the CLI path
    - In `athena_query_tool/cli.py`, pass `config.query_prefix` when constructing `QueryExecutor`
    - Pass `query_config.name` as `query_name` when calling `executor.execute_query`
    - _Requirements: 1.3, 4.1_

  - [x] 4.2 Pass `query_prefix` config through the Web path
    - In `athena_query_tool/web.py`, pass `config.query_prefix` when constructing `QueryExecutor`
    - Web queries call `executor.execute_query(sql)` without a `query_name` (default `None`)
    - _Requirements: 1.4, 4.2_

  - [ ]* 4.3 Write property test for cache key isolation
    - **Property 3: Cache key is always the original SQL**
    - Generate random SQL strings, execute through `QueryExecutor.execute_query` with a mocked Athena client and a spy on `CacheManager`
    - Assert the SQL passed to `get_cached_execution` and `store_execution` equals the original un-prefixed input
    - **Validates: Requirements 3.1, 3.2**

  - [ ]* 4.4 Write integration tests for CLI and Web paths
    - Test CLI path: mock `start_query_execution` and verify the `QueryString` argument starts with the expected comment prefix including `query_name`
    - Test Web path: use Flask test client to POST to `/api/query`, mock Athena client, verify `QueryString` starts with the tool-name-only prefix
    - _Requirements: 4.1, 4.2_

- [x] 5. Final checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP
- The design uses Python, so all code examples and tests use Python with Hypothesis for property-based testing
- Cache behaviour is unchanged; the prefix is applied after cache lookup and before Athena submission
- Each property test references a specific correctness property from the design document

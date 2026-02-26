# Implementation Plan: Web UI for Athena Query Tool

## Overview

Build a Flask-based web UI that wraps the existing Athena Query Tool CLI modules. The backend exposes REST endpoints for query execution and configuration retrieval. The frontend is a vanilla HTML/CSS/JS single-page application served by Flask as static files. Implementation proceeds bottom-up: backend API first, then frontend, then wiring and integration.

## Tasks

- [x] 1. Set up Flask backend with app factory and static serving
  - [x] 1.1 Create `athena_query_tool/web.py` with `create_app(config_path)` factory
    - Initialize Flask app with static folder pointing to `athena_query_tool/static/`
    - Accept `config_path` parameter, load config via `ConfigurationManager.load_config()`
    - Initialize `AuthenticationManager`, create boto3 session and Athena/S3 clients
    - Set up `QueryExecutor` with optional `CacheManager` (based on `config.cache.enabled`)
    - Store config, executor, and formatter instances on `app` for use in route handlers
    - Register a catch-all route for `/` that serves `index.html`
    - _Requirements: 1.1, 1.2, 1.3, 8.1_

  - [x] 1.2 Create `athena_query_tool/web_cli.py` entry point
    - Use `argparse` with `--config` argument (default: `config.yaml`)
    - Call `create_app(config_path)` and run the Flask dev server
    - On `ConfigurationError`, log the error and `sys.exit(1)`
    - _Requirements: 1.3, 7.2_

- [x] 2. Implement API endpoints
  - [x] 2.1 Implement `GET /api/config` endpoint in `web.py`
    - Return JSON: `{ "success": true, "data": { "region", "database", "workgroup" } }`
    - Map from stored `Config` object: `config.aws.region`, `config.athena.database`, `config.athena.workgroup`
    - _Requirements: 6.1, 6.2, 6.3, 6.4_

  - [x] 2.2 Implement `POST /api/query` endpoint in `web.py`
    - Validate request is JSON; return 400 if not
    - Validate `sql` field is present and non-empty after stripping whitespace; return 400 if invalid
    - Call `QueryExecutor.execute_query(sql)` and serialize `QueryResult` to JSON response
    - Include `columns` (with `name` and `type`), `rows`, `row_count`, and `from_cache` boolean
    - Determine `from_cache` by checking if `CacheManager.get_cached_execution()` returns a hit before execution
    - _Requirements: 2.1, 2.2, 2.5, 7.4, 8.2, 8.3_

  - [x] 2.3 Implement error handlers in `web.py`
    - Register Flask error handlers for `AuthenticationError` → 401, `QueryExecutionError` → 500
    - All error responses use `{ "success": false, "error": "<message>" }` format
    - Catch unexpected exceptions with a generic 500 handler
    - _Requirements: 2.5, 7.1, 7.4_

- [x] 3. Checkpoint - Backend API complete
  - Ensure all tests pass, ask the user if questions arise.

- [x] 4. Build the frontend single-page application
  - [x] 4.1 Create `athena_query_tool/static/index.html`
    - Configuration_Panel section: displays region, database, workgroup
    - Query_Editor section: `<textarea>` for SQL input + Execute button
    - Results_Panel section: `<table>` for results, row count display, "No results" message (hidden by default)
    - Export buttons: CSV and JSON download buttons (hidden/disabled when no results)
    - Error display area (hidden by default)
    - Loading indicator (hidden by default)
    - Link `style.css` and `app.js`
    - _Requirements: 1.1, 3.1, 3.2, 4.1, 4.2, 4.3, 4.4, 5.1, 5.2_

  - [x] 4.2 Create `athena_query_tool/static/style.css`
    - Style the layout: config panel, query editor, results table, error area, loading indicator
    - Style the results table with readable column headers and row data
    - Style disabled/loading states for the submit button
    - _Requirements: 1.2_

  - [x] 4.3 Create `athena_query_tool/static/app.js` with core application logic
    - `fetchConfig()`: GET `/api/config`, populate Configuration_Panel on page load
    - `executeQuery()`: POST `/api/query` with `{ "sql": "..." }`, manage loading state (disable button, show indicator), render results or display errors
    - `renderResults(data)`: Build table headers from `columns`, render rows, show row count, handle zero-row case with "No results returned" message
    - `exportCSV()`: Generate CSV blob from current results (headers in first row, data rows following), trigger file download
    - `exportJSON()`: Generate JSON blob with `columns`, `rows` (as objects keyed by column name), and `row_count`, trigger file download
    - Handle network errors: display "Connection error: unable to reach the server" on fetch failure
    - Handle API error responses: display the `error` message from `{ "success": false }` responses
    - Disable submit button during query execution, re-enable on completion
    - Hide export buttons when no results are loaded
    - _Requirements: 2.3, 2.4, 2.6, 3.3, 4.1, 4.2, 4.3, 4.4, 5.1, 5.2, 5.3, 5.4, 7.3_

- [x] 5. Checkpoint - Frontend complete
  - Ensure all tests pass, ask the user if questions arise.

- [x] 6. Backend API tests
  - [x] 6.1 Write unit tests in `tests/test_web.py` for backend API
    - Test `GET /` serves HTML
    - Test `GET /api/config` returns correct region, database, workgroup from config
    - Test `POST /api/query` with valid SQL returns columns, rows, row_count, from_cache
    - Test `POST /api/query` with missing `sql` field returns 400
    - Test `POST /api/query` with empty `sql` returns 400
    - Test `POST /api/query` with non-JSON body returns 400
    - Test authentication error returns 401
    - Test query execution error returns 500
    - Test cache integration: `from_cache` is `true` when cache hit occurs
    - Test startup exits with non-zero code on bad config
    - _Requirements: 1.1, 1.3, 2.1, 2.2, 2.5, 6.1, 6.2, 6.3, 6.4, 7.1, 7.2, 7.4, 8.2, 8.3_

  - [ ]* 6.2 Write property test for QueryResult serialization completeness
    - **Property 1: QueryResult serialization completeness**
    - Generate arbitrary `QueryResult` objects with random columns, rows, and row_count
    - Verify serialized JSON contains `columns` (each with `name` and `type`), `rows`, `row_count`, and `from_cache`
    - **Validates: Requirements 2.1, 2.2, 8.3**

  - [ ]* 6.3 Write property test for error propagation
    - **Property 2: Error propagation**
    - Generate arbitrary exception messages
    - Verify API response has `success: false` and `error` contains the original message
    - **Validates: Requirements 2.5, 7.1**

  - [ ]* 6.4 Write property test for request validation
    - **Property 3: Request validation rejects invalid input**
    - Generate requests with missing `sql`, empty `sql`, whitespace-only `sql`
    - Verify all return HTTP 400 with error response
    - **Validates: Requirements 7.4**

  - [ ]* 6.5 Write property test for configuration endpoint
    - **Property 4: Configuration endpoint returns loaded values**
    - Generate arbitrary region, database, workgroup strings
    - Verify `/api/config` response fields exactly match the loaded config values
    - **Validates: Requirements 6.1, 6.2, 6.3, 6.4**

- [ ] 7. Frontend export tests
  - [ ]* 7.1 Write property test for CSV export format
    - **Property 5: CSV export format correctness**
    - Generate arbitrary column names and row data
    - Verify CSV output has column names as first row, followed by data rows matching original data
    - **Validates: Requirements 5.3**

  - [ ]* 7.2 Write property test for JSON export format
    - **Property 6: JSON export format correctness**
    - Generate arbitrary columns and rows
    - Verify JSON export contains `columns` array, `rows` array (objects keyed by column name), and correct `row_count`
    - **Validates: Requirements 5.4**

- [x] 8. Final checkpoint - All tests pass
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP
- Each task references specific requirements for traceability
- The backend uses Flask's test client for unit testing; property tests use `hypothesis`
- Frontend export logic (CSV/JSON generation) can be property-tested via Python by replicating the JS logic or by testing the equivalent `ResultFormatter` methods
- Checkpoints ensure incremental validation between backend and frontend phases

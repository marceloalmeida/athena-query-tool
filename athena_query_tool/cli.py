"""Command-line interface for Athena Query Tool."""

import argparse
import logging
import sys


def setup_logging(debug: bool = False) -> None:
    """
    Configure logging for the application.
    
    Args:
        debug: Enable debug-level logging if True
    """
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )


def main() -> int:
    """
    Main entry point for the CLI.
    
    Returns:
        Exit code (0 for success, non-zero for errors)
    """
    parser = argparse.ArgumentParser(
        description='Execute SQL queries against AWS Athena'
    )
    parser.add_argument(
        'config',
        help='Path to configuration file (YAML format)'
    )
    parser.add_argument(
        '--debug',
        action='store_true',
        help='Enable debug logging'
    )
    
    args = parser.parse_args()
    setup_logging(args.debug)
    
    logger = logging.getLogger(__name__)
    logger.info(f"Starting Athena Query Tool with config: {args.config}")
    
    try:
        # Import required modules
        from .config import ConfigurationManager
        from .auth import AuthenticationManager
        from .executor import QueryExecutor
        from .formatter import ResultFormatter
        from .retry import RetryHandler
        from .exceptions import (
            ConfigurationError,
            AuthenticationError,
            QueryExecutionError,
            FileOutputError
        )
        
        # Initialize ConfigurationManager and load configuration
        logger.debug("Loading configuration...")
        config = ConfigurationManager.load_config(args.config)
        logger.info(f"Configuration loaded successfully: {len(config.queries)} queries found")
        
        # Initialize AuthenticationManager and create boto3 session
        logger.debug("Initializing AWS session...")
        auth_manager = AuthenticationManager()
        session = auth_manager.get_session(
            profile=config.aws.profile,
            region=config.aws.region
        )
        logger.info(f"AWS session created successfully (region: {config.aws.region})")
        
        # Create Athena client from session
        athena_client = session.client('athena')
        logger.debug("Athena client created")
        
        # Create S3 client from session
        s3_client = session.client('s3')
        logger.debug("S3 client created")
        
        # Initialize CacheManager if caching is enabled
        cache_manager = None
        if config.cache.enabled:
            from .cache import CacheManager
            cache_manager = CacheManager(config.cache, s3_client)
            logger.info(f"Cache enabled (TTL: {config.cache.ttl_seconds}s, directory: {config.cache.directory})")
        
        # Initialize RetryHandler
        retry_handler = RetryHandler()
        
        # Initialize QueryExecutor with client, configuration, and cache
        executor = QueryExecutor(athena_client, config.athena, retry_handler,
                                 s3_client=s3_client, cache_manager=cache_manager,
                                 query_prefix_config=config.query_prefix)
        logger.debug("QueryExecutor initialized")
        
        # Initialize ResultFormatter
        formatter = ResultFormatter()
        logger.debug("ResultFormatter initialized")
        
        # Loop through queries in configuration and execute each
        for i, query_config in enumerate(config.queries, 1):
            if query_config.skip:
                logger.info(f"Skipping query {i}/{len(config.queries)}: {query_config.name}")
                continue
            logger.info(f"Executing query {i}/{len(config.queries)}: {query_config.name}")
            logger.debug(f"SQL: {query_config.sql}")
            
            try:
                # Execute query
                result = executor.execute_query(query_config.sql, query_name=query_config.name)
                logger.info(f"Query '{query_config.name}' completed successfully: {result.row_count} rows returned")
                
                # Format results based on output configuration
                if config.output.format == 'table':
                    # Display results to stdout
                    table_output = formatter.format_as_table(result)
                    print(f"\n=== Query: {query_config.name} ===")
                    print(table_output)
                    print()
                    
                elif config.output.format == 'csv':
                    # Write to CSV file
                    if config.output.file:
                        file_path = config.output.file
                        # If multiple queries, append query name to filename
                        if len(config.queries) > 1:
                            # Insert query name before extension
                            parts = file_path.rsplit('.', 1)
                            if len(parts) == 2:
                                file_path = f"{parts[0]}_{query_config.name}.{parts[1]}"
                            else:
                                file_path = f"{file_path}_{query_config.name}"
                        
                        formatter.write_to_csv(result, file_path)
                        logger.info(f"Results written to CSV file: {file_path}")
                        print(f"Query '{query_config.name}': Results written to {file_path}")
                    else:
                        logger.error("CSV output format specified but no output file path provided")
                        print("Error: CSV output format requires 'output.file' to be specified in configuration")
                        return 1
                        
                elif config.output.format == 'json':
                    # Write to JSON file
                    if config.output.file:
                        file_path = config.output.file
                        # If multiple queries, append query name to filename
                        if len(config.queries) > 1:
                            # Insert query name before extension
                            parts = file_path.rsplit('.', 1)
                            if len(parts) == 2:
                                file_path = f"{parts[0]}_{query_config.name}.{parts[1]}"
                            else:
                                file_path = f"{file_path}_{query_config.name}"
                        
                        formatter.write_to_json(result, file_path)
                        logger.info(f"Results written to JSON file: {file_path}")
                        print(f"Query '{query_config.name}': Results written to {file_path}")
                    else:
                        logger.error("JSON output format specified but no output file path provided")
                        print("Error: JSON output format requires 'output.file' to be specified in configuration")
                        return 1
                
            except QueryExecutionError as e:
                logger.error(f"Query '{query_config.name}' failed: {str(e)}")
                print(f"\nError executing query '{query_config.name}': {str(e)}", file=sys.stderr)
                return 3  # Query execution error
                
            except FileOutputError as e:
                logger.error(f"Failed to write output file: {str(e)}")
                print(f"\nError writing output file: {str(e)}", file=sys.stderr)
                return 5  # File output error
        
        logger.info("All queries executed successfully")
        return 0  # Success
        
    except ConfigurationError as e:
        logger.error(f"Configuration error: {str(e)}")
        print(f"\nConfiguration Error: {str(e)}", file=sys.stderr)
        return 1  # Configuration error
        
    except AuthenticationError as e:
        logger.error(f"Authentication error: {str(e)}")
        print(f"\nAuthentication Error: {str(e)}", file=sys.stderr)
        return 2  # Authentication error
        
    except Exception as e:
        logger.exception("Unexpected error occurred")
        print(f"\nUnexpected Error: {str(e)}", file=sys.stderr)
        return 4  # AWS service error or other unexpected error


if __name__ == '__main__':
    sys.exit(main())

"""Configuration management for Athena Query Tool."""

import os
from dataclasses import dataclass
from typing import List, Optional

import yaml

from .exceptions import ConfigurationError


@dataclass
class AWSConfig:
    """AWS configuration settings."""
    profile: Optional[str]
    region: str


@dataclass
class AthenaConfig:
    """Athena-specific configuration settings."""
    database: str
    workgroup: str
    output_location: str


@dataclass
class OutputConfig:
    """Output format configuration."""
    format: str = "table"  # Options: "table", "csv", "json"
    file: Optional[str] = None  # File path for csv/json output


@dataclass
class QueryConfig:
    """Query configuration."""
    name: str
    sql: str


@dataclass
class Config:
    """Main configuration object."""
    aws: AWSConfig
    athena: AthenaConfig
    output: OutputConfig
    queries: List[QueryConfig]


class ConfigurationManager:
    """Manages configuration file loading and validation."""
    
    @staticmethod
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
        # Check if file exists
        if not os.path.exists(file_path):
            raise ConfigurationError(f"Configuration file not found: {file_path}")
        
        # Parse YAML file
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = yaml.safe_load(f)
        except yaml.YAMLError as e:
            # Extract location information from YAML error
            error_msg = f"Invalid YAML syntax in configuration file: {file_path}"
            if hasattr(e, 'problem_mark'):
                mark = e.problem_mark
                error_msg += f" (line {mark.line + 1}, column {mark.column + 1})"
            if hasattr(e, 'problem'):
                error_msg += f"\n{e.problem}"
            raise ConfigurationError(error_msg) from e
        except Exception as e:
            raise ConfigurationError(f"Error reading configuration file: {file_path}: {str(e)}") from e
        
        # Validate that data is a dictionary
        if not isinstance(data, dict):
            raise ConfigurationError(f"Configuration file must contain a YAML object/dictionary")
        
        # Validate and parse configuration sections
        try:
            aws_config = ConfigurationManager._parse_aws_config(data)
            athena_config = ConfigurationManager._parse_athena_config(data)
            output_config = ConfigurationManager._parse_output_config(data)
            queries = ConfigurationManager._parse_queries(data)
            
            return Config(
                aws=aws_config,
                athena=athena_config,
                output=output_config,
                queries=queries
            )
        except KeyError as e:
            raise ConfigurationError(f"Missing required configuration field: {str(e)}") from e
    
    @staticmethod
    def _parse_aws_config(data: dict) -> AWSConfig:
        """Parse AWS configuration section."""
        aws_data = data.get('aws', {})
        
        # Optional fields with defaults
        profile = aws_data.get('profile')
        region = aws_data.get('region', 'us-east-1')  # Default to us-east-1
        
        return AWSConfig(profile=profile, region=region)
    
    @staticmethod
    def _parse_athena_config(data: dict) -> AthenaConfig:
        """Parse Athena configuration section."""
        athena_data = data.get('athena')
        
        if not athena_data:
            raise ConfigurationError("Missing required configuration section: 'athena'")
        
        # Required fields
        required_fields = ['database', 'workgroup', 'output_location']
        for field in required_fields:
            if field not in athena_data:
                raise ConfigurationError(f"Missing required field in 'athena' section: '{field}'")
        
        database = athena_data['database']
        workgroup = athena_data['workgroup']
        output_location = athena_data['output_location']
        
        # Validate that required fields are not empty
        if not database or not isinstance(database, str):
            raise ConfigurationError("Field 'athena.database' must be a non-empty string")
        if not workgroup or not isinstance(workgroup, str):
            raise ConfigurationError("Field 'athena.workgroup' must be a non-empty string")
        if not output_location or not isinstance(output_location, str):
            raise ConfigurationError("Field 'athena.output_location' must be a non-empty string")
        
        return AthenaConfig(
            database=database,
            workgroup=workgroup,
            output_location=output_location
        )
    
    @staticmethod
    def _parse_output_config(data: dict) -> OutputConfig:
        """Parse output configuration section."""
        output_data = data.get('output', {})
        
        # Optional fields with defaults
        format_type = output_data.get('format', 'table')
        file_path = output_data.get('file')
        
        # Validate format type
        valid_formats = ['table', 'csv', 'json']
        if format_type not in valid_formats:
            raise ConfigurationError(
                f"Invalid output format: '{format_type}'. Must be one of: {', '.join(valid_formats)}"
            )
        
        return OutputConfig(format=format_type, file=file_path)
    
    @staticmethod
    def _parse_queries(data: dict) -> List[QueryConfig]:
        """Parse queries section."""
        queries_data = data.get('queries')
        
        if queries_data is None:
            raise ConfigurationError("Missing required configuration section: 'queries'")
        
        if not isinstance(queries_data, list):
            raise ConfigurationError("Field 'queries' must be a list")
        
        if len(queries_data) == 0:
            raise ConfigurationError("Field 'queries' must contain at least one query")
        
        queries = []
        for i, query_data in enumerate(queries_data):
            if not isinstance(query_data, dict):
                raise ConfigurationError(f"Query at index {i} must be an object/dictionary")
            
            # Required fields
            if 'name' not in query_data:
                raise ConfigurationError(f"Query at index {i} is missing required field: 'name'")
            if 'sql' not in query_data:
                raise ConfigurationError(f"Query at index {i} is missing required field: 'sql'")
            
            name = query_data['name']
            sql = query_data['sql']
            
            # Validate that required fields are not empty
            if not name or not isinstance(name, str):
                raise ConfigurationError(f"Query at index {i}: field 'name' must be a non-empty string")
            if not sql or not isinstance(sql, str):
                raise ConfigurationError(f"Query at index {i}: field 'sql' must be a non-empty string")
            
            queries.append(QueryConfig(name=name, sql=sql))
        
        return queries

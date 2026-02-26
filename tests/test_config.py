"""Unit tests for configuration management."""

from athena_query_tool.config import (
    AWSConfig,
    AthenaConfig,
    CacheConfig,
    OutputConfig,
    QueryConfig,
    Config,
)


def test_aws_config_creation():
    """Test AWSConfig dataclass creation."""
    aws_config = AWSConfig(profile="default", region="us-east-1")
    assert aws_config.profile == "default"
    assert aws_config.region == "us-east-1"


def test_aws_config_optional_profile():
    """Test AWSConfig with optional profile."""
    aws_config = AWSConfig(profile=None, region="us-west-2")
    assert aws_config.profile is None
    assert aws_config.region == "us-west-2"


def test_athena_config_creation():
    """Test AthenaConfig dataclass creation."""
    athena_config = AthenaConfig(
        database="my_database",
        workgroup="primary",
        output_location="s3://my-bucket/athena-results/",
    )
    assert athena_config.database == "my_database"
    assert athena_config.workgroup == "primary"
    assert athena_config.output_location == "s3://my-bucket/athena-results/"


def test_output_config_defaults():
    """Test OutputConfig with default values."""
    output_config = OutputConfig()
    assert output_config.format == "table"
    assert output_config.file is None


def test_output_config_with_file():
    """Test OutputConfig with file output."""
    output_config = OutputConfig(format="csv", file="/path/to/output.csv")
    assert output_config.format == "csv"
    assert output_config.file == "/path/to/output.csv"


def test_query_config_creation():
    """Test QueryConfig dataclass creation."""
    query_config = QueryConfig(name="test_query", sql="SELECT * FROM table")
    assert query_config.name == "test_query"
    assert query_config.sql == "SELECT * FROM table"


def test_config_creation():
    """Test complete Config dataclass creation."""
    aws_config = AWSConfig(profile="default", region="us-east-1")
    athena_config = AthenaConfig(
        database="my_database",
        workgroup="primary",
        output_location="s3://my-bucket/athena-results/",
    )
    output_config = OutputConfig(format="table")
    query_config = QueryConfig(name="test_query", sql="SELECT * FROM table")
    
    config = Config(
        aws=aws_config,
        athena=athena_config,
        cache=CacheConfig(),
        output=output_config,
        queries=[query_config],
    )
    
    assert config.aws.profile == "default"
    assert config.aws.region == "us-east-1"
    assert config.athena.database == "my_database"
    assert config.athena.workgroup == "primary"
    assert config.athena.output_location == "s3://my-bucket/athena-results/"
    assert config.output.format == "table"
    assert config.output.file is None
    assert len(config.queries) == 1
    assert config.queries[0].name == "test_query"
    assert config.queries[0].sql == "SELECT * FROM table"


def test_config_with_multiple_queries():
    """Test Config with multiple queries."""
    aws_config = AWSConfig(profile=None, region="us-west-2")
    athena_config = AthenaConfig(
        database="test_db",
        workgroup="test_wg",
        output_location="s3://test-bucket/results/",
    )
    output_config = OutputConfig(format="json", file="/tmp/output.json")
    queries = [
        QueryConfig(name="query1", sql="SELECT 1"),
        QueryConfig(name="query2", sql="SELECT 2"),
    ]
    
    config = Config(
        aws=aws_config,
        athena=athena_config,
        cache=CacheConfig(),
        output=output_config,
        queries=queries,
    )
    
    assert len(config.queries) == 2
    assert config.queries[0].name == "query1"
    assert config.queries[1].name == "query2"


import os
import tempfile
import pytest

from athena_query_tool.config import ConfigurationManager
from athena_query_tool.exceptions import ConfigurationError


def test_load_config_success():
    """Test successful configuration loading."""
    config_content = """
aws:
  profile: test_profile
  region: us-west-2

athena:
  database: test_db
  workgroup: test_wg
  output_location: s3://test-bucket/results/

output:
  format: table

queries:
  - name: query1
    sql: SELECT * FROM table1
  - name: query2
    sql: SELECT * FROM table2
"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write(config_content)
        temp_path = f.name
    
    try:
        config = ConfigurationManager.load_config(temp_path)
        
        assert config.aws.profile == "test_profile"
        assert config.aws.region == "us-west-2"
        assert config.athena.database == "test_db"
        assert config.athena.workgroup == "test_wg"
        assert config.athena.output_location == "s3://test-bucket/results/"
        assert config.output.format == "table"
        assert config.output.file is None
        assert len(config.queries) == 2
        assert config.queries[0].name == "query1"
        assert config.queries[0].sql == "SELECT * FROM table1"
        assert config.queries[1].name == "query2"
        assert config.queries[1].sql == "SELECT * FROM table2"
    finally:
        os.unlink(temp_path)


def test_load_config_with_defaults():
    """Test configuration loading with default values for optional fields."""
    config_content = """
athena:
  database: test_db
  workgroup: test_wg
  output_location: s3://test-bucket/results/

queries:
  - name: query1
    sql: SELECT 1
"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write(config_content)
        temp_path = f.name
    
    try:
        config = ConfigurationManager.load_config(temp_path)
        
        # AWS config should have defaults
        assert config.aws.profile is None
        assert config.aws.region == "us-east-1"  # Default region
        
        # Output config should have defaults
        assert config.output.format == "table"
        assert config.output.file is None
        
        # Required fields should be present
        assert config.athena.database == "test_db"
        assert len(config.queries) == 1
    finally:
        os.unlink(temp_path)


def test_load_config_file_not_found():
    """Test error handling when configuration file is missing."""
    with pytest.raises(ConfigurationError) as exc_info:
        ConfigurationManager.load_config("/nonexistent/path/config.yaml")
    
    assert "Configuration file not found" in str(exc_info.value)
    assert "/nonexistent/path/config.yaml" in str(exc_info.value)


def test_load_config_invalid_yaml_syntax():
    """Test error handling for invalid YAML syntax."""
    config_content = """
aws:
  profile: test
  region: us-east-1
athena:
  database: test_db
  workgroup: [invalid yaml syntax here
"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write(config_content)
        temp_path = f.name
    
    try:
        with pytest.raises(ConfigurationError) as exc_info:
            ConfigurationManager.load_config(temp_path)
        
        assert "Invalid YAML syntax" in str(exc_info.value)
        assert "line" in str(exc_info.value).lower()
    finally:
        os.unlink(temp_path)


def test_load_config_missing_athena_section():
    """Test error handling when athena section is missing."""
    config_content = """
aws:
  region: us-east-1

queries:
  - name: query1
    sql: SELECT 1
"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write(config_content)
        temp_path = f.name
    
    try:
        with pytest.raises(ConfigurationError) as exc_info:
            ConfigurationManager.load_config(temp_path)
        
        assert "athena" in str(exc_info.value).lower()
    finally:
        os.unlink(temp_path)


def test_load_config_missing_database_field():
    """Test error handling when database field is missing."""
    config_content = """
athena:
  workgroup: test_wg
  output_location: s3://test-bucket/results/

queries:
  - name: query1
    sql: SELECT 1
"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write(config_content)
        temp_path = f.name
    
    try:
        with pytest.raises(ConfigurationError) as exc_info:
            ConfigurationManager.load_config(temp_path)
        
        assert "database" in str(exc_info.value).lower()
    finally:
        os.unlink(temp_path)


def test_load_config_missing_workgroup_field():
    """Test error handling when workgroup field is missing."""
    config_content = """
athena:
  database: test_db
  output_location: s3://test-bucket/results/

queries:
  - name: query1
    sql: SELECT 1
"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write(config_content)
        temp_path = f.name
    
    try:
        with pytest.raises(ConfigurationError) as exc_info:
            ConfigurationManager.load_config(temp_path)
        
        assert "workgroup" in str(exc_info.value).lower()
    finally:
        os.unlink(temp_path)


def test_load_config_missing_output_location_field():
    """Test error handling when output_location field is missing."""
    config_content = """
athena:
  database: test_db
  workgroup: test_wg

queries:
  - name: query1
    sql: SELECT 1
"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write(config_content)
        temp_path = f.name
    
    try:
        with pytest.raises(ConfigurationError) as exc_info:
            ConfigurationManager.load_config(temp_path)
        
        assert "output_location" in str(exc_info.value).lower()
    finally:
        os.unlink(temp_path)


def test_load_config_missing_queries_section():
    """Test error handling when queries section is missing."""
    config_content = """
athena:
  database: test_db
  workgroup: test_wg
  output_location: s3://test-bucket/results/
"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write(config_content)
        temp_path = f.name
    
    try:
        with pytest.raises(ConfigurationError) as exc_info:
            ConfigurationManager.load_config(temp_path)
        
        assert "queries" in str(exc_info.value).lower()
    finally:
        os.unlink(temp_path)


def test_load_config_empty_queries_list():
    """Test error handling when queries list is empty."""
    config_content = """
athena:
  database: test_db
  workgroup: test_wg
  output_location: s3://test-bucket/results/

queries: []
"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write(config_content)
        temp_path = f.name
    
    try:
        with pytest.raises(ConfigurationError) as exc_info:
            ConfigurationManager.load_config(temp_path)
        
        assert "at least one query" in str(exc_info.value).lower()
    finally:
        os.unlink(temp_path)


def test_load_config_query_missing_name():
    """Test error handling when query is missing name field."""
    config_content = """
athena:
  database: test_db
  workgroup: test_wg
  output_location: s3://test-bucket/results/

queries:
  - sql: SELECT 1
"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write(config_content)
        temp_path = f.name
    
    try:
        with pytest.raises(ConfigurationError) as exc_info:
            ConfigurationManager.load_config(temp_path)
        
        assert "name" in str(exc_info.value).lower()
    finally:
        os.unlink(temp_path)


def test_load_config_query_missing_sql():
    """Test error handling when query is missing sql field."""
    config_content = """
athena:
  database: test_db
  workgroup: test_wg
  output_location: s3://test-bucket/results/

queries:
  - name: query1
"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write(config_content)
        temp_path = f.name
    
    try:
        with pytest.raises(ConfigurationError) as exc_info:
            ConfigurationManager.load_config(temp_path)
        
        assert "sql" in str(exc_info.value).lower()
    finally:
        os.unlink(temp_path)


def test_load_config_invalid_output_format():
    """Test error handling for invalid output format."""
    config_content = """
athena:
  database: test_db
  workgroup: test_wg
  output_location: s3://test-bucket/results/

output:
  format: invalid_format

queries:
  - name: query1
    sql: SELECT 1
"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write(config_content)
        temp_path = f.name
    
    try:
        with pytest.raises(ConfigurationError) as exc_info:
            ConfigurationManager.load_config(temp_path)
        
        assert "invalid output format" in str(exc_info.value).lower()
        assert "invalid_format" in str(exc_info.value)
    finally:
        os.unlink(temp_path)


def test_load_config_with_csv_output():
    """Test configuration with CSV output format."""
    config_content = """
athena:
  database: test_db
  workgroup: test_wg
  output_location: s3://test-bucket/results/

output:
  format: csv
  file: /tmp/output.csv

queries:
  - name: query1
    sql: SELECT 1
"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write(config_content)
        temp_path = f.name
    
    try:
        config = ConfigurationManager.load_config(temp_path)
        
        assert config.output.format == "csv"
        assert config.output.file == "/tmp/output.csv"
    finally:
        os.unlink(temp_path)


def test_load_config_with_json_output():
    """Test configuration with JSON output format."""
    config_content = """
athena:
  database: test_db
  workgroup: test_wg
  output_location: s3://test-bucket/results/

output:
  format: json
  file: /tmp/output.json

queries:
  - name: query1
    sql: SELECT 1
"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write(config_content)
        temp_path = f.name
    
    try:
        config = ConfigurationManager.load_config(temp_path)
        
        assert config.output.format == "json"
        assert config.output.file == "/tmp/output.json"
    finally:
        os.unlink(temp_path)


# Additional edge case tests for task 2.7

def test_load_config_empty_file():
    """Test error handling for empty configuration file."""
    config_content = ""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write(config_content)
        temp_path = f.name
    
    try:
        with pytest.raises(ConfigurationError) as exc_info:
            ConfigurationManager.load_config(temp_path)
        
        # Empty YAML file results in None, which should fail validation
        assert "yaml object/dictionary" in str(exc_info.value).lower() or "athena" in str(exc_info.value).lower()
    finally:
        os.unlink(temp_path)


def test_load_config_not_a_dictionary():
    """Test error handling when configuration file contains a list instead of dictionary."""
    config_content = """
- item1
- item2
"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write(config_content)
        temp_path = f.name
    
    try:
        with pytest.raises(ConfigurationError) as exc_info:
            ConfigurationManager.load_config(temp_path)
        
        assert "yaml object/dictionary" in str(exc_info.value).lower()
    finally:
        os.unlink(temp_path)


def test_load_config_empty_database():
    """Test error handling when database field is empty string."""
    config_content = """
athena:
  database: ""
  workgroup: test_wg
  output_location: s3://test-bucket/results/

queries:
  - name: query1
    sql: SELECT 1
"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write(config_content)
        temp_path = f.name
    
    try:
        with pytest.raises(ConfigurationError) as exc_info:
            ConfigurationManager.load_config(temp_path)
        
        assert "database" in str(exc_info.value).lower()
        assert "non-empty" in str(exc_info.value).lower()
    finally:
        os.unlink(temp_path)


def test_load_config_empty_workgroup():
    """Test error handling when workgroup field is empty string."""
    config_content = """
athena:
  database: test_db
  workgroup: ""
  output_location: s3://test-bucket/results/

queries:
  - name: query1
    sql: SELECT 1
"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write(config_content)
        temp_path = f.name
    
    try:
        with pytest.raises(ConfigurationError) as exc_info:
            ConfigurationManager.load_config(temp_path)
        
        assert "workgroup" in str(exc_info.value).lower()
        assert "non-empty" in str(exc_info.value).lower()
    finally:
        os.unlink(temp_path)


def test_load_config_empty_output_location():
    """Test error handling when output_location field is empty string."""
    config_content = """
athena:
  database: test_db
  workgroup: test_wg
  output_location: ""

queries:
  - name: query1
    sql: SELECT 1
"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write(config_content)
        temp_path = f.name
    
    try:
        with pytest.raises(ConfigurationError) as exc_info:
            ConfigurationManager.load_config(temp_path)
        
        assert "output_location" in str(exc_info.value).lower()
        assert "non-empty" in str(exc_info.value).lower()
    finally:
        os.unlink(temp_path)


def test_load_config_empty_query_name():
    """Test error handling when query name is empty string."""
    config_content = """
athena:
  database: test_db
  workgroup: test_wg
  output_location: s3://test-bucket/results/

queries:
  - name: ""
    sql: SELECT 1
"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write(config_content)
        temp_path = f.name
    
    try:
        with pytest.raises(ConfigurationError) as exc_info:
            ConfigurationManager.load_config(temp_path)
        
        assert "name" in str(exc_info.value).lower()
        assert "non-empty" in str(exc_info.value).lower()
    finally:
        os.unlink(temp_path)


def test_load_config_empty_query_sql():
    """Test error handling when query SQL is empty string."""
    config_content = """
athena:
  database: test_db
  workgroup: test_wg
  output_location: s3://test-bucket/results/

queries:
  - name: query1
    sql: ""
"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write(config_content)
        temp_path = f.name
    
    try:
        with pytest.raises(ConfigurationError) as exc_info:
            ConfigurationManager.load_config(temp_path)
        
        assert "sql" in str(exc_info.value).lower()
        assert "non-empty" in str(exc_info.value).lower()
    finally:
        os.unlink(temp_path)


def test_load_config_queries_not_list():
    """Test error handling when queries field is not a list."""
    config_content = """
athena:
  database: test_db
  workgroup: test_wg
  output_location: s3://test-bucket/results/

queries: "not a list"
"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write(config_content)
        temp_path = f.name
    
    try:
        with pytest.raises(ConfigurationError) as exc_info:
            ConfigurationManager.load_config(temp_path)
        
        assert "queries" in str(exc_info.value).lower()
        assert "list" in str(exc_info.value).lower()
    finally:
        os.unlink(temp_path)


def test_load_config_query_not_dict():
    """Test error handling when query item is not a dictionary."""
    config_content = """
athena:
  database: test_db
  workgroup: test_wg
  output_location: s3://test-bucket/results/

queries:
  - "not a dictionary"
"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write(config_content)
        temp_path = f.name
    
    try:
        with pytest.raises(ConfigurationError) as exc_info:
            ConfigurationManager.load_config(temp_path)
        
        assert "query at index 0" in str(exc_info.value).lower()
        assert "dictionary" in str(exc_info.value).lower()
    finally:
        os.unlink(temp_path)


def test_load_config_database_not_string():
    """Test error handling when database field is not a string."""
    config_content = """
athena:
  database: 123
  workgroup: test_wg
  output_location: s3://test-bucket/results/

queries:
  - name: query1
    sql: SELECT 1
"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write(config_content)
        temp_path = f.name
    
    try:
        with pytest.raises(ConfigurationError) as exc_info:
            ConfigurationManager.load_config(temp_path)
        
        assert "database" in str(exc_info.value).lower()
        assert "string" in str(exc_info.value).lower()
    finally:
        os.unlink(temp_path)


def test_load_config_workgroup_not_string():
    """Test error handling when workgroup field is not a string."""
    config_content = """
athena:
  database: test_db
  workgroup: 123
  output_location: s3://test-bucket/results/

queries:
  - name: query1
    sql: SELECT 1
"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write(config_content)
        temp_path = f.name
    
    try:
        with pytest.raises(ConfigurationError) as exc_info:
            ConfigurationManager.load_config(temp_path)
        
        assert "workgroup" in str(exc_info.value).lower()
        assert "string" in str(exc_info.value).lower()
    finally:
        os.unlink(temp_path)


def test_load_config_output_location_not_string():
    """Test error handling when output_location field is not a string."""
    config_content = """
athena:
  database: test_db
  workgroup: test_wg
  output_location: 123

queries:
  - name: query1
    sql: SELECT 1
"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write(config_content)
        temp_path = f.name
    
    try:
        with pytest.raises(ConfigurationError) as exc_info:
            ConfigurationManager.load_config(temp_path)
        
        assert "output_location" in str(exc_info.value).lower()
        assert "string" in str(exc_info.value).lower()
    finally:
        os.unlink(temp_path)


def test_load_config_multiple_invalid_formats():
    """Test error handling for various invalid output format values."""
    invalid_formats = ["xml", "html", "pdf", "txt", "INVALID", "Table", "CSV", "JSON"]
    
    for invalid_format in invalid_formats:
        config_content = f"""
athena:
  database: test_db
  workgroup: test_wg
  output_location: s3://test-bucket/results/

output:
  format: {invalid_format}

queries:
  - name: query1
    sql: SELECT 1
"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write(config_content)
            temp_path = f.name
        
        try:
            with pytest.raises(ConfigurationError) as exc_info:
                ConfigurationManager.load_config(temp_path)
            
            assert "invalid output format" in str(exc_info.value).lower()
            assert invalid_format in str(exc_info.value)
        finally:
            os.unlink(temp_path)


def test_query_config_skip_default():
    """Test QueryConfig skip defaults to False."""
    query_config = QueryConfig(name="test", sql="SELECT 1")
    assert query_config.skip is False


def test_query_config_skip_true():
    """Test QueryConfig with skip set to True."""
    query_config = QueryConfig(name="test", sql="SELECT 1", skip=True)
    assert query_config.skip is True


def test_load_config_with_skip():
    """Test configuration loading with skip field on queries."""
    config_content = """
athena:
  database: test_db
  workgroup: test_wg
  output_location: s3://test-bucket/results/

queries:
  - name: query1
    sql: SELECT 1
    skip: true
  - name: query2
    sql: SELECT 2
  - name: query3
    sql: SELECT 3
    skip: false
"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write(config_content)
        temp_path = f.name

    try:
        config = ConfigurationManager.load_config(temp_path)

        assert len(config.queries) == 3
        assert config.queries[0].name == "query1"
        assert config.queries[0].skip is True
        assert config.queries[1].name == "query2"
        assert config.queries[1].skip is False
        assert config.queries[2].name == "query3"
        assert config.queries[2].skip is False
    finally:
        os.unlink(temp_path)


def test_load_config_skip_not_boolean():
    """Test error handling when skip field is not a boolean."""
    config_content = """
athena:
  database: test_db
  workgroup: test_wg
  output_location: s3://test-bucket/results/

queries:
  - name: query1
    sql: SELECT 1
    skip: "yes"
"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write(config_content)
        temp_path = f.name

    try:
        with pytest.raises(ConfigurationError) as exc_info:
            ConfigurationManager.load_config(temp_path)

        assert "skip" in str(exc_info.value).lower()
        assert "boolean" in str(exc_info.value).lower()
    finally:
        os.unlink(temp_path)

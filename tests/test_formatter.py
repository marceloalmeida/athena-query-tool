"""Unit tests for result formatting."""

import pytest
from athena_query_tool.formatter import ResultFormatter
from athena_query_tool.executor import QueryResult, Column


class TestResultFormatter:
    """Test suite for ResultFormatter class."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.formatter = ResultFormatter()
    
    def test_format_simple_table(self):
        """Test formatting a simple table with data."""
        result = QueryResult(
            columns=[
                Column(name="id", type="integer"),
                Column(name="name", type="varchar")
            ],
            rows=[
                [1, "Alice"],
                [2, "Bob"]
            ],
            row_count=2
        )
        
        output = self.formatter.format_as_table(result)
        
        # Verify headers are present
        assert "id" in output
        assert "name" in output
        
        # Verify data is present
        assert "1" in output
        assert "Alice" in output
        assert "2" in output
        assert "Bob" in output
    
    def test_format_table_with_null_values(self):
        """Test formatting table with NULL values."""
        result = QueryResult(
            columns=[
                Column(name="id", type="integer"),
                Column(name="value", type="varchar")
            ],
            rows=[
                [1, "data"],
                [2, None],
                [3, "more"]
            ],
            row_count=3
        )
        
        output = self.formatter.format_as_table(result)
        
        # Verify NULL is displayed for None values
        assert "NULL" in output
        assert "data" in output
        assert "more" in output
    
    def test_format_zero_row_results(self):
        """Test formatting table with zero rows."""
        result = QueryResult(
            columns=[
                Column(name="id", type="integer"),
                Column(name="name", type="varchar")
            ],
            rows=[],
            row_count=0
        )
        
        output = self.formatter.format_as_table(result)
        
        # Verify headers are present
        assert "id" in output
        assert "name" in output
        
        # Verify message about zero rows
        assert "(0 rows returned)" in output
    
    def test_truncate_long_values(self):
        """Test truncation of long column values."""
        long_value = "a" * 100
        result = QueryResult(
            columns=[Column(name="data", type="varchar")],
            rows=[[long_value]],
            row_count=1
        )
        
        output = self.formatter.format_as_table(result, max_width=20)
        
        # Verify value is truncated with ellipsis
        assert "..." in output
        # The full long value should not be present
        assert long_value not in output
    
    def test_format_value_handles_none(self):
        """Test _format_value handles None as NULL."""
        assert self.formatter._format_value(None) == "NULL"
    
    def test_format_value_handles_strings(self):
        """Test _format_value handles string values."""
        assert self.formatter._format_value("test") == "test"
        assert self.formatter._format_value("") == ""
    
    def test_format_value_handles_integers(self):
        """Test _format_value handles integer values."""
        assert self.formatter._format_value(42) == "42"
        assert self.formatter._format_value(0) == "0"
        assert self.formatter._format_value(-10) == "-10"
    
    def test_format_value_handles_floats(self):
        """Test _format_value handles float values."""
        assert self.formatter._format_value(3.14) == "3.14"
        assert self.formatter._format_value(0.0) == "0.0"
    
    def test_format_value_handles_booleans(self):
        """Test _format_value handles boolean values."""
        assert self.formatter._format_value(True) == "True"
        assert self.formatter._format_value(False) == "False"
    
    def test_truncate_value_no_truncation_needed(self):
        """Test _truncate_value when value is within max_width."""
        value = "short"
        assert self.formatter._truncate_value(value, 50) == "short"
    
    def test_truncate_value_exact_max_width(self):
        """Test _truncate_value when value equals max_width."""
        value = "a" * 50
        assert self.formatter._truncate_value(value, 50) == value
    
    def test_truncate_value_exceeds_max_width(self):
        """Test _truncate_value when value exceeds max_width."""
        value = "a" * 100
        truncated = self.formatter._truncate_value(value, 20)
        
        assert len(truncated) == 20
        assert truncated.endswith("...")
        assert truncated == "a" * 17 + "..."
    
    def test_format_table_with_various_data_types(self):
        """Test formatting table with various data types."""
        result = QueryResult(
            columns=[
                Column(name="int_col", type="integer"),
                Column(name="float_col", type="double"),
                Column(name="str_col", type="varchar"),
                Column(name="bool_col", type="boolean"),
                Column(name="null_col", type="varchar")
            ],
            rows=[
                [42, 3.14, "text", True, None],
                [-10, 0.0, "", False, None]
            ],
            row_count=2
        )
        
        output = self.formatter.format_as_table(result)
        
        # Verify all data types are formatted correctly
        assert "42" in output
        assert "3.14" in output
        assert "text" in output
        assert "True" in output
        assert "False" in output
        assert "NULL" in output
    
    def test_format_table_with_special_characters(self):
        """Test formatting table with special characters."""
        result = QueryResult(
            columns=[Column(name="data", type="varchar")],
            rows=[
                ["line1\nline2"],
                ["tab\there"],
                ["quote\"test"]
            ],
            row_count=3
        )
        
        output = self.formatter.format_as_table(result)
        
        # Verify special characters are preserved
        assert "line1" in output
        assert "tab" in output
        assert "quote" in output


class TestCSVOutput:
    """Test suite for CSV file output."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.formatter = ResultFormatter()
    
    def test_write_to_csv_basic(self, tmp_path):
        """Test writing basic query results to CSV file."""
        result = QueryResult(
            columns=[
                Column(name="id", type="integer"),
                Column(name="name", type="varchar")
            ],
            rows=[
                [1, "Alice"],
                [2, "Bob"]
            ],
            row_count=2
        )
        
        csv_file = tmp_path / "output.csv"
        self.formatter.write_to_csv(result, str(csv_file))
        
        # Verify file was created
        assert csv_file.exists()
        
        # Read and verify content
        content = csv_file.read_text(encoding='utf-8')
        lines = content.strip().split('\n')
        
        # Verify headers
        assert lines[0] == "id,name"
        
        # Verify data rows
        assert lines[1] == "1,Alice"
        assert lines[2] == "2,Bob"
    
    def test_write_to_csv_with_null_values(self, tmp_path):
        """Test CSV output with NULL values represented as empty strings."""
        result = QueryResult(
            columns=[
                Column(name="id", type="integer"),
                Column(name="value", type="varchar")
            ],
            rows=[
                [1, "data"],
                [2, None],
                [3, "more"]
            ],
            row_count=3
        )
        
        csv_file = tmp_path / "output.csv"
        self.formatter.write_to_csv(result, str(csv_file))
        
        # Read and verify content
        content = csv_file.read_text(encoding='utf-8')
        lines = content.strip().split('\n')
        
        # Verify NULL is represented as empty string
        assert lines[0] == "id,value"
        assert lines[1] == "1,data"
        assert lines[2] == "2,"  # NULL as empty string
        assert lines[3] == "3,more"
    
    def test_write_to_csv_with_special_characters(self, tmp_path):
        """Test CSV output properly escapes special characters."""
        result = QueryResult(
            columns=[Column(name="data", type="varchar")],
            rows=[
                ["value with, comma"],
                ['value with "quotes"'],
                ["value with\nnewline"]
            ],
            row_count=3
        )
        
        csv_file = tmp_path / "output.csv"
        self.formatter.write_to_csv(result, str(csv_file))
        
        # Read using csv module to verify proper escaping
        import csv
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            rows = list(reader)
        
        # Verify headers
        assert rows[0] == ["data"]
        
        # Verify values are properly escaped and preserved
        assert rows[1] == ["value with, comma"]
        assert rows[2] == ['value with "quotes"']
        assert rows[3] == ["value with\nnewline"]
    
    def test_write_to_csv_utf8_encoding(self, tmp_path):
        """Test CSV output uses UTF-8 encoding."""
        result = QueryResult(
            columns=[Column(name="text", type="varchar")],
            rows=[
                ["Hello 世界"],
                ["Привет мир"],
                ["مرحبا العالم"]
            ],
            row_count=3
        )
        
        csv_file = tmp_path / "output.csv"
        self.formatter.write_to_csv(result, str(csv_file))
        
        # Read and verify UTF-8 content
        content = csv_file.read_text(encoding='utf-8')
        assert "世界" in content
        assert "Привет" in content
        assert "مرحبا" in content
    
    def test_write_to_csv_zero_rows(self, tmp_path):
        """Test CSV output with zero rows (headers only)."""
        result = QueryResult(
            columns=[
                Column(name="id", type="integer"),
                Column(name="name", type="varchar")
            ],
            rows=[],
            row_count=0
        )
        
        csv_file = tmp_path / "output.csv"
        self.formatter.write_to_csv(result, str(csv_file))
        
        # Read and verify content
        content = csv_file.read_text(encoding='utf-8')
        lines = content.strip().split('\n')
        
        # Should only have headers
        assert len(lines) == 1
        assert lines[0] == "id,name"
    
    def test_write_to_csv_file_error(self, tmp_path):
        """Test CSV output handles file write errors."""
        from athena_query_tool.exceptions import FileOutputError
        
        result = QueryResult(
            columns=[Column(name="data", type="varchar")],
            rows=[["test"]],
            row_count=1
        )
        
        # Try to write to an invalid path
        invalid_path = "/invalid/path/that/does/not/exist/output.csv"
        
        with pytest.raises(FileOutputError) as exc_info:
            self.formatter.write_to_csv(result, invalid_path)
        
        # Verify error message is descriptive
        assert "Failed to write CSV file" in str(exc_info.value)
        assert invalid_path in str(exc_info.value)


class TestJSONOutput:
    """Test suite for JSON file output."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.formatter = ResultFormatter()
    
    def test_write_to_json_basic(self, tmp_path):
        """Test writing basic query results to JSON file."""
        result = QueryResult(
            columns=[
                Column(name="id", type="integer"),
                Column(name="name", type="varchar")
            ],
            rows=[
                [1, "Alice"],
                [2, "Bob"]
            ],
            row_count=2
        )
        
        json_file = tmp_path / "output.json"
        self.formatter.write_to_json(result, str(json_file))
        
        # Verify file was created
        assert json_file.exists()
        
        # Read and verify content
        import json
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Verify structure
        assert "columns" in data
        assert "rows" in data
        assert "row_count" in data
        
        # Verify columns
        assert len(data["columns"]) == 2
        assert data["columns"][0] == {"name": "id", "type": "integer"}
        assert data["columns"][1] == {"name": "name", "type": "varchar"}
        
        # Verify rows
        assert len(data["rows"]) == 2
        assert data["rows"][0] == {"id": 1, "name": "Alice"}
        assert data["rows"][1] == {"id": 2, "name": "Bob"}
        
        # Verify row count
        assert data["row_count"] == 2
    
    def test_write_to_json_with_null_values(self, tmp_path):
        """Test JSON output with NULL values represented as JSON null."""
        result = QueryResult(
            columns=[
                Column(name="id", type="integer"),
                Column(name="value", type="varchar")
            ],
            rows=[
                [1, "data"],
                [2, None],
                [3, "more"]
            ],
            row_count=3
        )
        
        json_file = tmp_path / "output.json"
        self.formatter.write_to_json(result, str(json_file))
        
        # Read and verify content
        import json
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Verify NULL is represented as JSON null
        assert data["rows"][0] == {"id": 1, "value": "data"}
        assert data["rows"][1] == {"id": 2, "value": None}
        assert data["rows"][2] == {"id": 3, "value": "more"}
    
    def test_write_to_json_utf8_encoding(self, tmp_path):
        """Test JSON output uses UTF-8 encoding."""
        result = QueryResult(
            columns=[Column(name="text", type="varchar")],
            rows=[
                ["Hello 世界"],
                ["Привет мир"],
                ["مرحبا العالم"]
            ],
            row_count=3
        )
        
        json_file = tmp_path / "output.json"
        self.formatter.write_to_json(result, str(json_file))
        
        # Read and verify UTF-8 content
        import json
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        assert data["rows"][0]["text"] == "Hello 世界"
        assert data["rows"][1]["text"] == "Привет мир"
        assert data["rows"][2]["text"] == "مرحبا العالم"
    
    def test_write_to_json_zero_rows(self, tmp_path):
        """Test JSON output with zero rows."""
        result = QueryResult(
            columns=[
                Column(name="id", type="integer"),
                Column(name="name", type="varchar")
            ],
            rows=[],
            row_count=0
        )
        
        json_file = tmp_path / "output.json"
        self.formatter.write_to_json(result, str(json_file))
        
        # Read and verify content
        import json
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Should have columns but empty rows
        assert len(data["columns"]) == 2
        assert len(data["rows"]) == 0
        assert data["row_count"] == 0
    
    def test_write_to_json_proper_indentation(self, tmp_path):
        """Test JSON output uses proper indentation for readability."""
        result = QueryResult(
            columns=[Column(name="id", type="integer")],
            rows=[[1]],
            row_count=1
        )
        
        json_file = tmp_path / "output.json"
        self.formatter.write_to_json(result, str(json_file))
        
        # Read raw content to verify indentation
        content = json_file.read_text(encoding='utf-8')
        
        # Verify it's formatted with indentation (not minified)
        assert "\n" in content
        assert "  " in content  # Should have indentation spaces
    
    def test_write_to_json_various_data_types(self, tmp_path):
        """Test JSON output with various data types."""
        result = QueryResult(
            columns=[
                Column(name="int_col", type="integer"),
                Column(name="float_col", type="double"),
                Column(name="str_col", type="varchar"),
                Column(name="bool_col", type="boolean"),
                Column(name="null_col", type="varchar")
            ],
            rows=[
                [42, 3.14, "text", True, None],
                [-10, 0.0, "", False, None]
            ],
            row_count=2
        )
        
        json_file = tmp_path / "output.json"
        self.formatter.write_to_json(result, str(json_file))
        
        # Read and verify content
        import json
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Verify first row
        assert data["rows"][0]["int_col"] == 42
        assert data["rows"][0]["float_col"] == 3.14
        assert data["rows"][0]["str_col"] == "text"
        assert data["rows"][0]["bool_col"] is True
        assert data["rows"][0]["null_col"] is None
        
        # Verify second row
        assert data["rows"][1]["int_col"] == -10
        assert data["rows"][1]["float_col"] == 0.0
        assert data["rows"][1]["str_col"] == ""
        assert data["rows"][1]["bool_col"] is False
        assert data["rows"][1]["null_col"] is None
    
    def test_write_to_json_file_error(self, tmp_path):
        """Test JSON output handles file write errors."""
        from athena_query_tool.exceptions import FileOutputError
        
        result = QueryResult(
            columns=[Column(name="data", type="varchar")],
            rows=[["test"]],
            row_count=1
        )
        
        # Try to write to an invalid path
        invalid_path = "/invalid/path/that/does/not/exist/output.json"
        
        with pytest.raises(FileOutputError) as exc_info:
            self.formatter.write_to_json(result, invalid_path)
        
        # Verify error message is descriptive
        assert "Failed to write JSON file" in str(exc_info.value)
        assert invalid_path in str(exc_info.value)


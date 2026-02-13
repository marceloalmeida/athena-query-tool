"""Result formatting for query output."""

import csv
import json
from typing import Any
from tabulate import tabulate

from .executor import QueryResult
from .exceptions import FileOutputError


class ResultFormatter:
    """Formats query results for display or file output."""
    
    def format_as_table(self, result: QueryResult, max_width: int = 50) -> str:
        """
        Format query result as ASCII table.
        
        Args:
            result: QueryResult with columns and rows
            max_width: Maximum width for column values before truncation
            
        Returns:
            Formatted ASCII table string
        """
        # Extract column headers
        headers = [col.name for col in result.columns]
        
        # Handle zero-row results
        if result.row_count == 0:
            # Create table with headers only
            table = tabulate([], headers=headers, tablefmt='grid')
            return f"{table}\n\n(0 rows returned)"
        
        # Format and truncate values
        formatted_rows = []
        for row in result.rows:
            formatted_row = [
                self._truncate_value(self._format_value(value), max_width)
                for value in row
            ]
            formatted_rows.append(formatted_row)
        
        # Generate table using tabulate
        return tabulate(formatted_rows, headers=headers, tablefmt='grid')
    def write_to_csv(self, result: QueryResult, file_path: str) -> None:
        """
        Write query result to CSV file.

        Args:
            result: QueryResult with columns and rows
            file_path: Path to output CSV file

        Raises:
            FileOutputError: If file cannot be written
        """
        try:
            with open(file_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)

                # Write column headers in first row
                headers = [col.name for col in result.columns]
                writer.writerow(headers)

                # Write data rows
                for row in result.rows:
                    # Convert None (NULL) values to empty strings
                    csv_row = ['' if value is None else value for value in row]
                    writer.writerow(csv_row)

        except (IOError, OSError) as e:
            raise FileOutputError(f"Failed to write CSV file '{file_path}': {str(e)}")
    
    def write_to_json(self, result: QueryResult, file_path: str) -> None:
        """
        Write query result to JSON file.

        Args:
            result: QueryResult with columns and rows
            file_path: Path to output JSON file

        Raises:
            FileOutputError: If file cannot be written
        """
        try:
            # Build JSON structure
            json_data = {
                "columns": [
                    {"name": col.name, "type": col.type}
                    for col in result.columns
                ],
                "rows": [
                    {col.name: row[i] for i, col in enumerate(result.columns)}
                    for row in result.rows
                ],
                "row_count": result.row_count
            }

            # Write to file with proper indentation
            with open(file_path, 'w', encoding='utf-8') as jsonfile:
                json.dump(json_data, jsonfile, indent=2, ensure_ascii=False)

        except (IOError, OSError) as e:
            raise FileOutputError(f"Failed to write JSON file '{file_path}': {str(e)}")
    
    def _format_value(self, value: Any) -> str:
        """
        Format value for display (handle NULL, types).
        
        Args:
            value: Value to format
            
        Returns:
            Formatted string representation
        """
        # Handle NULL values (None in Python)
        if value is None:
            return "NULL"
        
        # Handle boolean values
        if isinstance(value, bool):
            return str(value)
        
        # Handle numeric values
        if isinstance(value, (int, float)):
            return str(value)
        
        # Handle string values (already strings from Athena)
        return str(value)
    
    def _truncate_value(self, value: str, max_width: int) -> str:
        """
        Truncate value if longer than max_width.
        
        Args:
            value: String value to truncate
            max_width: Maximum width before truncation
            
        Returns:
            Truncated string with ellipsis if needed
        """
        if len(value) <= max_width:
            return value
        
        # Truncate and add ellipsis
        return value[:max_width - 3] + "..."

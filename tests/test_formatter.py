"""Tests for mysqlpg.formatter — output formatting (table, batch, vertical)."""

import pytest
from mysqlpg.formatter import Formatter


class TestTableFormat:
    """Test MySQL-style bordered table output."""

    def test_basic_table(self):
        f = Formatter(table_mode=True)
        result = f.format_results(["id", "name"], [(1, "Alice"), (2, "Bob")])
        assert "+----+-------+" in result
        assert "| id | name  |" in result
        assert "| 1  | Alice |" in result
        assert "| 2  | Bob   |" in result
        assert "2 rows in set" in result

    def test_single_row(self):
        f = Formatter(table_mode=True)
        result = f.format_results(["id"], [(1,)])
        assert "1 row in set" in result

    def test_empty_set(self):
        f = Formatter(table_mode=True)
        result = f.format_results(["id", "name"], [])
        assert "Empty set" in result

    def test_null_value(self):
        f = Formatter(table_mode=True)
        result = f.format_results(["name"], [(None,)])
        assert "NULL" in result

    def test_boolean_value(self):
        f = Formatter(table_mode=True)
        result = f.format_results(["active"], [(True,), (False,)])
        assert "| 1 " in result or "| 1      |" in result
        assert "| 0 " in result or "| 0      |" in result

    def test_bytes_value(self):
        f = Formatter(table_mode=True)
        result = f.format_results(["data"], [(b"\xde\xad",)])
        assert "dead" in result

    def test_wide_columns(self):
        f = Formatter(table_mode=True)
        result = f.format_results(
            ["very_long_column_name"],
            [("short",)]
        )
        assert "very_long_column_name" in result

    def test_no_columns(self):
        f = Formatter(table_mode=True)
        result = f.format_results(None, None)
        assert result == "" or result is None

    def test_elapsed_time(self):
        f = Formatter(table_mode=True)
        result = f.format_results(["x"], [(1,)], elapsed=0.123)
        assert "0.12 sec" in result

    def test_silent_mode(self):
        f = Formatter(table_mode=True, silent=True)
        result = f.format_results(["id"], [(1,)])
        assert "row" not in result.lower()


class TestBatchFormat:
    """Test tab-separated batch output."""

    def test_basic_batch(self):
        f = Formatter(batch=True)
        result = f.format_results(["id", "name"], [(1, "Alice"), (2, "Bob")])
        lines = result.strip().split("\n")
        assert lines[0] == "id\tname"
        assert lines[1] == "1\tAlice"
        assert lines[2] == "2\tBob"

    def test_skip_column_names(self):
        f = Formatter(batch=True, skip_column_names=True)
        result = f.format_results(["id", "name"], [(1, "Alice")])
        lines = result.strip().split("\n")
        assert lines[0] == "1\tAlice"
        assert len(lines) == 1

    def test_null_in_batch(self):
        f = Formatter(batch=True)
        result = f.format_results(["name"], [(None,)])
        assert "NULL" in result

    def test_empty_batch(self):
        f = Formatter(batch=True)
        result = f.format_results(["id"], [])
        # Just header, no data
        assert "id" in result


class TestVerticalFormat:
    """Test \\G vertical output format."""

    def test_basic_vertical(self):
        f = Formatter(vertical=True)
        result = f.format_results(
            ["id", "name"], [(1, "Alice")]
        )
        assert "1. row" in result
        assert "id: 1" in result
        assert "name: Alice" in result

    def test_multiple_rows(self):
        f = Formatter(vertical=True)
        result = f.format_results(
            ["id"], [(1,), (2,)]
        )
        assert "1. row" in result
        assert "2. row" in result
        assert "2 rows in set" in result

    def test_vertical_override(self):
        f = Formatter()  # Not vertical by default
        result = f.format_results(
            ["id", "name"], [(1, "Alice")], vertical_override=True
        )
        assert "1. row" in result

    def test_null_in_vertical(self):
        f = Formatter(vertical=True)
        result = f.format_results(["name"], [(None,)])
        assert "NULL" in result

    def test_empty_vertical(self):
        f = Formatter(vertical=True)
        result = f.format_results(["id"], [])
        assert "Empty set" in result

    def test_column_alignment(self):
        f = Formatter(vertical=True)
        result = f.format_results(
            ["id", "long_column_name"], [(1, "val")]
        )
        # Column names should be right-aligned
        lines = result.split("\n")
        id_line = [l for l in lines if "id:" in l][0]
        name_line = [l for l in lines if "long_column_name:" in l][0]
        # Both colons should be at the same position
        assert id_line.index(":") == name_line.index(":")


class TestStatusFormat:
    """Test status line formatting."""

    def test_rows_affected(self):
        f = Formatter()
        result = f.format_status("INSERT 0 1", 1, 0.01)
        assert "Query OK, 1 rows affected" in result

    def test_zero_rows(self):
        f = Formatter()
        result = f.format_status("DELETE 0", 0, 0.01)
        assert "Query OK, 0 rows affected" in result

    def test_silent_status(self):
        f = Formatter(silent=True)
        result = f.format_status("OK", 1, 0.01)
        assert result == ""


class TestCellFormatting:
    """Test individual cell value formatting."""

    def test_none_is_null(self):
        f = Formatter()
        assert f._format_cell(None) == "NULL"

    def test_true_is_one(self):
        f = Formatter()
        assert f._format_cell(True) == "1"

    def test_false_is_zero(self):
        f = Formatter()
        assert f._format_cell(False) == "0"

    def test_bytes_hex(self):
        f = Formatter()
        assert f._format_cell(b"\xca\xfe") == "cafe"

    def test_memoryview_hex(self):
        f = Formatter()
        assert f._format_cell(memoryview(b"\xbe\xef")) == "beef"

    def test_int_string(self):
        f = Formatter()
        assert f._format_cell(42) == "42"

    def test_string_passthrough(self):
        f = Formatter()
        assert f._format_cell("hello") == "hello"


class TestTeeOutput:
    """Test tee (output logging) functionality."""

    def test_start_stop_tee(self, tmp_path):
        f = Formatter()
        tee_file = tmp_path / "output.log"
        f.start_tee(str(tee_file))
        f.print_message("test message")
        f.stop_tee()
        assert "test message" in tee_file.read_text()

    def test_tee_closed_on_close(self, tmp_path):
        f = Formatter(tee_file=str(tmp_path / "log.txt"))
        f.close()
        assert f.tee_fp is None


class TestPagerOutput:
    """Test pager functionality."""

    def test_set_clear_pager(self):
        f = Formatter()
        f.set_pager("less")
        assert f.pager_cmd == "less"
        f.clear_pager()
        assert f.pager_cmd is None

"""Tests for mysqlpg.dumpcli — mysqldump-compatible dump tool."""

import io
import pytest
from unittest.mock import patch, MagicMock
from mysqlpg.dumpcli import (
    build_parser, parse_password_arg, DumpOptions, Dumper,
)


class TestDumpPasswordParsing:

    def test_inline_password(self):
        assert parse_password_arg(["-psecret"]) == ["--password", "secret"]

    def test_flag_only(self):
        assert parse_password_arg(["-p"]) == ["-p"]

    def test_long_form(self):
        assert parse_password_arg(["--password", "x"]) == ["--password", "x"]


class TestDumpParser:

    def test_positional_db(self):
        parser = build_parser()
        args = parser.parse_args(["mydb"])
        assert args.positional == ["mydb"]

    def test_positional_db_and_tables(self):
        parser = build_parser()
        args = parser.parse_args(["mydb", "users", "posts"])
        assert args.positional == ["mydb", "users", "posts"]

    def test_all_databases(self):
        parser = build_parser()
        args = parser.parse_args(["--all-databases"])
        assert args.all_databases is True

    def test_databases_flag(self):
        parser = build_parser()
        args = parser.parse_args(["--databases", "db1", "db2"])
        assert args.databases is True
        assert args.positional == ["db1", "db2"]

    def test_no_data(self):
        parser = build_parser()
        args = parser.parse_args(["--no-data", "mydb"])
        assert args.no_data is True

    def test_no_create_info(self):
        parser = build_parser()
        args = parser.parse_args(["-t", "mydb"])
        assert args.no_create_info is True

    def test_complete_insert(self):
        parser = build_parser()
        args = parser.parse_args(["--complete-insert", "mydb"])
        assert args.complete_insert is True

    def test_single_transaction(self):
        parser = build_parser()
        args = parser.parse_args(["--single-transaction", "mydb"])
        assert args.single_transaction is True

    def test_compact(self):
        parser = build_parser()
        args = parser.parse_args(["--compact", "mydb"])
        assert args.compact is True

    def test_skip_opt(self):
        parser = build_parser()
        args = parser.parse_args(["--skip-opt", "mydb"])
        assert args.skip_opt is True

    def test_where(self):
        parser = build_parser()
        args = parser.parse_args(["--where", "id > 10", "mydb"])
        assert args.where == "id > 10"

    def test_ignore_table(self):
        parser = build_parser()
        args = parser.parse_args(["--ignore-table=mydb.logs", "mydb"])
        assert "mydb.logs" in args.ignore_table

    def test_routines(self):
        parser = build_parser()
        args = parser.parse_args(["--routines", "mydb"])
        assert args.routines is True

    def test_insert_ignore(self):
        parser = build_parser()
        args = parser.parse_args(["--insert-ignore", "mydb"])
        assert args.insert_ignore is True

    def test_replace(self):
        parser = build_parser()
        args = parser.parse_args(["--replace", "mydb"])
        assert args.replace is True

    def test_hex_blob(self):
        parser = build_parser()
        args = parser.parse_args(["--hex-blob", "mydb"])
        assert args.hex_blob is True

    def test_compatible_flag(self):
        parser = build_parser()
        args = parser.parse_args(["--compatible", "pgloader", "mydb"])
        assert args.compatible == "pgloader"

    def test_result_file(self):
        parser = build_parser()
        args = parser.parse_args(["--result-file", "/tmp/out.sql", "mydb"])
        assert args.result_file == "/tmp/out.sql"


class TestDumpOptions:

    def _make_args(self, **overrides):
        parser = build_parser()
        argv = overrides.pop("_argv", ["testdb"])
        args = parser.parse_args(argv)
        for k, v in overrides.items():
            setattr(args, k, v)
        return args

    def test_opt_defaults(self):
        opts = DumpOptions(self._make_args())
        assert opts.add_drop_table is True
        assert opts.extended_insert is True
        assert opts.add_locks is True
        assert opts.set_charset is True
        assert opts.lock_tables is True
        assert opts.quick is True
        assert opts.disable_keys is True
        assert opts.triggers is True
        assert opts.comments is True

    def test_skip_opt(self):
        opts = DumpOptions(self._make_args(_argv=["--skip-opt", "testdb"]))
        assert opts.add_drop_table is False
        assert opts.extended_insert is False
        assert opts.add_locks is False

    def test_compact_disables(self):
        opts = DumpOptions(self._make_args(_argv=["--compact", "testdb"]))
        assert opts.comments is False
        assert opts.add_locks is False
        assert opts.set_charset is False
        assert opts.disable_keys is False

    def test_skip_extended_insert(self):
        opts = DumpOptions(self._make_args(
            _argv=["--skip-extended-insert", "testdb"]
        ))
        assert opts.extended_insert is False

    def test_skip_triggers(self):
        opts = DumpOptions(self._make_args(_argv=["--skip-triggers", "testdb"]))
        assert opts.triggers is False

    def test_skip_comments(self):
        opts = DumpOptions(self._make_args(_argv=["--skip-comments", "testdb"]))
        assert opts.comments is False

    def test_compatible_option(self):
        opts = DumpOptions(self._make_args(
            _argv=["--compatible", "pgloader", "testdb"]
        ))
        assert opts.compatible == "pgloader"


class TestDumperOutput:
    """Test Dumper output formatting with mock connection."""

    def _make_dumper(self, opts_overrides=None, conn_overrides=None):
        conn = MagicMock()
        conn.database = "testdb"
        conn.host = "localhost"
        conn.port = 5432
        conn.get_server_version_string.return_value = "16.2"
        conn.get_databases.return_value = ["testdb"]
        conn.get_tables.return_value = ["users"]
        conn.execute.return_value = (None, None, "OK", 0, 0.001)
        if conn_overrides:
            for k, v in conn_overrides.items():
                setattr(conn, k, v)

        out = io.StringIO()

        parser = build_parser()
        args = parser.parse_args(["testdb"])
        if opts_overrides:
            for k, v in opts_overrides.items():
                setattr(args, k, v)
        opts = DumpOptions(args)

        return Dumper(conn, out, opts, args), out, conn

    def test_header_contains_version(self):
        dumper, out, _ = self._make_dumper()
        dumper._emit_header()
        output = out.getvalue()
        assert "mysqldumppg" in output
        assert "FOREIGN_KEY_CHECKS" in output

    def test_header_contains_charset(self):
        dumper, out, _ = self._make_dumper()
        dumper._emit_header()
        output = out.getvalue()
        assert "SET NAMES" in output

    def test_compact_header(self):
        dumper, out, _ = self._make_dumper(opts_overrides={"compact": True})
        dumper._emit_header()
        output = out.getvalue()
        assert "mysqldumppg" not in output  # no comments
        assert "SET NAMES" not in output  # no charset

    def test_footer_contains_date(self):
        dumper, out, _ = self._make_dumper()
        dumper._emit_footer()
        output = out.getvalue()
        assert "Dump completed" in output

    def test_database_header(self):
        dumper, out, _ = self._make_dumper()
        dumper._emit_database_header("mydb")
        output = out.getvalue()
        assert "Current Database" in output
        assert "CREATE DATABASE" in output
        assert "USE" in output

    def test_database_header_no_create(self):
        dumper, out, _ = self._make_dumper(opts_overrides={"no_create_db": True})
        dumper._emit_database_header("mydb")
        output = out.getvalue()
        assert "CREATE DATABASE" not in output

    def test_database_header_with_drop(self):
        dumper, out, _ = self._make_dumper(
            opts_overrides={"add_drop_database": True}
        )
        dumper._emit_database_header("mydb")
        output = out.getvalue()
        assert "DROP DATABASE" in output


class TestDumperValueFormatting:
    """Test value serialization for INSERT statements."""

    def _make_dumper(self):
        from mysqlpg.dumpcli import Dumper, DumpOptions, build_parser
        conn = MagicMock()
        conn.database = "testdb"
        conn.host = "localhost"
        out = io.StringIO()
        args = build_parser().parse_args(["testdb"])
        opts = DumpOptions(args)
        return Dumper(conn, out, opts, args)

    def test_null(self):
        d = self._make_dumper()
        assert d._format_value(None) == "NULL"

    def test_boolean_true(self):
        d = self._make_dumper()
        assert d._format_value(True) == "1"

    def test_boolean_false(self):
        d = self._make_dumper()
        assert d._format_value(False) == "0"

    def test_integer(self):
        d = self._make_dumper()
        assert d._format_value(42) == "42"

    def test_float(self):
        d = self._make_dumper()
        assert d._format_value(3.14) == "3.14"

    def test_string(self):
        d = self._make_dumper()
        assert d._format_value("hello") == "'hello'"

    def test_string_with_quotes(self):
        d = self._make_dumper()
        result = d._format_value("it's")
        assert "''" in result  # escaped single quote

    def test_string_with_backslash(self):
        d = self._make_dumper()
        result = d._format_value("a\\b")
        assert "\\\\" in result

    def test_string_with_newline(self):
        d = self._make_dumper()
        result = d._format_value("line1\nline2")
        assert "\\n" in result

    def test_datetime(self):
        import datetime
        d = self._make_dumper()
        dt = datetime.datetime(2024, 1, 15, 10, 30, 0)
        result = d._format_value(dt)
        assert result == "'2024-01-15 10:30:00'"

    def test_date(self):
        import datetime
        d = self._make_dumper()
        result = d._format_value(datetime.date(2024, 1, 15))
        assert result == "'2024-01-15'"

    def test_time(self):
        import datetime
        d = self._make_dumper()
        result = d._format_value(datetime.time(10, 30, 0))
        assert result == "'10:30:00'"

    def test_timedelta(self):
        import datetime
        d = self._make_dumper()
        result = d._format_value(datetime.timedelta(hours=1, minutes=30))
        assert result == "'01:30:00'"

    def test_bytes(self):
        d = self._make_dumper()
        result = d._format_value(b"\xca\xfe")
        assert "cafe" in result.lower()

    def test_bytes_hex_blob(self):
        from mysqlpg.dumpcli import Dumper, DumpOptions, build_parser
        conn = MagicMock()
        conn.database = "testdb"
        conn.host = "localhost"
        out = io.StringIO()
        args = build_parser().parse_args(["--hex-blob", "testdb"])
        opts = DumpOptions(args)
        d = Dumper(conn, out, opts, args)
        result = d._format_value(b"\xca\xfe")
        assert result == "0xcafe"


class TestDumperEscapeString:

    def _make_dumper(self):
        from mysqlpg.dumpcli import Dumper, DumpOptions, build_parser
        conn = MagicMock()
        conn.database = "testdb"
        conn.host = "localhost"
        out = io.StringIO()
        args = build_parser().parse_args(["testdb"])
        opts = DumpOptions(args)
        return Dumper(conn, out, opts, args)

    def test_single_quote(self):
        d = self._make_dumper()
        assert d._escape_string("it's") == "it''s"

    def test_backslash(self):
        d = self._make_dumper()
        assert d._escape_string("a\\b") == "a\\\\b"

    def test_newline(self):
        d = self._make_dumper()
        assert d._escape_string("a\nb") == "a\\nb"

    def test_tab(self):
        d = self._make_dumper()
        assert d._escape_string("a\tb") == "a\\tb"

    def test_carriage_return(self):
        d = self._make_dumper()
        assert d._escape_string("a\rb") == "a\\rb"

    def test_null_byte(self):
        d = self._make_dumper()
        assert d._escape_string("a\x00b") == "a\\0b"

    def test_ctrl_z(self):
        d = self._make_dumper()
        assert d._escape_string("a\x1ab") == "a\\Zb"

    def test_combined(self):
        d = self._make_dumper()
        result = d._escape_string("it's a\nnew line\\")
        assert "''" in result
        assert "\\n" in result
        assert "\\\\" in result


class TestDumperTableSorting:

    def test_sort_preserves_order_no_deps(self):
        conn = MagicMock()
        conn.database = "testdb"
        conn.host = "localhost"
        conn.execute.return_value = (None, [], "OK", 0, 0.001)
        out = io.StringIO()
        parser = build_parser()
        args = parser.parse_args(["testdb"])
        opts = DumpOptions(args)
        d = Dumper(conn, out, opts, args)

        result = d._sort_tables_by_deps(["a", "b", "c"])
        assert result == ["a", "b", "c"]

    def test_sort_respects_deps(self):
        conn = MagicMock()
        conn.database = "testdb"
        conn.host = "localhost"
        # posts depends on users
        conn.execute.return_value = (
            None,
            [("posts", "users")],
            "OK", 1, 0.001
        )
        out = io.StringIO()
        parser = build_parser()
        args = parser.parse_args(["testdb"])
        opts = DumpOptions(args)
        d = Dumper(conn, out, opts, args)

        result = d._sort_tables_by_deps(["posts", "users"])
        assert result.index("users") < result.index("posts")

    def test_sort_handles_empty(self):
        conn = MagicMock()
        conn.database = "testdb"
        conn.host = "localhost"
        out = io.StringIO()
        parser = build_parser()
        args = parser.parse_args(["testdb"])
        opts = DumpOptions(args)
        d = Dumper(conn, out, opts, args)

        assert d._sort_tables_by_deps([]) == []

    def test_sort_handles_circular_deps(self):
        conn = MagicMock()
        conn.database = "testdb"
        conn.host = "localhost"
        conn.execute.return_value = (
            None,
            [("a", "b"), ("b", "a")],
            "OK", 2, 0.001
        )
        out = io.StringIO()
        parser = build_parser()
        args = parser.parse_args(["testdb"])
        opts = DumpOptions(args)
        d = Dumper(conn, out, opts, args)

        result = d._sort_tables_by_deps(["a", "b"])
        # Both should be present regardless of circular dep
        assert set(result) == {"a", "b"}


class TestDumperInsertWriting:
    """Test INSERT statement generation."""

    def _make_dumper_with_output(self, extended=True, complete=False,
                                  insert_ignore=False, replace=False):
        conn = MagicMock()
        conn.database = "testdb"
        conn.host = "localhost"
        out = io.StringIO()
        parser = build_parser()

        argv = ["testdb"]
        if not extended:
            argv.append("--skip-extended-insert")
        if complete:
            argv.append("--complete-insert")
        if insert_ignore:
            argv.append("--insert-ignore")
        if replace:
            argv.append("--replace")
        args = parser.parse_args(argv)
        opts = DumpOptions(args)
        return Dumper(conn, out, opts, args), out

    def test_extended_insert(self):
        d, out = self._make_dumper_with_output(extended=True)
        d._write_inserts("users", ["id", "name"], [("text",), ("text",)],
                         [(1, "Alice"), (2, "Bob")])
        output = out.getvalue()
        assert "INSERT INTO" in output
        assert output.count("INSERT INTO") == 1  # single multi-row statement
        assert "(1,'Alice')" in output
        assert "(2,'Bob')" in output

    def test_single_row_insert(self):
        d, out = self._make_dumper_with_output(extended=False)
        d._write_inserts("users", ["id", "name"], [("text",), ("text",)],
                         [(1, "Alice"), (2, "Bob")])
        output = out.getvalue()
        assert output.count("INSERT INTO") == 2  # one per row

    def test_complete_insert(self):
        d, out = self._make_dumper_with_output(complete=True)
        d._write_inserts("users", ["id", "name"], [("text",), ("text",)],
                         [(1, "Alice")])
        output = out.getvalue()
        assert "`id`" in output
        assert "`name`" in output

    def test_insert_ignore_prefix(self):
        d, out = self._make_dumper_with_output(insert_ignore=True)
        d._write_inserts("users", ["id", "name"], [("text",), ("text",)],
                         [(1, "Alice")])
        output = out.getvalue()
        assert "INSERT IGNORE INTO" in output

    def test_replace_prefix(self):
        d, out = self._make_dumper_with_output(replace=True)
        d._write_inserts("users", ["id", "name"], [("text",), ("text",)],
                         [(1, "Alice")])
        output = out.getvalue()
        assert "REPLACE INTO" in output

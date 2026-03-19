"""Tests for psql backslash command support."""

import pytest
from unittest.mock import MagicMock
from mysqlpg.commands import handle_command, _handle_psql_command


@pytest.fixture
def formatter():
    f = MagicMock()
    f.print_message = MagicMock()
    f.print_results = MagicMock()
    f.start_tee = MagicMock()
    f.stop_tee = MagicMock()
    f.vertical = False
    return f


@pytest.fixture
def state():
    return {
        "database": "testdb",
        "delimiter": ";",
        "show_warnings": False,
        "force": False,
        "exit": False,
        "rehash": False,
    }


# ---------- \x toggle expanded ----------

class TestExpandedToggle:

    def test_toggle_on(self, mock_conn, formatter, state):
        result = handle_command("\\x", mock_conn, formatter, state)
        assert result is True
        assert formatter.vertical is True
        assert "on" in formatter.print_message.call_args[0][0].lower()

    def test_toggle_off(self, mock_conn, formatter, state):
        formatter.vertical = True
        handle_command("\\x", mock_conn, formatter, state)
        assert formatter.vertical is False
        assert "off" in formatter.print_message.call_args[0][0].lower()

    def test_explicit_on(self, mock_conn, formatter, state):
        handle_command("\\x on", mock_conn, formatter, state)
        assert formatter.vertical is True

    def test_explicit_off(self, mock_conn, formatter, state):
        formatter.vertical = True
        handle_command("\\x off", mock_conn, formatter, state)
        assert formatter.vertical is False


# ---------- \l list databases ----------

class TestListDatabases:

    def test_list_databases(self, mock_conn, formatter, state):
        result = handle_command("\\l", mock_conn, formatter, state)
        assert result is True
        # Should have called execute with a pg_database query
        formatter.print_results.assert_called_once()

    def test_list_databases_plus(self, mock_conn, formatter, state):
        result = handle_command("\\l+", mock_conn, formatter, state)
        assert result is True


# ---------- \dt list tables ----------

class TestListTables:

    def test_list_tables(self, mock_conn, formatter, state):
        result = handle_command("\\dt", mock_conn, formatter, state)
        assert result is True
        formatter.print_results.assert_called_once()

    def test_list_tables_plus(self, mock_conn, formatter, state):
        result = handle_command("\\dt+", mock_conn, formatter, state)
        assert result is True

    def test_list_tables_with_pattern(self, mock_conn, formatter, state):
        result = handle_command("\\dt post*", mock_conn, formatter, state)
        assert result is True

    def test_list_tables_exact(self, mock_conn, formatter, state):
        result = handle_command("\\dt users", mock_conn, formatter, state)
        assert result is True


# ---------- \di list indexes ----------

class TestListIndexes:

    def test_list_indexes(self, mock_conn, formatter, state):
        result = handle_command("\\di", mock_conn, formatter, state)
        assert result is True

    def test_list_indexes_with_pattern(self, mock_conn, formatter, state):
        result = handle_command("\\di idx_posts*", mock_conn, formatter, state)
        assert result is True


# ---------- \d describe ----------

class TestDescribe:

    def test_describe_table(self, mock_conn, formatter, state):
        result = handle_command("\\d users", mock_conn, formatter, state)
        assert result is True

    def test_describe_table_plus(self, mock_conn, formatter, state):
        result = handle_command("\\d+ users", mock_conn, formatter, state)
        assert result is True

    def test_describe_no_args(self, mock_conn, formatter, state):
        result = handle_command("\\d", mock_conn, formatter, state)
        assert result is True

    def test_d_not_caught_by_source(self, mock_conn, formatter, state):
        """\\d should NOT be caught by the \\. (SOURCE) handler."""
        result = handle_command("\\d posts", mock_conn, formatter, state)
        assert result is True
        # Should NOT show "Failed to open file"
        if formatter.print_message.called:
            msg = formatter.print_message.call_args[0][0]
            assert "Failed to open" not in msg

    def test_dt_not_caught_by_source(self, mock_conn, formatter, state):
        """\\dt should NOT be caught by \\. (SOURCE)."""
        result = handle_command("\\dt", mock_conn, formatter, state)
        assert result is True
        if formatter.print_message.called:
            msg = formatter.print_message.call_args[0][0]
            assert "Failed to open" not in msg


# ---------- \dn list schemas ----------

class TestListSchemas:

    def test_list_schemas(self, mock_conn, formatter, state):
        result = handle_command("\\dn", mock_conn, formatter, state)
        assert result is True


# ---------- \du list roles ----------

class TestListRoles:

    def test_list_roles(self, mock_conn, formatter, state):
        result = handle_command("\\du", mock_conn, formatter, state)
        assert result is True


# ---------- \dv list views ----------

class TestListViews:

    def test_list_views(self, mock_conn, formatter, state):
        result = handle_command("\\dv", mock_conn, formatter, state)
        assert result is True


# ---------- \ds list sequences ----------

class TestListSequences:

    def test_list_sequences(self, mock_conn, formatter, state):
        result = handle_command("\\ds", mock_conn, formatter, state)
        assert result is True


# ---------- \df list functions ----------

class TestListFunctions:

    def test_list_functions(self, mock_conn, formatter, state):
        result = handle_command("\\df", mock_conn, formatter, state)
        assert result is True


# ---------- \c connect to database ----------

class TestConnectDatabase:

    def test_connect(self, mock_conn, formatter, state):
        result = handle_command("\\c newdb", mock_conn, formatter, state)
        assert result is True
        assert state["database"] == "newdb"
        assert state["rehash"] is True
        msg = formatter.print_message.call_args[0][0]
        assert "newdb" in msg

    def test_connect_error(self, mock_conn, formatter, state):
        mock_conn.reconnect = MagicMock(side_effect=Exception("connection refused"))
        result = handle_command("\\c baddb", mock_conn, formatter, state)
        assert result is True
        assert "ERROR" in formatter.print_message.call_args[0][0]


# ---------- \conninfo ----------

class TestConnInfo:

    def test_conninfo(self, mock_conn, formatter, state):
        result = handle_command("\\conninfo", mock_conn, formatter, state)
        assert result is True
        msg = formatter.print_message.call_args[0][0]
        assert "testdb" in msg
        assert "postgres" in msg
        assert "localhost" in msg


# ---------- \timing ----------

class TestTiming:

    def test_timing(self, mock_conn, formatter, state):
        result = handle_command("\\timing", mock_conn, formatter, state)
        assert result is True


# ---------- \i include file ----------

class TestIncludeFile:

    def test_include_missing_file(self, mock_conn, formatter, state):
        result = handle_command("\\i /nonexistent/file.sql", mock_conn, formatter, state)
        assert result is True
        assert "ERROR" in formatter.print_message.call_args[0][0]

    def test_include_real_file(self, mock_conn, formatter, state, tmp_path):
        f = tmp_path / "test.sql"
        f.write_text("SELECT 1;\n")
        result = handle_command(f"\\i {f}", mock_conn, formatter, state)
        assert result is True


# ---------- \o output redirect ----------

class TestOutputRedirect:

    def test_output_to_file(self, mock_conn, formatter, state, tmp_path):
        result = handle_command(f"\\o {tmp_path}/out.log", mock_conn, formatter, state)
        assert result is True
        formatter.start_tee.assert_called_once()

    def test_output_to_stdout(self, mock_conn, formatter, state):
        result = handle_command("\\o", mock_conn, formatter, state)
        assert result is True
        formatter.stop_tee.assert_called_once()


# ---------- \copy not supported ----------

class TestCopyNotSupported:

    def test_copy_shows_error(self, mock_conn, formatter, state):
        result = handle_command("\\copy t FROM '/tmp/data.csv' CSV", mock_conn, formatter, state)
        assert result is True
        assert "psql" in formatter.print_message.call_args[0][0]


# ---------- \e not supported ----------

class TestEditNotSupported:

    def test_edit_shows_error(self, mock_conn, formatter, state):
        result = handle_command("\\e", mock_conn, formatter, state)
        assert result is True
        assert "not supported" in formatter.print_message.call_args[0][0]


# ---------- unknown command ----------

class TestUnknownCommand:

    def test_unknown_backslash(self, mock_conn, formatter, state):
        result = handle_command("\\zzz", mock_conn, formatter, state)
        assert result is True
        assert "Invalid command" in formatter.print_message.call_args[0][0]


# ---------- \? psql help ----------

class TestPsqlHelp:

    def test_psql_help_via_handle(self, mock_conn, formatter, state):
        # \? is caught by MySQL HELP handler, but should still show help
        result = handle_command("\\?", mock_conn, formatter, state)
        assert result is True
        msg = formatter.print_message.call_args[0][0]
        # Should contain both mysql and psql commands
        assert "help" in msg.lower() or "HELP" in msg


# ---------- Ensure MySQL commands still work ----------

class TestMySQLCommandsStillWork:

    def test_use(self, mock_conn, formatter, state):
        result = handle_command("USE testdb", mock_conn, formatter, state)
        assert result is True

    def test_status(self, mock_conn, formatter, state):
        result = handle_command("STATUS", mock_conn, formatter, state)
        assert result is True

    def test_source_dot(self, mock_conn, formatter, state, tmp_path):
        f = tmp_path / "test.sql"
        f.write_text("")
        result = handle_command(f"\\. {f}", mock_conn, formatter, state)
        assert result is True

    def test_system(self, mock_conn, formatter, state):
        result = handle_command("SYSTEM echo hello", mock_conn, formatter, state)
        assert result is True

    def test_exit(self, mock_conn, formatter, state):
        result = handle_command("EXIT", mock_conn, formatter, state)
        assert result is True
        assert state["exit"] is True

    def test_pager(self, mock_conn, formatter, state):
        formatter.set_pager = MagicMock()
        result = handle_command("PAGER less", mock_conn, formatter, state)
        assert result is True

    def test_show_not_a_command(self, mock_conn, formatter, state):
        """SHOW TABLES should NOT be handled by commands — it's SQL."""
        result = handle_command("SHOW TABLES", mock_conn, formatter, state)
        assert result is False

    def test_select_not_a_command(self, mock_conn, formatter, state):
        result = handle_command("SELECT 1", mock_conn, formatter, state)
        assert result is False


# ---------- Integration: live PG ----------

from tests.conftest import pg_available


@pg_available
class TestPsqlCommandsLive:

    def test_list_databases(self, live_conn):
        from mysqlpg.formatter import Formatter
        f = Formatter(table_mode=True)
        state = {"database": "testdb"}
        result = handle_command("\\l", live_conn, f, state)
        assert result is True

    def test_list_tables(self, live_conn):
        from mysqlpg.formatter import Formatter
        f = Formatter(table_mode=True)
        state = {"database": "testdb"}
        result = handle_command("\\dt", live_conn, f, state)
        assert result is True

    def test_list_roles(self, live_conn):
        from mysqlpg.formatter import Formatter
        f = Formatter(table_mode=True)
        state = {"database": "testdb"}
        result = handle_command("\\du", live_conn, f, state)
        assert result is True

    def test_conninfo(self, live_conn):
        from mysqlpg.formatter import Formatter
        f = Formatter(table_mode=True)
        state = {"database": live_conn.get_current_database()}
        result = handle_command("\\conninfo", live_conn, f, state)
        assert result is True

    def test_describe_table(self, live_conn):
        """Create a temp table and describe it."""
        from mysqlpg.formatter import Formatter
        f = Formatter(table_mode=True)
        state = {"database": "testdb"}
        try:
            live_conn.execute("DROP TABLE IF EXISTS _psql_test_desc")
            live_conn.execute("CREATE TABLE _psql_test_desc (id serial, name text)")
            result = handle_command("\\d _psql_test_desc", live_conn, f, state)
            assert result is True
        finally:
            live_conn.execute("DROP TABLE IF EXISTS _psql_test_desc")

    def test_list_schemas(self, live_conn):
        from mysqlpg.formatter import Formatter
        f = Formatter(table_mode=True)
        state = {"database": "testdb"}
        result = handle_command("\\dn", live_conn, f, state)
        assert result is True

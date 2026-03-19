"""Tests for mysqlpg.commands — meta-command handling."""

import os
import pytest
from unittest.mock import MagicMock, patch, call
from mysqlpg.commands import handle_command


@pytest.fixture
def formatter():
    f = MagicMock()
    f.print_message = MagicMock()
    f.print_results = MagicMock()
    f.start_tee = MagicMock()
    f.stop_tee = MagicMock()
    f.set_pager = MagicMock()
    f.clear_pager = MagicMock()
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


class TestUseCommand:

    def test_use_database(self, mock_conn, formatter, state):
        result = handle_command("USE newdb", mock_conn, formatter, state)
        assert result is True
        assert state["database"] == "newdb"
        formatter.print_message.assert_called_with("Database changed")

    def test_use_backtick(self, mock_conn, formatter, state):
        result = handle_command("USE `mydb`", mock_conn, formatter, state)
        assert result is True
        assert state["database"] == "mydb"

    def test_use_triggers_rehash(self, mock_conn, formatter, state):
        handle_command("USE newdb", mock_conn, formatter, state)
        assert state["rehash"] is True

    def test_use_error(self, mock_conn, formatter, state):
        mock_conn.reconnect = MagicMock(side_effect=Exception("connection refused"))
        handle_command("USE baddb", mock_conn, formatter, state)
        assert "ERROR" in formatter.print_message.call_args[0][0]


class TestStatusCommand:

    def test_status(self, mock_conn, formatter, state):
        result = handle_command("STATUS", mock_conn, formatter, state)
        assert result is True
        msg = formatter.print_message.call_args[0][0]
        assert "Connection id:" in msg
        assert "Current database:" in msg

    def test_backslash_s(self, mock_conn, formatter, state):
        result = handle_command("\\s", mock_conn, formatter, state)
        assert result is True


class TestSourceCommand:

    def test_source_missing_file(self, mock_conn, formatter, state):
        result = handle_command("SOURCE /nonexistent/file.sql", mock_conn, formatter, state)
        assert result is True
        assert "ERROR" in formatter.print_message.call_args[0][0]

    def test_source_real_file(self, mock_conn, formatter, state, tmp_path):
        sql_file = tmp_path / "test.sql"
        sql_file.write_text("SELECT 1;\n")
        result = handle_command(f"SOURCE {sql_file}", mock_conn, formatter, state)
        assert result is True

    def test_backslash_dot(self, mock_conn, formatter, state, tmp_path):
        sql_file = tmp_path / "test.sql"
        sql_file.write_text("")
        result = handle_command(f"\\. {sql_file}", mock_conn, formatter, state)
        assert result is True


class TestSystemCommand:

    def test_system_echo(self, mock_conn, formatter, state):
        result = handle_command("SYSTEM echo hello", mock_conn, formatter, state)
        assert result is True

    def test_backslash_bang(self, mock_conn, formatter, state):
        result = handle_command("\\! echo hello", mock_conn, formatter, state)
        assert result is True


class TestTeeCommand:

    def test_tee(self, mock_conn, formatter, state, tmp_path):
        result = handle_command(f"TEE {tmp_path}/out.log", mock_conn, formatter, state)
        assert result is True
        formatter.start_tee.assert_called_once()

    def test_notee(self, mock_conn, formatter, state):
        result = handle_command("NOTEE", mock_conn, formatter, state)
        assert result is True
        formatter.stop_tee.assert_called_once()


class TestPagerCommand:

    def test_pager_with_cmd(self, mock_conn, formatter, state):
        result = handle_command("PAGER less -S", mock_conn, formatter, state)
        assert result is True
        formatter.set_pager.assert_called_once_with("less -S")

    def test_pager_default(self, mock_conn, formatter, state):
        result = handle_command("PAGER", mock_conn, formatter, state)
        assert result is True
        formatter.set_pager.assert_called_once_with("less")

    def test_nopager(self, mock_conn, formatter, state):
        result = handle_command("NOPAGER", mock_conn, formatter, state)
        assert result is True
        formatter.clear_pager.assert_called_once()


class TestWarningsCommand:

    def test_warnings(self, mock_conn, formatter, state):
        handle_command("WARNINGS", mock_conn, formatter, state)
        assert state["show_warnings"] is True

    def test_nowarning(self, mock_conn, formatter, state):
        state["show_warnings"] = True
        handle_command("NOWARNING", mock_conn, formatter, state)
        assert state["show_warnings"] is False


class TestDelimiterCommand:

    def test_set_delimiter(self, mock_conn, formatter, state):
        handle_command("DELIMITER //", mock_conn, formatter, state)
        assert state["delimiter"] == "//"

    def test_set_delimiter_dollar(self, mock_conn, formatter, state):
        handle_command("DELIMITER $$", mock_conn, formatter, state)
        assert state["delimiter"] == "$$"


class TestConnectCommand:

    def test_connect(self, mock_conn, formatter, state):
        result = handle_command("CONNECT newdb", mock_conn, formatter, state)
        assert result is True

    def test_connect_with_host(self, mock_conn, formatter, state):
        result = handle_command("CONNECT newdb newhost", mock_conn, formatter, state)
        assert result is True


class TestRehashCommand:

    def test_rehash(self, mock_conn, formatter, state):
        handle_command("REHASH", mock_conn, formatter, state)
        assert state["rehash"] is True

    def test_backslash_hash(self, mock_conn, formatter, state):
        handle_command("\\#", mock_conn, formatter, state)
        assert state["rehash"] is True


class TestExitCommand:

    def test_exit(self, mock_conn, formatter, state):
        handle_command("EXIT", mock_conn, formatter, state)
        assert state["exit"] is True
        formatter.print_message.assert_called_with("Bye")

    def test_quit(self, mock_conn, formatter, state):
        handle_command("QUIT", mock_conn, formatter, state)
        assert state["exit"] is True

    def test_backslash_q(self, mock_conn, formatter, state):
        handle_command("\\q", mock_conn, formatter, state)
        assert state["exit"] is True


class TestHelpCommand:

    def test_help(self, mock_conn, formatter, state):
        result = handle_command("HELP", mock_conn, formatter, state)
        assert result is True
        msg = formatter.print_message.call_args[0][0]
        assert "mysqlpg" in msg.lower() or "source" in msg.lower()


class TestSetAutocommit:

    def test_autocommit_off(self, mock_conn, formatter, state):
        result = handle_command("SET autocommit = 0", mock_conn, formatter, state)
        assert result is True

    def test_autocommit_on(self, mock_conn, formatter, state):
        result = handle_command("SET autocommit = 1", mock_conn, formatter, state)
        assert result is True


class TestNotACommand:

    def test_select_not_command(self, mock_conn, formatter, state):
        result = handle_command("SELECT 1", mock_conn, formatter, state)
        assert result is False

    def test_insert_not_command(self, mock_conn, formatter, state):
        result = handle_command("INSERT INTO t VALUES (1)", mock_conn, formatter, state)
        assert result is False

    def test_show_not_command(self, mock_conn, formatter, state):
        result = handle_command("SHOW DATABASES", mock_conn, formatter, state)
        assert result is False

    def test_empty_not_command(self, mock_conn, formatter, state):
        result = handle_command("", mock_conn, formatter, state)
        assert result is False

"""Tests for mysqlpg.connection — PostgreSQL connection management.

These tests use a live PostgreSQL connection where available,
and mock-based tests for unit testing without a database.
"""

import pytest
from unittest.mock import patch, MagicMock
from tests.conftest import pg_available


class TestMockConnection:
    """Test Connection behavior with mocked psycopg2."""

    @patch("mysqlpg.connection.psycopg2")
    def test_connect(self, mock_pg):
        mock_conn = MagicMock()
        mock_conn.server_version = 160002
        mock_conn.notices = []
        mock_pg.connect.return_value = mock_conn

        from mysqlpg.connection import Connection
        conn = Connection(host="localhost", port=5432, user="test")

        mock_pg.connect.assert_called_once()
        assert mock_conn.autocommit is True

    @patch("mysqlpg.connection.psycopg2")
    def test_execute_select(self, mock_pg):
        mock_conn = MagicMock()
        mock_conn.server_version = 160002
        mock_conn.notices = []
        mock_pg.connect.return_value = mock_conn

        mock_cursor = MagicMock()
        mock_cursor.description = [("id",), ("name",)]
        mock_cursor.fetchall.return_value = [(1, "Alice")]
        mock_cursor.statusmessage = "SELECT 1"
        mock_cursor.rowcount = 1
        mock_conn.cursor.return_value = mock_cursor

        from mysqlpg.connection import Connection
        conn = Connection(host="localhost")
        columns, rows, status, rowcount, elapsed = conn.execute("SELECT 1")

        assert columns == ["id", "name"]
        assert rows == [(1, "Alice")]
        assert elapsed >= 0

    @patch("mysqlpg.connection.psycopg2")
    def test_execute_insert(self, mock_pg):
        mock_conn = MagicMock()
        mock_conn.server_version = 160002
        mock_conn.notices = []
        mock_pg.connect.return_value = mock_conn

        mock_cursor = MagicMock()
        mock_cursor.description = None
        mock_cursor.statusmessage = "INSERT 0 1"
        mock_cursor.rowcount = 1
        mock_conn.cursor.return_value = mock_cursor

        from mysqlpg.connection import Connection
        conn = Connection(host="localhost")
        columns, rows, status, rowcount, elapsed = conn.execute(
            "INSERT INTO t VALUES (1)"
        )

        assert columns is None
        assert rows is None
        assert rowcount == 1

    @patch("mysqlpg.connection.psycopg2")
    def test_reconnect(self, mock_pg):
        mock_conn = MagicMock()
        mock_conn.server_version = 160002
        mock_conn.notices = []
        mock_conn.closed = False
        mock_pg.connect.return_value = mock_conn

        from mysqlpg.connection import Connection
        conn = Connection(host="localhost")
        conn.reconnect(database="newdb")

        assert conn.database == "newdb"
        assert mock_pg.connect.call_count == 2  # initial + reconnect

    @patch("mysqlpg.connection.psycopg2")
    def test_close(self, mock_pg):
        mock_conn = MagicMock()
        mock_conn.server_version = 160002
        mock_conn.notices = []
        mock_conn.closed = False
        mock_pg.connect.return_value = mock_conn

        from mysqlpg.connection import Connection
        conn = Connection(host="localhost")
        conn.close()
        mock_conn.close.assert_called_once()

    @patch("mysqlpg.connection.psycopg2")
    def test_autocommit_property(self, mock_pg):
        mock_conn = MagicMock()
        mock_conn.server_version = 160002
        mock_conn.notices = []
        mock_conn.autocommit = True
        mock_pg.connect.return_value = mock_conn

        from mysqlpg.connection import Connection
        conn = Connection(host="localhost")
        assert conn.autocommit is True

        conn.set_autocommit(False)
        mock_conn.autocommit = False

    @patch("mysqlpg.connection.psycopg2")
    def test_notice_collection(self, mock_pg):
        mock_conn = MagicMock()
        mock_conn.server_version = 160002
        mock_conn.notices = []
        mock_pg.connect.return_value = mock_conn

        mock_cursor = MagicMock()
        mock_cursor.description = None
        mock_cursor.statusmessage = "OK"
        mock_cursor.rowcount = 0
        mock_conn.cursor.return_value = mock_cursor

        from mysqlpg.connection import Connection
        conn = Connection(host="localhost")

        # Simulate a notice appearing after query execution
        mock_conn.notices = ["NOTICE: test warning\n"]
        conn.execute("SELECT 1")

        notices = conn.pop_notices()
        assert len(notices) == 1
        assert "test warning" in notices[0]

    @patch("mysqlpg.connection.psycopg2")
    def test_execute_error_propagates(self, mock_pg):
        mock_conn = MagicMock()
        mock_conn.server_version = 160002
        mock_conn.notices = []
        mock_pg.connect.return_value = mock_conn

        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = Exception("syntax error")
        mock_conn.cursor.return_value = mock_cursor

        from mysqlpg.connection import Connection
        conn = Connection(host="localhost")
        with pytest.raises(Exception, match="syntax error"):
            conn.execute("INVALID SQL")


@pg_available
class TestLiveConnection:
    """Integration tests with a real PostgreSQL instance."""

    def test_connect_and_query(self, live_conn):
        cols, rows, status, rowcount, elapsed = live_conn.execute("SELECT 1 AS x")
        assert cols == ["x"]
        assert rows == [(1,)]

    def test_get_databases(self, live_conn):
        dbs = live_conn.get_databases()
        assert isinstance(dbs, list)
        assert len(dbs) > 0

    def test_get_tables(self, live_conn):
        tables = live_conn.get_tables()
        assert isinstance(tables, list)

    def test_get_current_database(self, live_conn):
        db = live_conn.get_current_database()
        assert isinstance(db, str)

    def test_get_current_user(self, live_conn):
        user = live_conn.get_current_user()
        assert isinstance(user, str)

    def test_get_server_version(self, live_conn):
        version = live_conn.get_server_version_string()
        assert isinstance(version, str)
        # Should contain a version number
        assert any(c.isdigit() for c in version)

    def test_get_connection_id(self, live_conn):
        pid = live_conn.get_connection_id()
        assert isinstance(pid, int)
        assert pid > 0

    def test_get_uptime(self, live_conn):
        uptime = live_conn.get_uptime()
        assert isinstance(uptime, str)

    def test_pop_notices_empty(self, live_conn):
        notices = live_conn.pop_notices()
        assert isinstance(notices, list)

    def test_reconnect(self, live_conn):
        old_pid = live_conn.get_connection_id()
        live_conn.reconnect()
        new_pid = live_conn.get_connection_id()
        # PIDs should differ after reconnect
        assert old_pid != new_pid

    def test_execute_with_params(self, live_conn):
        cols, rows, *_ = live_conn.execute(
            "SELECT %s::int AS val", (42,)
        )
        assert rows[0][0] == 42

    def test_execute_ddl(self, live_conn):
        try:
            live_conn.execute("DROP TABLE IF EXISTS _mysqlpg_test_tmp")
            cols, rows, status, rowcount, elapsed = live_conn.execute(
                "CREATE TABLE _mysqlpg_test_tmp (id serial PRIMARY KEY, name text)"
            )
            assert "CREATE TABLE" in status
        finally:
            live_conn.execute("DROP TABLE IF EXISTS _mysqlpg_test_tmp")

    def test_primary_key_columns(self, live_conn):
        try:
            live_conn.execute("DROP TABLE IF EXISTS _mysqlpg_test_pk")
            live_conn.execute(
                "CREATE TABLE _mysqlpg_test_pk (a int, b int, PRIMARY KEY (a, b))"
            )
            pk = live_conn.get_primary_key_columns("_mysqlpg_test_pk")
            assert set(pk) == {"a", "b"}
        finally:
            live_conn.execute("DROP TABLE IF EXISTS _mysqlpg_test_pk")

    def test_get_columns(self, live_conn):
        try:
            live_conn.execute("DROP TABLE IF EXISTS _mysqlpg_test_cols")
            live_conn.execute(
                "CREATE TABLE _mysqlpg_test_cols (id serial, name text, age int)"
            )
            cols = live_conn.get_columns("_mysqlpg_test_cols")
            assert cols == ["id", "name", "age"]
        finally:
            live_conn.execute("DROP TABLE IF EXISTS _mysqlpg_test_cols")

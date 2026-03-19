"""Shared test fixtures for mysqlpg test suite."""

import os
import pytest
from unittest.mock import MagicMock, patch


# ---------------------------------------------------------------------------
# Mock Connection fixture (no live PG required)
# ---------------------------------------------------------------------------

class MockConnection:
    """Lightweight mock of mysqlpg.connection.Connection for unit tests."""

    def __init__(self, database="testdb", user="postgres", host="localhost",
                 port=5432):
        self.database = database
        self.user = user
        self.host = host
        self.port = port
        self.notices = []
        self._autocommit = True
        self._tables = ["users", "posts", "comments"]
        self._columns = {
            "users": ["id", "name", "email", "active"],
            "posts": ["id", "user_id", "title", "body", "created_at"],
            "comments": ["id", "post_id", "author", "text"],
        }
        self._pk_columns = {
            "users": ["id"],
            "posts": ["id"],
            "comments": ["id"],
        }

    def execute(self, sql, params=None):
        """Simulate query execution — returns sensible defaults."""
        import time
        elapsed = 0.001
        upper = sql.strip().upper()

        if upper.startswith("SELECT CURRENT_DATABASE"):
            return ["current_database"], [(self.database,)], "SELECT 1", 1, elapsed
        if upper.startswith("SELECT CURRENT_USER"):
            return ["current_user"], [(self.user,)], "SELECT 1", 1, elapsed
        if upper.startswith("SHOW SERVER_VERSION"):
            return ["server_version"], [("16.2",)], "SHOW", 1, elapsed
        if upper.startswith("SELECT PG_BACKEND_PID"):
            return ["pg_backend_pid"], [(12345,)], "SELECT 1", 1, elapsed
        if "PG_DATABASE" in upper and "DATISTEMPLATE" in upper:
            return ["datname"], [("testdb",), ("postgres",)], "SELECT 2", 2, elapsed
        if "INFORMATION_SCHEMA.TABLES" in upper:
            rows = [(t,) for t in self._tables]
            return ["table_name"], rows, f"SELECT {len(rows)}", len(rows), elapsed
        if "INFORMATION_SCHEMA.COLUMNS" in upper and params:
            table = params[-1] if params else "users"
            cols = self._columns.get(table, [])
            rows = [(c,) for c in cols]
            return ["column_name"], rows, f"SELECT {len(rows)}", len(rows), elapsed
        if "PG_CONSTRAINT" in upper and params:
            table = params[0] if params else "users"
            pk = self._pk_columns.get(table, [])
            rows = [(c,) for c in pk]
            return ["attname"], rows, f"SELECT {len(rows)}", len(rows), elapsed

        # Default: non-SELECT
        return None, None, "OK", 0, elapsed

    def execute_with_cursor(self, sql, name="cursor", itersize=1000):
        return None, iter([]), True

    def finish_cursor(self, cur, old_ac):
        pass

    def reconnect(self, database=None):
        if database:
            self.database = database

    def get_databases(self):
        return ["testdb", "postgres"]

    def get_tables(self, schema="public"):
        return list(self._tables)

    def get_columns(self, table, schema="public"):
        return list(self._columns.get(table, []))

    def get_primary_key_columns(self, table, schema="public"):
        return list(self._pk_columns.get(table, []))

    def get_current_database(self):
        return self.database

    def get_current_user(self):
        return self.user

    def get_server_version_string(self):
        return "16.2"

    def get_connection_id(self):
        return 12345

    def get_uptime(self):
        return "01:23:45"

    def pop_notices(self):
        n = list(self.notices)
        self.notices.clear()
        return n

    def close(self):
        pass

    @property
    def closed(self):
        return False

    def set_autocommit(self, value):
        self._autocommit = value

    @property
    def autocommit(self):
        return self._autocommit


@pytest.fixture
def mock_conn():
    """Provide a MockConnection instance."""
    return MockConnection()


@pytest.fixture
def mock_conn_with_data():
    """MockConnection with richer data for testing."""
    conn = MockConnection()
    conn._tables = ["users", "posts", "comments", "tags", "post_tags"]
    conn._columns["tags"] = ["id", "name"]
    conn._columns["post_tags"] = ["post_id", "tag_id"]
    conn._pk_columns["tags"] = ["id"]
    conn._pk_columns["post_tags"] = ["post_id", "tag_id"]
    return conn


# ---------------------------------------------------------------------------
# Live PG connection fixture (requires running PostgreSQL)
# ---------------------------------------------------------------------------

def _pg_available():
    """Check if PostgreSQL is reachable."""
    try:
        import psycopg2
        conn = psycopg2.connect(
            host=os.environ.get("PGHOST", "localhost"),
            port=int(os.environ.get("PGPORT", "5432")),
            user=os.environ.get("PGUSER", "postgres"),
            dbname=os.environ.get("PGDATABASE", "testdb"),
        )
        conn.close()
        return True
    except Exception:
        return False


pg_available = pytest.mark.skipif(
    not _pg_available(),
    reason="PostgreSQL not available"
)


@pytest.fixture
def live_conn():
    """Provide a live Connection to PostgreSQL (skipped if unavailable)."""
    from mysqlpg.connection import Connection
    conn = Connection(
        host=os.environ.get("PGHOST", "localhost"),
        port=int(os.environ.get("PGPORT", "5432")),
        user=os.environ.get("PGUSER", "postgres"),
        database=os.environ.get("PGDATABASE", "testdb"),
    )
    yield conn
    conn.close()

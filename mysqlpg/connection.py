"""PostgreSQL connection management with MySQL-compatible interface."""

import psycopg2
import psycopg2.extras
import time


class Connection:
    """Wraps psycopg2 connection with MySQL-compatible behavior."""

    def __init__(self, host="localhost", port=5432, user=None, password=None,
                 database=None, connect_timeout=10):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.connect_timeout = connect_timeout
        self.conn = None
        self.notices = []
        self.server_version = None
        self._connect()

    def _connect(self):
        params = {
            "host": self.host,
            "port": self.port,
            "connect_timeout": self.connect_timeout,
        }
        if self.user:
            params["user"] = self.user
        if self.password:
            params["password"] = self.password
        if self.database:
            params["dbname"] = self.database

        self.conn = psycopg2.connect(**params)
        self.conn.autocommit = True
        self.server_version = self.conn.server_version
        self.conn.notices = []

        # Set up notice handler
        self.notices = []

    def _collect_notices(self):
        """Collect any NOTICE messages from the connection."""
        if self.conn.notices:
            for n in self.conn.notices:
                self.notices.append(n.strip())
            self.conn.notices.clear()

    def execute(self, sql, params=None):
        """Execute SQL and return (columns, rows, status_message, rowcount).

        Returns (None, None, status, rowcount) for non-SELECT statements.
        """
        start = time.time()
        cur = self.conn.cursor()
        try:
            cur.execute(sql, params)
            self._collect_notices()
            status = cur.statusmessage
            rowcount = cur.rowcount

            if cur.description:
                columns = [desc[0] for desc in cur.description]
                rows = cur.fetchall()
                elapsed = time.time() - start
                return columns, rows, status, rowcount, elapsed
            else:
                elapsed = time.time() - start
                return None, None, status, rowcount, elapsed
        except Exception:
            self._collect_notices()
            raise
        finally:
            cur.close()

    def execute_with_cursor(self, sql, name="dump_cursor", itersize=1000):
        """Execute with a server-side cursor for streaming large results."""
        cur = self.conn.cursor(name=name)
        cur.itersize = itersize
        # Server-side cursors require a transaction
        old_autocommit = self.conn.autocommit
        if old_autocommit:
            self.conn.autocommit = False
        try:
            cur.execute(sql)
            columns = [desc[0] for desc in cur.description] if cur.description else None
            return columns, cur, old_autocommit
        except Exception:
            cur.close()
            if old_autocommit:
                self.conn.autocommit = True
            raise

    def finish_cursor(self, cur, old_autocommit):
        """Clean up after streaming cursor."""
        cur.close()
        if old_autocommit:
            self.conn.rollback()
            self.conn.autocommit = True

    def reconnect(self, database=None):
        """Reconnect, optionally to a different database (for USE command)."""
        if database:
            self.database = database
        if self.conn and not self.conn.closed:
            self.conn.close()
        self._connect()

    def get_databases(self):
        """List all non-template databases."""
        cols, rows, *_ = self.execute(
            "SELECT datname FROM pg_database WHERE datistemplate = false ORDER BY datname"
        )
        return [r[0] for r in rows] if rows else []

    def get_tables(self, schema="public"):
        """List tables in the given schema."""
        cols, rows, *_ = self.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = %s ORDER BY table_name",
            (schema,)
        )
        return [r[0] for r in rows] if rows else []

    def get_columns(self, table, schema="public"):
        """List columns for a table."""
        cols, rows, *_ = self.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = %s AND table_name = %s "
            "ORDER BY ordinal_position",
            (schema, table)
        )
        return [r[0] for r in rows] if rows else []

    def get_primary_key_columns(self, table, schema="public"):
        """Get primary key column names for a table."""
        cols, rows, *_ = self.execute(
            """
            SELECT a.attname
            FROM pg_constraint c
            JOIN pg_attribute a ON a.attnum = ANY(c.conkey) AND a.attrelid = c.conrelid
            WHERE c.contype = 'p'
              AND c.conrelid = (
                  SELECT oid FROM pg_class
                  WHERE relname = %s
                    AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = %s)
              )
            ORDER BY array_position(c.conkey, a.attnum)
            """,
            (table, schema)
        )
        return [r[0] for r in rows] if rows else []

    def get_current_database(self):
        """Return current database name."""
        cols, rows, *_ = self.execute("SELECT current_database()")
        return rows[0][0] if rows else None

    def get_current_user(self):
        """Return current user name."""
        cols, rows, *_ = self.execute("SELECT current_user")
        return rows[0][0] if rows else None

    def get_server_version_string(self):
        """Return human-readable server version."""
        cols, rows, *_ = self.execute("SHOW server_version")
        return rows[0][0] if rows else str(self.server_version)

    def get_connection_id(self):
        """Return backend PID (equivalent to MySQL connection_id)."""
        cols, rows, *_ = self.execute("SELECT pg_backend_pid()")
        return rows[0][0] if rows else None

    def get_uptime(self):
        """Return server uptime string."""
        cols, rows, *_ = self.execute(
            "SELECT now() - pg_postmaster_start_time()"
        )
        return str(rows[0][0]) if rows else "unknown"

    def pop_notices(self):
        """Return and clear collected NOTICE messages."""
        n = list(self.notices)
        self.notices.clear()
        return n

    def close(self):
        if self.conn and not self.conn.closed:
            self.conn.close()

    @property
    def closed(self):
        return self.conn is None or self.conn.closed

    def set_autocommit(self, value):
        self.conn.autocommit = value

    @property
    def autocommit(self):
        return self.conn.autocommit

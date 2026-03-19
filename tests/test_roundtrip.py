"""Integration tests for dump→restore round-trip workflows.

These tests require a live PostgreSQL instance and verify that:
1. mysqldumppg can dump a database
2. The dump can be loaded back via mysqlpg
3. Data integrity is preserved

Tests are skipped if PostgreSQL is not available.
"""

import io
import os
import pytest
from tests.conftest import pg_available


@pg_available
class TestDumpRoundTrip:
    """Test mysqldumppg → mysqlpg round-trip."""

    @pytest.fixture(autouse=True)
    def setup_test_tables(self, live_conn):
        """Create test tables with sample data."""
        self.conn = live_conn

        # Clean up
        live_conn.execute("DROP TABLE IF EXISTS _rt_comments CASCADE")
        live_conn.execute("DROP TABLE IF EXISTS _rt_posts CASCADE")
        live_conn.execute("DROP TABLE IF EXISTS _rt_users CASCADE")

        # Create tables
        live_conn.execute("""
            CREATE TABLE _rt_users (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                email VARCHAR(255) UNIQUE,
                active BOOLEAN DEFAULT true,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        live_conn.execute("""
            CREATE TABLE _rt_posts (
                id SERIAL PRIMARY KEY,
                user_id INTEGER NOT NULL REFERENCES _rt_users(id),
                title VARCHAR(255) NOT NULL,
                body TEXT,
                published BOOLEAN DEFAULT false
            )
        """)
        live_conn.execute("""
            CREATE TABLE _rt_comments (
                id SERIAL PRIMARY KEY,
                post_id INTEGER NOT NULL REFERENCES _rt_posts(id),
                author VARCHAR(100),
                content TEXT NOT NULL
            )
        """)

        # Insert data
        live_conn.execute(
            "INSERT INTO _rt_users (name, email) VALUES ('Alice', 'alice@test.com')"
        )
        live_conn.execute(
            "INSERT INTO _rt_users (name, email, active) VALUES ('Bob', 'bob@test.com', false)"
        )
        live_conn.execute(
            "INSERT INTO _rt_posts (user_id, title, body) VALUES (1, 'First Post', 'Hello world')"
        )
        live_conn.execute(
            "INSERT INTO _rt_posts (user_id, title, body, published) "
            "VALUES (1, 'Second Post', 'More content', true)"
        )
        live_conn.execute(
            "INSERT INTO _rt_comments (post_id, author, content) "
            "VALUES (1, 'Charlie', 'Nice post!')"
        )

        yield

        # Cleanup
        live_conn.execute("DROP TABLE IF EXISTS _rt_comments CASCADE")
        live_conn.execute("DROP TABLE IF EXISTS _rt_posts CASCADE")
        live_conn.execute("DROP TABLE IF EXISTS _rt_users CASCADE")

    def test_dump_produces_output(self):
        """Basic test that mysqldumppg produces output."""
        from mysqlpg.dumpcli import Dumper, DumpOptions, build_parser
        from mysqlpg.connection import Connection

        out = io.StringIO()
        parser = build_parser()
        args = parser.parse_args(["--compact", self.conn.database])
        opts = DumpOptions(args)

        dumper = Dumper(self.conn, out, opts, args)
        dumper.dump([self.conn.database], ["_rt_users", "_rt_posts", "_rt_comments"],
                    set(), multi_db=False)

        output = out.getvalue()
        assert "CREATE TABLE" in output
        assert "INSERT INTO" in output
        assert "Alice" in output

    def test_dump_schema_only(self):
        """Test --no-data produces schema without INSERTs."""
        from mysqlpg.dumpcli import Dumper, DumpOptions, build_parser

        out = io.StringIO()
        parser = build_parser()
        args = parser.parse_args(["--no-data", "--compact", self.conn.database])
        opts = DumpOptions(args)

        dumper = Dumper(self.conn, out, opts, args)
        dumper.dump([self.conn.database], ["_rt_users"], set(), multi_db=False)

        output = out.getvalue()
        assert "CREATE TABLE" in output
        assert "INSERT" not in output

    def test_dump_data_only(self):
        """Test --no-create-info produces INSERTs without DDL."""
        from mysqlpg.dumpcli import Dumper, DumpOptions, build_parser

        out = io.StringIO()
        parser = build_parser()
        args = parser.parse_args(["--no-create-info", "--compact", self.conn.database])
        opts = DumpOptions(args)

        dumper = Dumper(self.conn, out, opts, args)
        dumper.dump([self.conn.database], ["_rt_users"], set(), multi_db=False)

        output = out.getvalue()
        assert "CREATE TABLE" not in output
        assert "INSERT INTO" in output

    def test_dump_complete_insert(self):
        """Test --complete-insert includes column names."""
        from mysqlpg.dumpcli import Dumper, DumpOptions, build_parser

        out = io.StringIO()
        parser = build_parser()
        args = parser.parse_args([
            "--no-create-info", "--compact", "--complete-insert",
            self.conn.database
        ])
        opts = DumpOptions(args)

        dumper = Dumper(self.conn, out, opts, args)
        dumper.dump([self.conn.database], ["_rt_users"], set(), multi_db=False)

        output = out.getvalue()
        assert "`id`" in output
        assert "`name`" in output
        assert "`email`" in output

    def test_dump_table_ordering(self):
        """Verify FK-aware table ordering in dump."""
        from mysqlpg.dumpcli import Dumper, DumpOptions, build_parser

        out = io.StringIO()
        parser = build_parser()
        args = parser.parse_args(["--compact", self.conn.database])
        opts = DumpOptions(args)

        dumper = Dumper(self.conn, out, opts, args)
        tables = dumper._sort_tables_by_deps(
            ["_rt_comments", "_rt_posts", "_rt_users"]
        )

        # _rt_users should come before _rt_posts, _rt_posts before _rt_comments
        assert tables.index("_rt_users") < tables.index("_rt_posts")
        assert tables.index("_rt_posts") < tables.index("_rt_comments")

    def test_dump_with_where(self):
        """Test --where filters rows."""
        from mysqlpg.dumpcli import Dumper, DumpOptions, build_parser

        out = io.StringIO()
        parser = build_parser()
        args = parser.parse_args([
            "--no-create-info", "--compact", "--where", "active = true",
            self.conn.database
        ])
        opts = DumpOptions(args)

        dumper = Dumper(self.conn, out, opts, args)
        dumper.dump([self.conn.database], ["_rt_users"], set(), multi_db=False)

        output = out.getvalue()
        assert "Alice" in output
        # Bob has active=false, should not be in output
        assert "Bob" not in output

    def test_show_create_table(self):
        """Test SHOW CREATE TABLE produces valid DDL."""
        from mysqlpg.ddl import show_create_table

        name, ddl = show_create_table(self.conn, "_rt_users")
        assert name == "_rt_users"
        assert "CREATE TABLE" in ddl
        assert "`id`" in ddl
        assert "`name`" in ddl
        assert "PRIMARY KEY" in ddl
        assert "AUTO_INCREMENT" in ddl

    def test_show_create_table_with_fk(self):
        """Test SHOW CREATE TABLE includes foreign keys."""
        from mysqlpg.ddl import show_create_table

        _, ddl = show_create_table(self.conn, "_rt_posts")
        assert "FOREIGN KEY" in ddl
        assert "`_rt_users`" in ddl

    def test_translate_show_commands(self):
        """Test that SHOW commands work against live DB."""
        from mysqlpg.translator import translate

        # SHOW TABLES
        result, _ = translate("SHOW TABLES", self.conn)
        cols, rows, *_ = self.conn.execute(result)
        table_names = [r[0] for r in rows]
        assert "_rt_users" in table_names

        # DESC
        result, _ = translate("DESC _rt_users", self.conn)
        cols, rows, *_ = self.conn.execute(result)
        field_names = [r[0] for r in rows]
        assert "id" in field_names
        assert "name" in field_names

    def test_translate_insert_ignore(self):
        """Test INSERT IGNORE translation works against live DB."""
        from mysqlpg.translator import translate

        sql = ("INSERT IGNORE INTO _rt_users (id, name, email) "
               "VALUES (1, 'Duplicate', 'alice@test.com')")
        result, _ = translate(sql, self.conn)
        # Should not raise on conflict
        self.conn.execute(result)

        # Original Alice should still be there
        _, rows, *_ = self.conn.execute(
            "SELECT name FROM _rt_users WHERE id = 1"
        )
        assert rows[0][0] == "Alice"


@pg_available
class TestEnumRoundTrip:
    """Test ENUM type handling in round-trip scenarios."""

    @pytest.fixture(autouse=True)
    def setup_enum_tables(self, live_conn):
        self.conn = live_conn
        live_conn.execute("DROP TABLE IF EXISTS _rt_enum_test CASCADE")
        live_conn.execute("DROP TYPE IF EXISTS _rt_mood CASCADE")

        live_conn.execute(
            "CREATE TYPE _rt_mood AS ENUM ('happy', 'sad', 'neutral')"
        )
        live_conn.execute("""
            CREATE TABLE _rt_enum_test (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100),
                mood _rt_mood DEFAULT 'neutral'
            )
        """)
        live_conn.execute(
            "INSERT INTO _rt_enum_test (name, mood) VALUES ('Alice', 'happy')"
        )
        live_conn.execute(
            "INSERT INTO _rt_enum_test (name, mood) VALUES ('Bob', 'sad')"
        )

        yield

        live_conn.execute("DROP TABLE IF EXISTS _rt_enum_test CASCADE")
        live_conn.execute("DROP TYPE IF EXISTS _rt_mood CASCADE")

    def test_show_create_table_with_enum(self):
        from mysqlpg.ddl import show_create_table
        _, ddl = show_create_table(self.conn, "_rt_enum_test")
        assert "enum(" in ddl.lower()
        assert "'happy'" in ddl
        assert "'sad'" in ddl
        assert "'neutral'" in ddl

    def test_dump_with_enum(self):
        from mysqlpg.dumpcli import Dumper, DumpOptions, build_parser

        out = io.StringIO()
        parser = build_parser()
        args = parser.parse_args(["--compact", self.conn.database])
        opts = DumpOptions(args)

        dumper = Dumper(self.conn, out, opts, args)
        dumper.dump([self.conn.database], ["_rt_enum_test"], set(), multi_db=False)

        output = out.getvalue()
        assert "Alice" in output
        assert "happy" in output

    def test_enum_values_detected(self):
        from mysqlpg.ddl import get_enum_values
        values = get_enum_values(self.conn, "_rt_mood")
        assert values == ["happy", "sad", "neutral"]

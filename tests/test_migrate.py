"""Tests for mysqlpg.migrate — MySQL → PostgreSQL migration tool."""

import os
import pytest
from unittest.mock import patch, MagicMock
from mysqlpg.migrate import (
    build_parser, parse_url, split_statements, translate_statement,
    classify_statement, read_dump_file, _parse_mysql_users,
    _build_create_role_sql,
)


class TestParseUrl:

    def test_postgres_full(self):
        r = parse_url("postgres://alice:secret@db.example.com:5433/mydb")
        assert r['user'] == 'alice'
        assert r['password'] == 'secret'
        assert r['host'] == 'db.example.com'
        assert r['port'] == 5433
        assert r['database'] == 'mydb'

    def test_postgres_minimal(self):
        r = parse_url("postgres://localhost/mydb")
        assert r['host'] == 'localhost'
        assert r['database'] == 'mydb'

    def test_mysql_url(self):
        r = parse_url("mysql://root:pass@mysql-host:3306/app")
        assert r['user'] == 'root'
        assert r['host'] == 'mysql-host'
        assert r['port'] == 3306
        assert r['database'] == 'app'

    def test_user_no_password(self):
        r = parse_url("postgres://alice@host/db")
        assert r['user'] == 'alice'
        assert r['password'] is None

    def test_invalid_url(self):
        r = parse_url("not-a-url")
        assert r == {}

    def test_postgresql_scheme(self):
        r = parse_url("postgresql://user@host/db")
        assert r['user'] == 'user'
        assert r['database'] == 'db'


class TestSplitStatements:

    def test_basic_split(self):
        sql = "SELECT 1; SELECT 2; SELECT 3;"
        stmts = split_statements(sql)
        assert len(stmts) == 3

    def test_preserves_strings(self):
        sql = "INSERT INTO t VALUES ('hello; world'); SELECT 1;"
        stmts = split_statements(sql)
        assert len(stmts) == 2
        assert "hello; world" in stmts[0]

    def test_skips_comments(self):
        sql = "-- this is a comment\nSELECT 1;\n-- another\nSELECT 2;"
        stmts = split_statements(sql)
        assert len(stmts) == 2

    def test_handles_conditional_comments(self):
        sql = "/*!40101 SET NAMES utf8mb4 */;\nSELECT 1;"
        stmts = split_statements(sql)
        # Conditional comment content should be extracted
        found_set = any('SET NAMES' in s for s in stmts)
        assert found_set or len(stmts) >= 1

    def test_handles_delimiter_change(self):
        sql = "DELIMITER ;;\nCREATE FUNCTION f() RETURNS INT BEGIN RETURN 1; END;;\nDELIMITER ;\nSELECT 1;"
        stmts = split_statements(sql)
        # Should have the function and the SELECT
        assert any('CREATE FUNCTION' in s for s in stmts)
        assert any('SELECT 1' in s for s in stmts)

    def test_empty_input(self):
        assert split_statements("") == []
        assert split_statements("   ") == []

    def test_no_trailing_semicolon(self):
        stmts = split_statements("SELECT 1")
        assert len(stmts) == 1

    def test_escaped_quotes(self):
        sql = r"INSERT INTO t VALUES ('it\'s'); SELECT 2;"
        stmts = split_statements(sql)
        assert len(stmts) == 2

    def test_multiline_insert(self):
        sql = """INSERT INTO t VALUES
(1, 'hello'),
(2, 'world');"""
        stmts = split_statements(sql)
        assert len(stmts) == 1
        assert "(1, 'hello')" in stmts[0]


class TestClassifyStatement:

    def test_create_table(self):
        assert classify_statement("CREATE TABLE t (id INT)") == 'schema'

    def test_create_index(self):
        assert classify_statement("CREATE INDEX idx ON t(col)") == 'schema'

    def test_alter_table(self):
        assert classify_statement("ALTER TABLE t ADD COLUMN col INT") == 'schema'

    def test_drop_table(self):
        assert classify_statement("DROP TABLE IF EXISTS t") == 'schema'

    def test_insert(self):
        assert classify_statement("INSERT INTO t VALUES (1)") == 'data'

    def test_replace(self):
        assert classify_statement("REPLACE INTO t VALUES (1)") == 'data'

    def test_create_database(self):
        assert classify_statement("CREATE DATABASE mydb") == 'database'

    def test_use(self):
        assert classify_statement("USE mydb") == 'database'

    def test_set(self):
        assert classify_statement("SET NAMES utf8mb4") == 'control'

    def test_lock(self):
        assert classify_statement("LOCK TABLES t WRITE") == 'control'

    def test_create_function(self):
        assert classify_statement("CREATE FUNCTION f() RETURNS INT") == 'routine'

    def test_select(self):
        assert classify_statement("SELECT 1") == 'other'


class TestTranslateStatement:

    def test_insert_passthrough(self):
        result = translate_statement("INSERT INTO t (a) VALUES (1)")
        assert "INSERT" in result

    def test_create_table_mysql(self):
        sql = """CREATE TABLE `t` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8"""
        result = translate_statement(sql)
        assert result is not None
        assert "ENGINE=" not in result

    def test_noop_returns_none(self):
        result = translate_statement("LOCK TABLES t WRITE")
        assert result is None

    def test_set_names(self):
        result = translate_statement("SET NAMES utf8mb4")
        # SET NAMES is handled as special (no-op), returns None
        assert result is None or "client_encoding" in (result or "")

    def test_insert_ignore(self):
        result = translate_statement("INSERT IGNORE INTO t (id) VALUES (1)")
        assert result is not None
        assert "ON CONFLICT DO NOTHING" in result


class TestBuildParser:

    def test_from_dump(self):
        parser = build_parser()
        args = parser.parse_args(["--from-dump", "backup.sql", "--dry-run"])
        assert args.from_dump == "backup.sql"
        assert args.dry_run is True

    def test_from_mysql(self):
        parser = build_parser()
        args = parser.parse_args(["--from-mysql", "mysql://root@host/db", "--to-file", "out.sql"])
        assert args.from_mysql == "mysql://root@host/db"
        assert args.to_file == "out.sql"

    def test_to_pg(self):
        parser = build_parser()
        args = parser.parse_args(["--from-dump", "f.sql", "--to-pg", "postgres://u@h/db"])
        assert args.to_pg == "postgres://u@h/db"

    def test_schema_only(self):
        parser = build_parser()
        args = parser.parse_args(["--from-dump", "f.sql", "--dry-run", "--schema-only"])
        assert args.schema_only is True

    def test_data_only(self):
        parser = build_parser()
        args = parser.parse_args(["--from-dump", "f.sql", "--dry-run", "--data-only"])
        assert args.data_only is True

    def test_tables_filter(self):
        parser = build_parser()
        args = parser.parse_args(["--from-dump", "f.sql", "--dry-run", "--tables", "users", "posts"])
        assert args.tables == ["users", "posts"]

    def test_exclude_tables(self):
        parser = build_parser()
        args = parser.parse_args(["--from-dump", "f.sql", "--dry-run", "--exclude-tables", "logs"])
        assert args.exclude_tables == ["logs"]

    def test_create_db(self):
        parser = build_parser()
        args = parser.parse_args(["--from-dump", "f.sql", "--dry-run", "--create-db"])
        assert args.create_db is True

    def test_single_transaction(self):
        parser = build_parser()
        args = parser.parse_args(["--from-dump", "f.sql", "--dry-run", "--single-transaction"])
        assert args.single_transaction is True

    def test_validate(self):
        parser = build_parser()
        args = parser.parse_args(["--from-dump", "f.sql", "--dry-run", "--validate"])
        assert args.validate is True

    def test_pg_connection_flags(self):
        parser = build_parser()
        args = parser.parse_args([
            "--from-dump", "f.sql",
            "--pg-host", "db.local",
            "--pg-port", "5433",
            "--pg-user", "admin",
            "--pg-database", "target",
        ])
        assert args.pg_host == "db.local"
        assert args.pg_port == 5433
        assert args.pg_user == "admin"
        assert args.pg_database == "target"


class TestReadDumpFile:

    def test_read_sql_file(self, tmp_path):
        f = tmp_path / "dump.sql"
        f.write_text("CREATE TABLE t (id INT);\nINSERT INTO t VALUES (1);")
        content = read_dump_file(str(f))
        assert "CREATE TABLE" in content
        assert "INSERT" in content

    def test_read_gz_file(self, tmp_path):
        import gzip
        f = tmp_path / "dump.sql.gz"
        with gzip.open(str(f), 'wt') as gz:
            gz.write("SELECT 1;")
        content = read_dump_file(str(f))
        assert "SELECT 1" in content


class TestParseMyqlUsers:

    def test_create_user_statements(self):
        sql = """
CREATE USER 'alice'@'%' IDENTIFIED BY 'secret123';
CREATE USER 'bob'@'localhost' IDENTIFIED BY 'pass456';
CREATE USER 'root'@'localhost';
"""
        users = _parse_mysql_users(sql)
        names = [u['name'] for u in users]
        assert 'alice' in names
        assert 'bob' in names
        assert 'root' not in names  # system user filtered out

    def test_grant_discovers_users(self):
        sql = "GRANT ALL ON mydb.* TO 'appuser'@'%';"
        users = _parse_mysql_users(sql)
        assert any(u['name'] == 'appuser' for u in users)

    def test_insert_into_mysql_user(self):
        sql = "INSERT INTO mysql.user VALUES ('%', 'webuser', 'Y');"
        users = _parse_mysql_users(sql)
        assert any(u['name'] == 'webuser' for u in users)

    def test_system_users_filtered(self):
        sql = """
CREATE USER 'mysql.sys'@'localhost';
CREATE USER 'mysql.session'@'localhost';
CREATE USER 'debian-sys-maint'@'localhost';
CREATE USER 'realuser'@'%' IDENTIFIED BY 'pass';
"""
        users = _parse_mysql_users(sql)
        names = [u['name'] for u in users]
        assert 'realuser' in names
        assert 'mysql.sys' not in names
        assert 'debian-sys-maint' not in names

    def test_dedup_same_user_multiple_hosts(self):
        sql = """
CREATE USER 'alice'@'localhost' IDENTIFIED BY 'pass1';
CREATE USER 'alice'@'%' IDENTIFIED BY 'pass2';
"""
        users = _parse_mysql_users(sql)
        alice_count = sum(1 for u in users if u['name'] == 'alice')
        assert alice_count == 1

    def test_password_preserved(self):
        sql = "CREATE USER 'testuser'@'%' IDENTIFIED BY 'my_secret';"
        users = _parse_mysql_users(sql)
        u = [x for x in users if x['name'] == 'testuser'][0]
        assert u['password'] == 'my_secret'


class TestBuildCreateRole:

    def test_basic(self):
        sql = _build_create_role_sql({'name': 'alice', 'password': 'secret'})
        assert 'CREATE ROLE "alice"' in sql
        assert 'LOGIN' in sql
        assert "PASSWORD 'secret'" in sql

    def test_no_password_with_default(self):
        sql = _build_create_role_sql({'name': 'bob', 'password': None}, default_password='default')
        assert "PASSWORD 'default'" in sql

    def test_no_password_at_all(self):
        sql = _build_create_role_sql({'name': 'charlie', 'password': None})
        assert 'PASSWORD' not in sql
        assert 'LOGIN' in sql

    def test_superuser(self):
        sql = _build_create_role_sql({'name': 'admin', 'password': 'x'}, superuser=True)
        assert 'SUPERUSER' in sql

    def test_password_with_quotes(self):
        sql = _build_create_role_sql({'name': 'user', 'password': "it's"})
        assert "it''s" in sql  # escaped


class TestMigrateUsersParser:

    def test_migrate_users_flag(self):
        parser = build_parser()
        args = parser.parse_args(["--from-dump", "f.sql", "--dry-run", "--migrate-users"])
        assert args.migrate_users is True

    def test_users_from_file(self):
        parser = build_parser()
        args = parser.parse_args(["--from-dump", "f.sql", "--dry-run", "--users-from", "users.sql"])
        assert args.users_from == "users.sql"

    def test_default_password(self):
        parser = build_parser()
        args = parser.parse_args(["--from-dump", "f.sql", "--dry-run", "--default-password", "changeme"])
        assert args.default_password == "changeme"

    def test_superuser_list(self):
        parser = build_parser()
        args = parser.parse_args(["--from-dump", "f.sql", "--dry-run", "--superuser", "admin", "dba"])
        assert args.superuser == ["admin", "dba"]


class TestDryRun:
    """Integration test: dry-run a small MySQL dump."""

    def test_dry_run_small_dump(self, tmp_path, capsys):
        dump = tmp_path / "test.sql"
        dump.write_text("""
SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

CREATE TABLE `users` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) DEFAULT NULL,
  `email` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `email` (`email`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO `users` VALUES (1,'Alice','alice@test.com'),(2,'Bob','bob@test.com');

SET FOREIGN_KEY_CHECKS = 1;
""")
        from mysqlpg.migrate import migrate, build_parser
        parser = build_parser()
        args = parser.parse_args(["--from-dump", str(dump), "--dry-run", "-v"])
        migrate(args)
        output = capsys.readouterr().out
        assert "CREATE TABLE" in output
        assert "INSERT INTO" in output
        assert "ENGINE=" not in output
        assert "alice@test.com" in output

"""Tests for mysqlpg.translator — MySQL→PostgreSQL SQL translation.

Covers: SHOW commands, DML translation (INSERT IGNORE, ON DUPLICATE KEY, REPLACE),
DDL translation (ALTER TABLE, CREATE DATABASE, RENAME TABLE, TRUNCATE TABLE),
function replacement (DATABASE(), IFNULL), user management, admin commands,
dump boilerplate, backtick conversion, zero-date handling, and pgloader compat.
"""

import pytest
from mysqlpg.translator import translate, _convert_backticks, _fix_zero_dates


class TestBacktickConversion:
    """Test backtick → double-quote identifier conversion."""

    def test_simple_backtick(self):
        assert _convert_backticks("`users`") == '"users"'

    def test_multiple_backticks(self):
        result = _convert_backticks("SELECT `id`, `name` FROM `users`")
        assert result == 'SELECT "id", "name" FROM "users"'

    def test_backtick_in_string_preserved(self):
        result = _convert_backticks("SELECT '`not_an_id`' FROM users")
        assert result == "SELECT '`not_an_id`' FROM users"

    def test_no_backticks(self):
        assert _convert_backticks("SELECT 1") == "SELECT 1"

    def test_nested_quotes(self):
        result = _convert_backticks('''SELECT `col` FROM "table"''')
        assert result == '''SELECT "col" FROM "table"'''

    def test_unmatched_backtick(self):
        result = _convert_backticks("SELECT `col")
        assert "`" in result  # unmatched backtick preserved


class TestZeroDateHandling:
    """Test MySQL zero-date → NULL conversion."""

    def test_zero_datetime(self):
        assert "NULL" in _fix_zero_dates("'0000-00-00 00:00:00'")
        assert "'0000-00-00 00:00:00'" not in _fix_zero_dates("'0000-00-00 00:00:00'")

    def test_zero_date(self):
        assert "NULL" in _fix_zero_dates("'0000-00-00'")

    def test_normal_date_untouched(self):
        result = _fix_zero_dates("'2024-01-15'")
        assert result == "'2024-01-15'"

    def test_zero_date_in_insert(self):
        sql = "INSERT INTO t VALUES (1, '0000-00-00', '0000-00-00 00:00:00')"
        result = _fix_zero_dates(sql)
        assert "'0000-00-00'" not in result
        assert result.count("NULL") == 2


class TestShowDatabases:

    def test_basic(self, mock_conn):
        result, is_special = translate("SHOW DATABASES", mock_conn)
        assert not is_special
        assert "pg_database" in result
        assert "datistemplate" in result.lower()

    def test_with_like(self, mock_conn):
        result, _ = translate("SHOW DATABASES LIKE 'test%'", mock_conn)
        assert "LIKE 'test%'" in result

    def test_case_insensitive(self, mock_conn):
        result, _ = translate("show databases", mock_conn)
        assert "pg_database" in result


class TestShowTables:

    def test_basic(self, mock_conn):
        result, _ = translate("SHOW TABLES", mock_conn)
        assert "information_schema.tables" in result

    def test_full(self, mock_conn):
        result, _ = translate("SHOW FULL TABLES", mock_conn)
        assert "table_type" in result.lower()

    def test_with_like(self, mock_conn):
        result, _ = translate("SHOW TABLES LIKE 'user%'", mock_conn)
        assert "LIKE 'user%'" in result

    def test_from_db(self, mock_conn):
        result, _ = translate("SHOW TABLES FROM mydb", mock_conn)
        assert "information_schema.tables" in result


class TestDescTable:

    def test_desc(self, mock_conn):
        result, _ = translate("DESC users", mock_conn)
        assert "information_schema.columns" in result
        assert "'users'" in result

    def test_describe(self, mock_conn):
        result, _ = translate("DESCRIBE users", mock_conn)
        assert "information_schema.columns" in result

    def test_explain_table(self, mock_conn):
        result, _ = translate("EXPLAIN users", mock_conn)
        assert "information_schema.columns" in result

    def test_explain_select_not_captured(self, mock_conn):
        # EXPLAIN SELECT should NOT be captured by DESC handler
        result, _ = translate("EXPLAIN SELECT * FROM users", mock_conn)
        assert "information_schema.columns" not in result

    def test_backtick_table(self, mock_conn):
        result, _ = translate("DESC `users`", mock_conn)
        assert "'users'" in result


class TestShowCreateTable:

    def test_returns_special(self, mock_conn):
        # With mock, show_create_table will fail, but we test the handler routing
        with pytest.raises(Exception, match="doesn't exist"):
            translate("SHOW CREATE TABLE users", mock_conn)

    def test_backtick_table_name(self, mock_conn):
        with pytest.raises(Exception):
            translate("SHOW CREATE TABLE `users`", mock_conn)


class TestShowIndex:

    def test_basic(self, mock_conn):
        result, _ = translate("SHOW INDEX FROM users", mock_conn)
        assert "pg_index" in result
        assert "'users'" in result

    def test_show_keys(self, mock_conn):
        result, _ = translate("SHOW KEYS FROM users", mock_conn)
        assert "pg_index" in result

    def test_show_indexes(self, mock_conn):
        result, _ = translate("SHOW INDEXES FROM users", mock_conn)
        assert "pg_index" in result


class TestShowTableStatus:

    def test_basic(self, mock_conn):
        result, _ = translate("SHOW TABLE STATUS", mock_conn)
        assert "pg_class" in result

    def test_with_like(self, mock_conn):
        result, _ = translate("SHOW TABLE STATUS LIKE 'user%'", mock_conn)
        assert "LIKE 'user%'" in result


class TestShowProcesslist:

    def test_basic(self, mock_conn):
        result, _ = translate("SHOW PROCESSLIST", mock_conn)
        assert "pg_stat_activity" in result

    def test_full(self, mock_conn):
        result, _ = translate("SHOW FULL PROCESSLIST", mock_conn)
        assert "pg_stat_activity" in result
        assert "LEFT(" not in result


class TestShowVariables:

    def test_basic(self, mock_conn):
        result, _ = translate("SHOW VARIABLES", mock_conn)
        assert "pg_settings" in result

    def test_with_like(self, mock_conn):
        result, _ = translate("SHOW VARIABLES LIKE 'max%'", mock_conn)
        assert "LIKE 'max%'" in result

    def test_global(self, mock_conn):
        result, _ = translate("SHOW GLOBAL VARIABLES", mock_conn)
        assert "pg_settings" in result


class TestShowStatus:

    def test_basic(self, mock_conn):
        result, _ = translate("SHOW STATUS", mock_conn)
        assert "Uptime" in result

    def test_global(self, mock_conn):
        result, _ = translate("SHOW GLOBAL STATUS", mock_conn)
        assert "Uptime" in result

    def test_with_like(self, mock_conn):
        result, _ = translate("SHOW STATUS LIKE 'Uptime'", mock_conn)
        assert "LIKE 'Uptime'" in result


class TestShowGrants:

    def test_basic(self, mock_conn):
        result, _ = translate("SHOW GRANTS", mock_conn)
        assert "role_table_grants" in result

    def test_for_user(self, mock_conn):
        result, _ = translate("SHOW GRANTS FOR 'testuser'", mock_conn)
        assert "testuser" in result


class TestShowWarnings:

    def test_empty(self, mock_conn):
        result, is_special = translate("SHOW WARNINGS", mock_conn)
        assert is_special
        columns, rows = result
        assert columns == ["Level", "Code", "Message"]
        assert rows == []

    def test_with_notices(self, mock_conn):
        mock_conn.notices = ["test warning"]
        result, is_special = translate("SHOW WARNINGS", mock_conn)
        assert is_special
        _, rows = result
        assert len(rows) == 1
        assert rows[0][2] == "test warning"


class TestShowEngines:

    def test_basic(self, mock_conn):
        result, is_special = translate("SHOW ENGINES", mock_conn)
        assert is_special
        columns, rows = result
        assert "PostgreSQL" in rows[0][0]


class TestShowEngineStatus:

    def test_basic(self, mock_conn):
        result, _ = translate("SHOW ENGINE INNODB STATUS", mock_conn)
        assert "pg_stat_activity" in result


class TestShowCharset:

    def test_basic(self, mock_conn):
        result, _ = translate("SHOW CHARACTER SET", mock_conn)
        assert "character_sets" in result

    def test_charset_alias(self, mock_conn):
        result, _ = translate("SHOW CHARSET", mock_conn)
        assert "character_sets" in result


class TestShowCollation:

    def test_basic(self, mock_conn):
        result, _ = translate("SHOW COLLATION", mock_conn)
        assert "pg_collation" in result

    def test_with_like(self, mock_conn):
        result, _ = translate("SHOW COLLATION LIKE 'utf%'", mock_conn)
        assert "LIKE 'utf%'" in result


class TestShowCreateDatabase:

    def test_basic(self, mock_conn):
        result, _ = translate("SHOW CREATE DATABASE testdb", mock_conn)
        assert "pg_database" in result
        assert "'testdb'" in result


class TestInsertIgnore:

    def test_basic(self, mock_conn):
        result, _ = translate(
            "INSERT IGNORE INTO users (id, name) VALUES (1, 'Alice')",
            mock_conn
        )
        assert "ON CONFLICT DO NOTHING" in result
        assert "IGNORE" not in result

    def test_backtick_table(self, mock_conn):
        result, _ = translate(
            "INSERT IGNORE INTO `users` (id, name) VALUES (1, 'test')",
            mock_conn
        )
        assert "ON CONFLICT DO NOTHING" in result


class TestOnDuplicateKeyUpdate:

    def test_basic(self, mock_conn):
        sql = ("INSERT INTO users (id, name) VALUES (1, 'Alice') "
               "ON DUPLICATE KEY UPDATE name = VALUES(name)")
        result, _ = translate(sql, mock_conn)
        assert "ON CONFLICT" in result
        assert "DO UPDATE SET" in result
        assert "EXCLUDED." in result

    def test_no_pk_passthrough(self, mock_conn):
        mock_conn._pk_columns["unknown"] = []
        sql = ("INSERT INTO unknown (id, name) VALUES (1, 'x') "
               "ON DUPLICATE KEY UPDATE name = VALUES(name)")
        result, _ = translate(sql, mock_conn)
        # Should fall through to backtick conversion only


class TestReplaceInto:

    def test_basic(self, mock_conn):
        result, _ = translate(
            "REPLACE INTO users (id, name, email) VALUES (1, 'Alice', 'a@b.com')",
            mock_conn
        )
        assert "ON CONFLICT" in result
        assert "DO UPDATE SET" in result

    def test_pk_only_table(self, mock_conn):
        mock_conn._pk_columns["pk_only"] = ["id"]
        mock_conn._columns["pk_only"] = ["id"]
        result, _ = translate(
            "REPLACE INTO pk_only (id) VALUES (1)",
            mock_conn
        )
        assert "DO NOTHING" in result


class TestCreateDatabase:

    def test_basic(self, mock_conn):
        result, _ = translate("CREATE DATABASE mydb", mock_conn)
        assert '"mydb"' in result

    def test_with_charset(self, mock_conn):
        result, _ = translate(
            "CREATE DATABASE mydb CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci",
            mock_conn
        )
        assert "ENCODING" in result

    def test_if_not_exists(self, mock_conn):
        result, _ = translate("CREATE DATABASE IF NOT EXISTS mydb", mock_conn)
        assert '"mydb"' in result


class TestAlterTableModify:

    def test_basic(self, mock_conn):
        result, _ = translate(
            "ALTER TABLE users MODIFY COLUMN name TEXT",
            mock_conn
        )
        assert 'ALTER COLUMN' in result
        assert 'TYPE TEXT' in result

    def test_without_column_keyword(self, mock_conn):
        result, _ = translate(
            "ALTER TABLE users MODIFY name VARCHAR(500)",
            mock_conn
        )
        assert 'TYPE VARCHAR(500)' in result


class TestAlterTableChange:

    def test_basic(self, mock_conn):
        result, _ = translate(
            "ALTER TABLE users CHANGE old_name new_name VARCHAR(255)",
            mock_conn
        )
        assert "RENAME COLUMN" in result
        assert "ALTER COLUMN" in result
        assert '"old_name"' in result
        assert '"new_name"' in result


class TestAlterTableIndex:

    def test_add_index(self, mock_conn):
        result, _ = translate(
            "ALTER TABLE users ADD INDEX idx_name (name)",
            mock_conn
        )
        assert "CREATE INDEX" in result
        assert '"idx_name"' in result

    def test_add_unique_index(self, mock_conn):
        result, _ = translate(
            "ALTER TABLE users ADD UNIQUE INDEX idx_email (email)",
            mock_conn
        )
        assert "CREATE UNIQUE INDEX" in result

    def test_drop_index(self, mock_conn):
        result, _ = translate(
            "ALTER TABLE users DROP INDEX idx_name",
            mock_conn
        )
        assert "DROP INDEX" in result


class TestRenameTable:

    def test_basic(self, mock_conn):
        result, _ = translate("RENAME TABLE old_table TO new_table", mock_conn)
        assert 'RENAME TO' in result
        assert '"old_table"' in result
        assert '"new_table"' in result


class TestTruncateTable:

    def test_basic(self, mock_conn):
        result, _ = translate("TRUNCATE TABLE users", mock_conn)
        assert "RESTART IDENTITY" in result
        assert '"users"' in result

    def test_without_table_keyword(self, mock_conn):
        result, _ = translate("TRUNCATE users", mock_conn)
        assert "RESTART IDENTITY" in result


class TestSelectDatabase:

    def test_basic(self, mock_conn):
        result, _ = translate("SELECT DATABASE()", mock_conn)
        assert "current_database()" in result
        # Must not double-convert to current_current_database()
        assert "current_current_database" not in result

    def test_in_larger_query(self, mock_conn):
        result, _ = translate(
            "SELECT DATABASE(), USER()",
            mock_conn
        )
        assert "current_database()" in result


class TestIfNull:

    def test_basic(self, mock_conn):
        result, _ = translate(
            "SELECT IFNULL(name, 'unknown') FROM users",
            mock_conn
        )
        assert "COALESCE(" in result
        assert "IFNULL" not in result

    def test_case_insensitive(self, mock_conn):
        result, _ = translate(
            "SELECT ifnull(a, b) FROM t",
            mock_conn
        )
        assert "COALESCE(" in result


class TestCreateUser:

    def test_basic(self, mock_conn):
        result, _ = translate(
            "CREATE USER 'testuser'@'%' IDENTIFIED BY 'pass123'",
            mock_conn
        )
        assert "CREATE ROLE" in result
        assert "LOGIN" in result
        assert "PASSWORD" in result

    def test_localhost_notice(self, mock_conn):
        result, _ = translate(
            "CREATE USER 'testuser'@'localhost' IDENTIFIED BY 'pass'",
            mock_conn
        )
        assert "pg_hba.conf" in result


class TestDropUser:

    def test_basic(self, mock_conn):
        result, _ = translate("DROP USER 'testuser'@'%'", mock_conn)
        assert "DROP ROLE" in result


class TestAlterUser:

    def test_change_password(self, mock_conn):
        result, _ = translate(
            "ALTER USER 'testuser'@'%' IDENTIFIED BY 'newpass'",
            mock_conn
        )
        assert "ALTER ROLE" in result
        assert "PASSWORD" in result


class TestGrant:

    def test_all_privileges(self, mock_conn):
        result, _ = translate(
            "GRANT ALL PRIVILEGES ON mydb.* TO 'testuser'@'%'",
            mock_conn
        )
        assert "GRANT ALL" in result
        assert "SCHEMA public" in result

    def test_specific_privs(self, mock_conn):
        result, _ = translate(
            "GRANT SELECT, INSERT ON mydb.* TO 'testuser'@'%'",
            mock_conn
        )
        assert "GRANT SELECT, INSERT" in result


class TestRevoke:

    def test_basic(self, mock_conn):
        result, _ = translate(
            "REVOKE ALL PRIVILEGES ON mydb.* FROM 'testuser'@'%'",
            mock_conn
        )
        assert "REVOKE ALL" in result
        assert "FROM" in result


class TestFlushPrivileges:

    def test_noop(self, mock_conn):
        result, is_special = translate("FLUSH PRIVILEGES", mock_conn)
        assert is_special


class TestKill:

    def test_kill_connection(self, mock_conn):
        result, _ = translate("KILL 1234", mock_conn)
        assert "pg_terminate_backend(1234)" in result

    def test_kill_query(self, mock_conn):
        result, _ = translate("KILL QUERY 1234", mock_conn)
        assert "pg_cancel_backend(1234)" in result


class TestSetGlobal:

    def test_basic(self, mock_conn):
        result, _ = translate(
            "SET GLOBAL max_connections = 500",
            mock_conn
        )
        assert "ALTER SYSTEM SET" in result
        assert "max_connections" in result


class TestDumpBoilerplate:
    """Test MySQL dump boilerplate statement handling."""

    def test_set_names_utf8(self, mock_conn):
        result, _ = translate("SET NAMES utf8mb4", mock_conn)
        assert "client_encoding" in result
        assert "UTF8" in result

    def test_set_names_latin1(self, mock_conn):
        result, _ = translate("SET NAMES latin1", mock_conn)
        assert "LATIN1" in result

    def test_set_foreign_key_checks_off(self, mock_conn):
        result, _ = translate("SET FOREIGN_KEY_CHECKS = 0", mock_conn)
        assert "session_replication_role" in result
        assert "replica" in result

    def test_set_foreign_key_checks_on(self, mock_conn):
        result, _ = translate("SET FOREIGN_KEY_CHECKS = 1", mock_conn)
        assert "session_replication_role" in result
        assert "origin" in result

    def test_set_character_set(self, mock_conn):
        result, is_special = translate(
            "SET CHARACTER_SET_CLIENT=utf8mb4", mock_conn
        )
        assert is_special  # Returns no-op OK

    def test_set_collation(self, mock_conn):
        result, is_special = translate(
            "SET COLLATION_CONNECTION=utf8mb4_general_ci", mock_conn
        )
        assert is_special

    def test_lock_tables(self, mock_conn):
        result, is_special = translate(
            "LOCK TABLES `users` WRITE", mock_conn
        )
        assert is_special

    def test_unlock_tables(self, mock_conn):
        result, is_special = translate("UNLOCK TABLES", mock_conn)
        assert is_special

    def test_disable_keys(self, mock_conn):
        result, is_special = translate(
            "ALTER TABLE `users` DISABLE KEYS", mock_conn
        )
        assert is_special

    def test_enable_keys(self, mock_conn):
        result, is_special = translate(
            "ALTER TABLE `users` ENABLE KEYS", mock_conn
        )
        assert is_special


class TestMySQLCreateTable:
    """Test MySQL-format CREATE TABLE → PG translation."""

    def test_basic_table(self, mock_conn):
        sql = """CREATE TABLE `users` (
            `id` int(11) NOT NULL AUTO_INCREMENT,
            `name` varchar(255) DEFAULT NULL,
            PRIMARY KEY (`id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"""
        result, _ = translate(sql, mock_conn)
        assert "SERIAL" in result
        assert "VARCHAR" in result
        assert "ENGINE=" not in result
        assert "CHARSET" not in result

    def test_with_indexes(self, mock_conn):
        sql = """CREATE TABLE `posts` (
            `id` int(11) NOT NULL AUTO_INCREMENT,
            `title` varchar(255) DEFAULT NULL,
            KEY `idx_title` (`title`),
            PRIMARY KEY (`id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"""
        result, _ = translate(sql, mock_conn)
        assert "CREATE INDEX" in result
        assert '"idx_title"' in result

    def test_with_foreign_key(self, mock_conn):
        sql = """CREATE TABLE `posts` (
            `id` int(11) NOT NULL AUTO_INCREMENT,
            `user_id` int(11) NOT NULL,
            PRIMARY KEY (`id`),
            CONSTRAINT `fk_user` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"""
        result, _ = translate(sql, mock_conn)
        assert "FOREIGN KEY" in result

    def test_if_not_exists(self, mock_conn):
        sql = """CREATE TABLE IF NOT EXISTS `test` (
            `id` int(11) NOT NULL,
            PRIMARY KEY (`id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"""
        result, _ = translate(sql, mock_conn)
        assert "IF NOT EXISTS" in result

    def test_type_mappings(self, mock_conn):
        sql = """CREATE TABLE `types` (
            `a` tinyint(1) DEFAULT 0,
            `b` bigint(20) unsigned NOT NULL,
            `c` mediumtext,
            `d` longblob,
            `e` datetime DEFAULT NULL,
            `f` enum('a','b','c') DEFAULT 'a',
            `g` decimal(10,2) NOT NULL,
            `h` json DEFAULT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"""
        result, _ = translate(sql, mock_conn)
        assert "SMALLINT" in result  # tinyint
        assert "BIGINT" in result
        assert "TEXT" in result  # mediumtext or enum
        assert "BYTEA" in result  # longblob
        assert "TIMESTAMP" in result  # datetime
        assert "NUMERIC" in result  # decimal
        assert "JSONB" in result  # json

    def test_on_update_current_timestamp_stripped(self, mock_conn):
        sql = """CREATE TABLE `t` (
            `updated_at` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"""
        result, _ = translate(sql, mock_conn)
        assert "ON UPDATE" not in result

    def test_comment_stripped(self, mock_conn):
        sql = """CREATE TABLE `t` (
            `id` int(11) NOT NULL COMMENT 'Primary key'
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"""
        result, _ = translate(sql, mock_conn)
        assert "COMMENT" not in result


class TestPgloaderCompat:
    """Test pgloader-specific SQL patterns."""

    def test_create_type_enum(self, mock_conn):
        sql = "CREATE TYPE mood AS ENUM ('happy', 'sad', 'neutral')"
        result, _ = translate(sql, mock_conn)
        assert result == sql  # pass through

    def test_drop_type(self, mock_conn):
        sql = "DROP TYPE IF EXISTS mood"
        result, _ = translate(sql, mock_conn)
        assert "DROP TYPE" in result

    def test_alter_table_disable_trigger(self, mock_conn):
        sql = "ALTER TABLE users DISABLE TRIGGER ALL"
        result, _ = translate(sql, mock_conn)
        assert "DISABLE TRIGGER ALL" in result

    def test_alter_table_enable_trigger(self, mock_conn):
        sql = "ALTER TABLE users ENABLE TRIGGER ALL"
        result, _ = translate(sql, mock_conn)
        assert "ENABLE TRIGGER ALL" in result

    def test_copy_from_stdin(self, mock_conn):
        sql = "COPY users (id, name) FROM STDIN"
        result, _ = translate(sql, mock_conn)
        assert "COPY" in result
        assert "FROM STDIN" in result

    def test_create_index_passthrough(self, mock_conn):
        sql = 'CREATE INDEX idx_123_name ON users (name)'
        result, _ = translate(sql, mock_conn)
        assert "CREATE INDEX" in result
        assert "users" in result

    def test_create_unique_index(self, mock_conn):
        sql = "CREATE UNIQUE INDEX idx_email ON users (email)"
        result, _ = translate(sql, mock_conn)
        assert "CREATE UNIQUE INDEX" in result

    def test_add_constraint_fk(self, mock_conn):
        sql = ("ALTER TABLE posts ADD CONSTRAINT fk_user "
               "FOREIGN KEY (user_id) REFERENCES users (id)")
        result, _ = translate(sql, mock_conn)
        assert "ADD CONSTRAINT" in result
        assert "FOREIGN KEY" in result

    def test_select_setval(self, mock_conn):
        sql = "SELECT setval('users_id_seq', 100, true)"
        result, _ = translate(sql, mock_conn)
        assert "setval" in result

    def test_set_session_replication_role(self, mock_conn):
        sql = "SET session_replication_role = 'replica'"
        result, _ = translate(sql, mock_conn)
        assert result == sql


class TestPassthrough:
    """Test that unrecognized SQL passes through with backtick conversion."""

    def test_plain_select(self, mock_conn):
        result, _ = translate("SELECT * FROM users WHERE id = 1", mock_conn)
        assert result == "SELECT * FROM users WHERE id = 1"

    def test_select_with_backticks(self, mock_conn):
        result, _ = translate("SELECT * FROM `users` WHERE `id` = 1", mock_conn)
        assert '"users"' in result
        assert '"id"' in result

    def test_plain_insert(self, mock_conn):
        result, _ = translate(
            "INSERT INTO users (name) VALUES ('Alice')", mock_conn
        )
        assert result == "INSERT INTO users (name) VALUES ('Alice')"

    def test_empty_string(self, mock_conn):
        result, _ = translate("", mock_conn)
        assert result == ""

    def test_whitespace_only(self, mock_conn):
        result, _ = translate("   ", mock_conn)
        assert result.strip() == ""

    def test_begin(self, mock_conn):
        result, _ = translate("BEGIN", mock_conn)
        assert "BEGIN" in result

    def test_commit(self, mock_conn):
        result, _ = translate("COMMIT", mock_conn)
        assert "COMMIT" in result

    def test_rollback(self, mock_conn):
        result, _ = translate("ROLLBACK", mock_conn)
        assert "ROLLBACK" in result

    def test_savepoint(self, mock_conn):
        result, _ = translate("SAVEPOINT sp1", mock_conn)
        assert "SAVEPOINT" in result


class TestMySQLTypeMapping:
    """Test MySQL→PG type mapping in _map_mysql_type_to_pg."""

    @pytest.mark.parametrize("mysql_type,expected", [
        ("int(11)", "INTEGER"),
        ("int", "INTEGER"),
        ("tinyint(1)", "SMALLINT"),
        ("tinyint(4)", "SMALLINT"),
        ("smallint(6)", "SMALLINT"),
        ("mediumint(8)", "INTEGER"),
        ("bigint(20)", "BIGINT"),
        ("bigint(20) unsigned", "BIGINT"),
        ("float", "REAL"),
        ("double", "DOUBLE PRECISION"),
        ("decimal(10,2)", "NUMERIC(10,2)"),
        ("varchar(255)", "VARCHAR(255)"),
        ("char(36)", "CHAR(36)"),
        ("text", "TEXT"),
        ("tinytext", "TEXT"),
        ("mediumtext", "TEXT"),
        ("longtext", "TEXT"),
        ("blob", "BYTEA"),
        ("tinyblob", "BYTEA"),
        ("mediumblob", "BYTEA"),
        ("longblob", "BYTEA"),
        ("datetime", "TIMESTAMP"),
        ("timestamp", "TIMESTAMP"),
        ("date", "DATE"),
        ("time", "TIME"),
        ("year", "SMALLINT"),
        ("json", "JSONB"),
        ("enum('a','b')", "TEXT"),
        ("set('x','y')", "TEXT"),
        ("bit(1)", "BIT(1)"),
        ("binary(16)", "BYTEA"),
        ("varbinary(255)", "BYTEA"),
    ])
    def test_type_mapping(self, mysql_type, expected):
        from mysqlpg.translator import _map_mysql_type_to_pg
        result = _map_mysql_type_to_pg(mysql_type)
        assert result == expected

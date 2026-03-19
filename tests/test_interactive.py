"""Tests for mysqlpg.interactive — REPL, autocomplete, prompt expansion."""

import pytest
from mysqlpg.interactive import (
    MySQLCompleter, expand_prompt, SQL_KEYWORDS,
)


class TestMySQLCompleter:

    def test_init(self):
        c = MySQLCompleter()
        assert len(c.keywords) > 0
        assert c.tables == []
        assert c.columns == {}
        assert c.databases == []

    def test_refresh(self, mock_conn):
        c = MySQLCompleter()
        c.refresh(mock_conn)
        assert "users" in c.tables
        assert "posts" in c.tables
        assert "testdb" in c.databases
        assert "id" in c.columns.get("users", [])

    def test_keyword_completions(self):
        c = MySQLCompleter()
        from prompt_toolkit.document import Document
        doc = Document("SEL", cursor_position=3)
        completions = list(c.get_completions(doc, None))
        texts = [comp.text for comp in completions]
        assert "SELECT" in texts

    def test_table_completions(self, mock_conn):
        c = MySQLCompleter()
        c.refresh(mock_conn)
        from prompt_toolkit.document import Document
        doc = Document("user", cursor_position=4)
        completions = list(c.get_completions(doc, None))
        texts = [comp.text for comp in completions]
        assert "users" in texts

    def test_database_completions(self, mock_conn):
        c = MySQLCompleter()
        c.refresh(mock_conn)
        from prompt_toolkit.document import Document
        doc = Document("test", cursor_position=4)
        completions = list(c.get_completions(doc, None))
        texts = [comp.text for comp in completions]
        assert "testdb" in texts

    def test_column_completions(self, mock_conn):
        c = MySQLCompleter()
        c.refresh(mock_conn)
        from prompt_toolkit.document import Document
        doc = Document("emai", cursor_position=4)
        completions = list(c.get_completions(doc, None))
        texts = [comp.text for comp in completions]
        assert "email" in texts

    def test_empty_input_no_completions(self):
        c = MySQLCompleter()
        from prompt_toolkit.document import Document
        doc = Document("", cursor_position=0)
        completions = list(c.get_completions(doc, None))
        assert completions == []

    def test_no_duplicate_completions(self, mock_conn):
        c = MySQLCompleter()
        c.refresh(mock_conn)
        from prompt_toolkit.document import Document
        doc = Document("SEL", cursor_position=3)
        completions = list(c.get_completions(doc, None))
        texts = [comp.text for comp in completions]
        assert len(texts) == len(set(texts))


class TestExpandPrompt:

    def test_default_prompt(self, mock_conn):
        state = {"database": "testdb"}
        result = expand_prompt(None, mock_conn, state)
        assert "mysql" in result
        assert "testdb" in result

    def test_user_expansion(self, mock_conn):
        result = expand_prompt("\\u> ", mock_conn, {"database": "testdb"})
        assert "postgres" in result

    def test_host_expansion(self, mock_conn):
        result = expand_prompt("\\h> ", mock_conn, {"database": "testdb"})
        assert "localhost" in result

    def test_database_expansion(self, mock_conn):
        result = expand_prompt("\\d> ", mock_conn, {"database": "mydb"})
        assert "mydb" in result

    def test_port_expansion(self, mock_conn):
        result = expand_prompt("\\p> ", mock_conn, {"database": "testdb"})
        assert "5432" in result

    def test_datetime_expansion(self, mock_conn):
        result = expand_prompt("\\D> ", mock_conn, {"database": "testdb"})
        # Should contain a date-like string
        assert "-" in result  # date separator

    def test_no_database(self, mock_conn):
        result = expand_prompt(None, mock_conn, {"database": "(none)"})
        assert "(none)" in result


class TestSQLKeywords:

    def test_keywords_not_empty(self):
        assert len(SQL_KEYWORDS) > 20

    def test_common_keywords_present(self):
        assert "SELECT" in SQL_KEYWORDS
        assert "FROM" in SQL_KEYWORDS
        assert "WHERE" in SQL_KEYWORDS
        assert "INSERT" in SQL_KEYWORDS
        assert "UPDATE" in SQL_KEYWORDS
        assert "DELETE" in SQL_KEYWORDS
        assert "CREATE" in SQL_KEYWORDS
        assert "DROP" in SQL_KEYWORDS
        assert "ALTER" in SQL_KEYWORDS

    def test_mysql_specific_present(self):
        assert "SHOW" in SQL_KEYWORDS
        assert "DESCRIBE" in SQL_KEYWORDS
        assert "USE" in SQL_KEYWORDS

    def test_show_variants_present(self):
        assert "SHOW DATABASES" in SQL_KEYWORDS
        assert "SHOW TABLES" in SQL_KEYWORDS
        assert "SHOW CREATE TABLE" in SQL_KEYWORDS

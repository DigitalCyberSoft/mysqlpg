"""Tests for DML translation — MySQL/MariaDB → PostgreSQL.

Covers: NULL-safe equality, FROM DUAL, SELECT modifiers, CAST conversions,
CONVERT, FIELD, ELT, FIND_IN_SET, SOUNDS LIKE, INSERT SET syntax,
DO statement, index hints, WITH ROLLUP, STRCMP, MINUS→EXCEPT,
and various edge cases from the MySQL/MariaDB DML reference.
"""

import re
import pytest
from mysqlpg.translator import translate, _translate_functions


# ---------- NULL-safe equality ----------

class TestNullSafeEquality:

    def test_null_safe_equals(self, mock_conn):
        result, _ = translate("SELECT * FROM t WHERE col1 <=> col2", mock_conn)
        assert "IS NOT DISTINCT FROM" in result
        assert "<=>" not in result

    def test_null_safe_equals_null(self, mock_conn):
        result, _ = translate("SELECT * FROM t WHERE col <=> NULL", mock_conn)
        assert "IS NOT DISTINCT FROM" in result

    def test_null_safe_in_join(self, mock_conn):
        result, _ = translate(
            "SELECT * FROM t1 JOIN t2 ON t1.a <=> t2.a",
            mock_conn
        )
        assert "IS NOT DISTINCT FROM" in result


# ---------- FROM DUAL ----------

class TestFromDual:

    def test_select_from_dual(self, mock_conn):
        result, _ = translate("SELECT 1 + 1 FROM DUAL", mock_conn)
        assert "DUAL" not in result
        assert "SELECT 1 + 1" in result

    def test_select_now_from_dual(self, mock_conn):
        result, _ = translate("SELECT NOW() FROM DUAL", mock_conn)
        assert "DUAL" not in result


# ---------- SELECT modifiers ----------

class TestSelectModifiers:

    def test_sql_no_cache(self, mock_conn):
        result, _ = translate("SELECT SQL_NO_CACHE * FROM users", mock_conn)
        assert "SQL_NO_CACHE" not in result
        assert "SELECT" in result

    def test_sql_cache(self, mock_conn):
        result, _ = translate("SELECT SQL_CACHE * FROM users", mock_conn)
        assert "SQL_CACHE" not in result

    def test_sql_buffer_result(self, mock_conn):
        result, _ = translate("SELECT SQL_BUFFER_RESULT * FROM large_table", mock_conn)
        assert "SQL_BUFFER_RESULT" not in result

    def test_sql_small_result(self, mock_conn):
        result, _ = translate(
            "SELECT SQL_SMALL_RESULT col, COUNT(*) FROM t GROUP BY col",
            mock_conn
        )
        assert "SQL_SMALL_RESULT" not in result
        assert "GROUP BY" in result

    def test_sql_big_result(self, mock_conn):
        result, _ = translate(
            "SELECT SQL_BIG_RESULT col, COUNT(*) FROM t GROUP BY col",
            mock_conn
        )
        assert "SQL_BIG_RESULT" not in result

    def test_high_priority(self, mock_conn):
        result, _ = translate("SELECT HIGH_PRIORITY * FROM t", mock_conn)
        assert "HIGH_PRIORITY" not in result
        assert "SELECT" in result


# ---------- CAST translations ----------

class TestCastTranslation:

    def test_cast_unsigned(self, mock_conn):
        result, _ = translate("SELECT CAST(col AS UNSIGNED) FROM t", mock_conn)
        assert "BIGINT" in result
        assert "UNSIGNED" not in result

    def test_cast_unsigned_integer(self, mock_conn):
        result, _ = translate("SELECT CAST(col AS UNSIGNED INTEGER) FROM t", mock_conn)
        assert "BIGINT" in result

    def test_cast_signed(self, mock_conn):
        result, _ = translate("SELECT CAST(col AS SIGNED) FROM t", mock_conn)
        assert "INTEGER" in result
        assert "SIGNED" not in result

    def test_cast_signed_integer(self, mock_conn):
        result, _ = translate("SELECT CAST(col AS SIGNED INTEGER) FROM t", mock_conn)
        assert "INTEGER" in result

    def test_cast_char_no_length(self, mock_conn):
        result, _ = translate("SELECT CAST(col AS CHAR) FROM t", mock_conn)
        assert "TEXT" in result

    def test_cast_datetime(self, mock_conn):
        result, _ = translate("SELECT CAST(col AS DATETIME) FROM t", mock_conn)
        assert "TIMESTAMP" in result
        assert "DATETIME" not in result

    def test_cast_with_length_preserved(self, mock_conn):
        # CAST(x AS CHAR(100)) should be preserved (PG supports it)
        result, _ = translate("SELECT CAST(col AS CHAR(100)) FROM t", mock_conn)
        assert "CHAR(100)" in result


# ---------- CONVERT ----------

class TestConvert:

    def test_convert_unsigned(self, mock_conn):
        result, _ = translate("SELECT CONVERT(col, UNSIGNED) FROM t", mock_conn)
        assert "CAST" in result
        assert "BIGINT" in result

    def test_convert_signed(self, mock_conn):
        result, _ = translate("SELECT CONVERT(col, SIGNED) FROM t", mock_conn)
        assert "CAST" in result
        assert "INTEGER" in result

    def test_convert_char(self, mock_conn):
        result, _ = translate("SELECT CONVERT(col, CHAR) FROM t", mock_conn)
        assert "CAST" in result
        assert "TEXT" in result

    def test_convert_using_charset(self, mock_conn):
        result, _ = translate("SELECT CONVERT(col USING utf8mb4) FROM t", mock_conn)
        assert "USING" not in result
        assert "CONVERT" not in result
        # Should just pass through the expression
        assert "col" in result


# ---------- FIELD / ELT / FIND_IN_SET ----------

class TestFieldFunction:

    def test_field_basic(self, mock_conn):
        result, _ = translate(
            "SELECT * FROM t ORDER BY FIELD(status, 'active', 'pending', 'closed')",
            mock_conn
        )
        assert "ARRAY_POSITION" in result
        assert "ARRAY[" in result
        assert "'active'" in result
        assert "'pending'" in result
        assert "'closed'" in result

    def test_field_in_where(self, mock_conn):
        result, _ = translate(
            "SELECT * FROM t WHERE FIELD(status, 'a', 'b') > 0",
            mock_conn
        )
        assert "ARRAY_POSITION" in result


class TestEltFunction:

    def test_elt_basic(self, mock_conn):
        result, _ = translate("SELECT ELT(2, 'a', 'b', 'c')", mock_conn)
        assert "ARRAY[" in result
        assert "'a'" in result
        assert "'b'" in result
        assert "'c'" in result
        assert "[2]" in result


class TestFindInSet:

    def test_find_in_set(self, mock_conn):
        result, _ = translate(
            "SELECT * FROM t WHERE FIND_IN_SET('admin', roles)",
            mock_conn
        )
        assert "ANY" in result
        assert "string_to_array" in result


# ---------- SOUNDS LIKE ----------

class TestSoundsLike:

    def test_sounds_like(self, mock_conn):
        result, _ = translate(
            "SELECT * FROM t WHERE name SOUNDS LIKE 'Smith'",
            mock_conn
        )
        assert "SOUNDEX" in result


# ---------- INSERT ... SET syntax ----------

class TestInsertSetSyntax:

    def test_basic(self, mock_conn):
        result, _ = translate(
            "INSERT INTO users SET name = 'Alice', email = 'alice@test.com'",
            mock_conn
        )
        assert "INSERT INTO" in result
        assert "VALUES" in result
        assert "'Alice'" in result
        assert "'alice@test.com'" in result

    def test_with_expression(self, mock_conn):
        result, _ = translate(
            "INSERT INTO users SET name = 'Bob', created_at = NOW()",
            mock_conn
        )
        assert "VALUES" in result
        assert "NOW()" in result

    def test_with_backticks(self, mock_conn):
        result, _ = translate(
            "INSERT INTO `users` SET `name` = 'test', `active` = 1",
            mock_conn
        )
        assert "VALUES" in result


# ---------- DO statement ----------

class TestDoStatement:

    def test_do_sleep(self, mock_conn):
        result, _ = translate("DO SLEEP(5)", mock_conn)
        assert "pg_sleep" in result

    def test_do_expression(self, mock_conn):
        result, _ = translate("DO 1 + 1", mock_conn)
        assert "SELECT" in result


# ---------- Index hints ----------

class TestIndexHints:

    def test_use_index_stripped(self, mock_conn):
        result, _ = translate(
            "SELECT * FROM t USE INDEX (idx_name) WHERE col = 1",
            mock_conn
        )
        assert "USE INDEX" not in result
        assert "idx_name" not in result
        assert "WHERE" in result

    def test_force_index_stripped(self, mock_conn):
        result, _ = translate(
            "SELECT * FROM t FORCE INDEX (idx_name) WHERE col = 1",
            mock_conn
        )
        assert "FORCE INDEX" not in result

    def test_ignore_index_stripped(self, mock_conn):
        result, _ = translate(
            "SELECT * FROM t IGNORE INDEX (idx_a, idx_b) WHERE col = 1",
            mock_conn
        )
        assert "IGNORE INDEX" not in result

    def test_use_index_for_order_by(self, mock_conn):
        result, _ = translate(
            "SELECT * FROM t USE INDEX FOR ORDER BY (idx_name) WHERE col = 1",
            mock_conn
        )
        assert "USE INDEX" not in result


# ---------- WITH ROLLUP ----------

class TestWithRollup:

    def test_basic_rollup(self, mock_conn):
        result, _ = translate(
            "SELECT dept, SUM(salary) FROM t GROUP BY dept WITH ROLLUP",
            mock_conn
        )
        assert "ROLLUP(dept)" in result
        assert "WITH ROLLUP" not in result

    def test_multi_column_rollup(self, mock_conn):
        result, _ = translate(
            "SELECT dept, role, SUM(salary) FROM t GROUP BY dept, role WITH ROLLUP",
            mock_conn
        )
        assert "ROLLUP(dept, role)" in result


# ---------- STRCMP ----------

class TestStrcmp:

    def test_strcmp(self, mock_conn):
        result, _ = translate("SELECT STRCMP('a', 'b') FROM t", mock_conn)
        assert "CASE WHEN" in result
        assert "-1" in result
        assert "1" in result
        assert "0" in result


# ---------- MINUS → EXCEPT ----------

class TestMinusExcept:

    def test_minus_to_except(self, mock_conn):
        result, _ = translate(
            "SELECT a FROM t1 MINUS SELECT a FROM t2",
            mock_conn
        )
        assert "EXCEPT" in result
        assert "MINUS" not in result


# ---------- Combined / Edge Cases ----------

class TestDMLEdgeCases:

    def test_null_safe_with_cast(self, mock_conn):
        result, _ = translate(
            "SELECT * FROM t WHERE CAST(col AS UNSIGNED) <=> 0",
            mock_conn
        )
        assert "IS NOT DISTINCT FROM" in result
        assert "BIGINT" in result

    def test_select_modifiers_combined(self, mock_conn):
        result, _ = translate(
            "SELECT SQL_NO_CACHE SQL_CALC_FOUND_ROWS * FROM t LIMIT 10",
            mock_conn
        )
        assert "SQL_NO_CACHE" not in result
        # SQL_CALC_FOUND_ROWS may appear in a comment but not as a keyword
        assert "LIMIT 10" in result
        # Verify the keyword itself was stripped (comment is OK)
        assert re.search(r'\bSQL_CALC_FOUND_ROWS\b(?![^/]*\*/)', result) is None

    def test_field_with_from_dual(self, mock_conn):
        result, _ = translate(
            "SELECT FIELD('b', 'a', 'b', 'c') FROM DUAL",
            mock_conn
        )
        assert "ARRAY_POSITION" in result
        assert "DUAL" not in result

    def test_convert_using_with_concat(self, mock_conn):
        result, _ = translate(
            "SELECT CONCAT(CONVERT(name USING utf8mb4), ' ', email) FROM t",
            mock_conn
        )
        assert "CONVERT" not in result
        assert "USING" not in result

    def test_insert_set_with_function(self, mock_conn):
        result, _ = translate(
            "INSERT INTO t SET col = CURDATE(), col2 = RAND()",
            mock_conn
        )
        assert "VALUES" in result
        assert "CURRENT_DATE" in result
        assert "RANDOM()" in result

    def test_multiple_rollup_functions(self, mock_conn):
        result, _ = translate(
            "SELECT dept, COUNT(*), SUM(salary) FROM t GROUP BY dept WITH ROLLUP",
            mock_conn
        )
        assert "ROLLUP" in result
        assert "COUNT(*)" in result
        assert "SUM(salary)" in result

    def test_select_with_index_hint_and_regexp(self, mock_conn):
        result, _ = translate(
            "SELECT * FROM t USE INDEX (idx_name) WHERE col REGEXP '^test'",
            mock_conn
        )
        assert "USE INDEX" not in result
        assert "~*" in result

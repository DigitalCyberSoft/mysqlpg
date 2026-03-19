"""Tests for MySQL function → PostgreSQL function translation.

Covers: string functions, numeric functions, date/time functions,
conditional functions, information functions, regex operators,
query modifiers, and LIMIT syntax conversion.
"""

import pytest
from mysqlpg.translator import translate, _translate_functions, _convert_date_format


# ---------- String Functions ----------

class TestGroupConcat:

    def test_basic(self, mock_conn):
        result, _ = translate("SELECT GROUP_CONCAT(name) FROM users", mock_conn)
        assert "STRING_AGG" in result
        assert "::text" in result

    def test_with_separator(self, mock_conn):
        result, _ = translate(
            "SELECT GROUP_CONCAT(name SEPARATOR '; ') FROM users", mock_conn
        )
        assert "STRING_AGG" in result
        assert "'; '" in result

    def test_with_order_by(self, mock_conn):
        result, _ = translate(
            "SELECT GROUP_CONCAT(name ORDER BY name ASC) FROM users", mock_conn
        )
        assert "STRING_AGG" in result
        assert "ORDER BY" in result

    def test_with_distinct(self, mock_conn):
        result, _ = translate(
            "SELECT GROUP_CONCAT(DISTINCT name) FROM users", mock_conn
        )
        assert "STRING_AGG" in result
        assert "DISTINCT" in result

    def test_full_syntax(self, mock_conn):
        result, _ = translate(
            "SELECT GROUP_CONCAT(DISTINCT name ORDER BY name SEPARATOR '|') FROM users",
            mock_conn
        )
        assert "STRING_AGG" in result
        assert "DISTINCT" in result
        assert "'|'" in result


class TestLocateInstr:

    def test_locate(self, mock_conn):
        result, _ = translate("SELECT LOCATE('abc', name) FROM users", mock_conn)
        assert "POSITION" in result
        assert " IN " in result

    def test_instr(self, mock_conn):
        result, _ = translate("SELECT INSTR(name, 'abc') FROM users", mock_conn)
        assert "POSITION" in result
        assert " IN " in result


class TestCharChr:

    def test_char_function(self, mock_conn):
        result, _ = translate("SELECT CHAR(65)", mock_conn)
        assert "CHR(65)" in result


class TestSpace:

    def test_space(self, mock_conn):
        result, _ = translate("SELECT SPACE(10)", mock_conn)
        assert "REPEAT(' ', 10)" in result


class TestHexUnhex:

    def test_hex(self, mock_conn):
        result, _ = translate("SELECT HEX(name) FROM users", mock_conn)
        assert "ENCODE" in result
        assert "::bytea" in result
        assert "'hex'" in result

    def test_unhex(self, mock_conn):
        result, _ = translate("SELECT UNHEX('48656C6C6F')", mock_conn)
        assert "DECODE" in result
        assert "'hex'" in result


# ---------- Numeric Functions ----------

class TestRand:

    def test_rand(self, mock_conn):
        result, _ = translate("SELECT RAND()", mock_conn)
        assert "RANDOM()" in result

    def test_rand_in_query(self, mock_conn):
        result, _ = translate("SELECT * FROM users ORDER BY RAND()", mock_conn)
        assert "RANDOM()" in result


class TestTruncateFunc:

    def test_truncate_number(self, mock_conn):
        result, _ = translate("SELECT TRUNCATE(3.14159, 2)", mock_conn)
        assert "TRUNC(3.14159, 2)" in result


class TestLogFunctions:

    def test_log_single_arg(self, mock_conn):
        result, _ = translate("SELECT LOG(10)", mock_conn)
        assert "LN(10)" in result

    def test_log_two_args(self, mock_conn):
        result, _ = translate("SELECT LOG(2, 8)", mock_conn)
        assert "LOG(2, 8)" in result

    def test_log2(self, mock_conn):
        result, _ = translate("SELECT LOG2(8)", mock_conn)
        assert "LOG(2, 8)" in result

    def test_log10(self, mock_conn):
        result, _ = translate("SELECT LOG10(100)", mock_conn)
        assert "LOG(10, 100)" in result


# ---------- Date/Time Functions ----------

class TestDateFormat:

    def test_basic(self, mock_conn):
        result, _ = translate(
            "SELECT DATE_FORMAT(created_at, '%Y-%m-%d %H:%i:%s') FROM events",
            mock_conn
        )
        assert "TO_CHAR" in result
        assert "YYYY-MM-DD HH24:MI:SS" in result

    def test_short_format(self, mock_conn):
        result, _ = translate(
            "SELECT DATE_FORMAT(col, '%Y/%m/%d') FROM t",
            mock_conn
        )
        assert "TO_CHAR" in result
        assert "YYYY/MM/DD" in result


class TestStrToDate:

    def test_basic(self, mock_conn):
        result, _ = translate(
            "SELECT STR_TO_DATE('2024-01-15', '%Y-%m-%d')",
            mock_conn
        )
        assert "TO_TIMESTAMP" in result
        assert "YYYY-MM-DD" in result


class TestCurdateCurtime:

    def test_curdate(self, mock_conn):
        result, _ = translate("SELECT CURDATE()", mock_conn)
        assert "CURRENT_DATE" in result

    def test_curtime(self, mock_conn):
        result, _ = translate("SELECT CURTIME()", mock_conn)
        assert "CURRENT_TIME" in result


class TestSysdate:

    def test_sysdate(self, mock_conn):
        result, _ = translate("SELECT SYSDATE()", mock_conn)
        assert "CLOCK_TIMESTAMP()" in result


class TestUnixTimestamp:

    def test_no_arg(self, mock_conn):
        result, _ = translate("SELECT UNIX_TIMESTAMP()", mock_conn)
        assert "EXTRACT(EPOCH FROM NOW())" in result

    def test_with_arg(self, mock_conn):
        result, _ = translate("SELECT UNIX_TIMESTAMP(created_at) FROM events", mock_conn)
        assert "EXTRACT(EPOCH FROM created_at)" in result


class TestFromUnixtime:

    def test_basic(self, mock_conn):
        result, _ = translate("SELECT FROM_UNIXTIME(1700000000)", mock_conn)
        assert "TO_TIMESTAMP(1700000000)" in result


class TestDatediff:

    def test_basic(self, mock_conn):
        result, _ = translate("SELECT DATEDIFF(end_date, start_date) FROM events", mock_conn)
        assert "::date" in result
        assert "-" in result


class TestDateAddSub:

    def test_date_add(self, mock_conn):
        result, _ = translate(
            "SELECT DATE_ADD(created_at, INTERVAL 7 DAY) FROM events",
            mock_conn
        )
        assert "+" in result
        assert "INTERVAL" in result

    def test_date_sub(self, mock_conn):
        result, _ = translate(
            "SELECT DATE_SUB(created_at, INTERVAL 30 DAY) FROM events",
            mock_conn
        )
        assert "-" in result
        assert "INTERVAL" in result


class TestDateExtractors:

    def test_year(self, mock_conn):
        result, _ = translate("SELECT YEAR(created_at) FROM events", mock_conn)
        assert "EXTRACT(YEAR FROM created_at)" in result

    def test_month(self, mock_conn):
        result, _ = translate("SELECT MONTH(created_at) FROM events", mock_conn)
        assert "EXTRACT(MONTH FROM created_at)" in result

    def test_day(self, mock_conn):
        result, _ = translate("SELECT DAY(created_at) FROM events", mock_conn)
        assert "EXTRACT(DAY FROM created_at)" in result

    def test_hour(self, mock_conn):
        result, _ = translate("SELECT HOUR(created_at) FROM events", mock_conn)
        assert "EXTRACT(HOUR FROM created_at)" in result

    def test_minute(self, mock_conn):
        result, _ = translate("SELECT MINUTE(created_at) FROM events", mock_conn)
        assert "EXTRACT(MINUTE FROM created_at)" in result

    def test_second(self, mock_conn):
        result, _ = translate("SELECT SECOND(created_at) FROM events", mock_conn)
        assert "EXTRACT(SECOND FROM created_at)" in result

    def test_dayofweek(self, mock_conn):
        result, _ = translate("SELECT DAYOFWEEK(created_at) FROM events", mock_conn)
        assert "EXTRACT(DOW FROM" in result

    def test_dayofmonth(self, mock_conn):
        result, _ = translate("SELECT DAYOFMONTH(created_at) FROM events", mock_conn)
        assert "EXTRACT(DAY FROM" in result

    def test_dayofyear(self, mock_conn):
        result, _ = translate("SELECT DAYOFYEAR(created_at) FROM events", mock_conn)
        assert "EXTRACT(DOY FROM" in result

    def test_week(self, mock_conn):
        result, _ = translate("SELECT WEEK(created_at) FROM events", mock_conn)
        assert "EXTRACT(WEEK FROM" in result

    def test_weekofyear(self, mock_conn):
        result, _ = translate("SELECT WEEKOFYEAR(created_at) FROM events", mock_conn)
        assert "EXTRACT(WEEK FROM" in result


class TestLastDay:

    def test_basic(self, mock_conn):
        result, _ = translate("SELECT LAST_DAY(created_at) FROM events", mock_conn)
        assert "DATE_TRUNC" in result
        assert "INTERVAL '1 month'" in result


class TestDateTimeCast:

    def test_date_cast(self, mock_conn):
        result, _ = translate("SELECT DATE(created_at) FROM events", mock_conn)
        assert "::date" in result

    def test_time_cast(self, mock_conn):
        result, _ = translate("SELECT TIME(created_at) FROM events", mock_conn)
        assert "::time" in result


# ---------- Conditional Functions ----------

class TestIfFunction:

    def test_basic(self, mock_conn):
        result, _ = translate(
            "SELECT IF(active, 'yes', 'no') FROM users",
            mock_conn
        )
        assert "CASE WHEN" in result
        assert "THEN" in result
        assert "ELSE" in result
        assert "END" in result

    def test_with_comparison(self, mock_conn):
        result, _ = translate(
            "SELECT IF(status = 1, 'active', 'inactive') FROM users",
            mock_conn
        )
        assert "CASE WHEN" in result


class TestIsNull:

    def test_isnull_function(self, mock_conn):
        result, _ = translate("SELECT ISNULL(name) FROM users", mock_conn)
        assert "IS NULL" in result


# ---------- Information Functions ----------

class TestUserFunction:

    def test_user(self, mock_conn):
        result, _ = translate("SELECT USER()", mock_conn)
        assert "current_user" in result

    def test_version(self, mock_conn):
        result, _ = translate("SELECT VERSION()", mock_conn)
        assert "version()" in result

    def test_last_insert_id(self, mock_conn):
        result, _ = translate("SELECT LAST_INSERT_ID()", mock_conn)
        assert "lastval()" in result


class TestFoundRows:

    def test_found_rows(self, mock_conn):
        result, _ = translate("SELECT FOUND_ROWS()", mock_conn)
        assert "FOUND_ROWS" not in result or "not supported" in result

    def test_sql_calc_found_rows(self, mock_conn):
        result, _ = translate(
            "SELECT SQL_CALC_FOUND_ROWS * FROM users LIMIT 10",
            mock_conn
        )
        assert "SQL_CALC_FOUND_ROWS" not in result or "removed" in result


# ---------- Regex Operators ----------

class TestRegexp:

    def test_regexp(self, mock_conn):
        result, _ = translate(
            "SELECT * FROM users WHERE name REGEXP '^A'",
            mock_conn
        )
        assert "~*" in result
        assert "REGEXP" not in result

    def test_rlike(self, mock_conn):
        result, _ = translate(
            "SELECT * FROM users WHERE name RLIKE 'pattern'",
            mock_conn
        )
        assert "~*" in result

    def test_not_regexp(self, mock_conn):
        result, _ = translate(
            "SELECT * FROM users WHERE name NOT REGEXP 'pattern'",
            mock_conn
        )
        assert "!~*" in result


# ---------- Query Modifiers ----------

class TestLimitCommaForm:

    def test_comma_limit(self, mock_conn):
        result, _ = translate(
            "SELECT * FROM users LIMIT 20, 10",
            mock_conn
        )
        assert "LIMIT 10 OFFSET 20" in result

    def test_standard_limit_offset(self, mock_conn):
        result, _ = translate(
            "SELECT * FROM users LIMIT 10 OFFSET 20",
            mock_conn
        )
        assert "LIMIT 10 OFFSET 20" in result


class TestInsertModifiers:

    def test_low_priority_stripped(self, mock_conn):
        result, _ = translate(
            "INSERT LOW_PRIORITY INTO users (name) VALUES ('test')",
            mock_conn
        )
        assert "LOW_PRIORITY" not in result
        assert "INSERT" in result

    def test_delayed_stripped(self, mock_conn):
        result, _ = translate(
            "INSERT DELAYED INTO users (name) VALUES ('test')",
            mock_conn
        )
        assert "DELAYED" not in result

    def test_high_priority_stripped(self, mock_conn):
        result, _ = translate(
            "INSERT HIGH_PRIORITY INTO users (name) VALUES ('test')",
            mock_conn
        )
        assert "HIGH_PRIORITY" not in result


class TestLockInShareMode:

    def test_lock_in_share_mode(self, mock_conn):
        result, _ = translate(
            "SELECT * FROM users WHERE id = 1 LOCK IN SHARE MODE",
            mock_conn
        )
        assert "FOR SHARE" in result
        assert "LOCK IN SHARE MODE" not in result


class TestStraightJoin:

    def test_straight_join(self, mock_conn):
        result, _ = translate(
            "SELECT * FROM t1 STRAIGHT_JOIN t2 ON t1.id = t2.t1_id",
            mock_conn
        )
        assert "STRAIGHT_JOIN" not in result
        assert "JOIN" in result


# ---------- Statement-Level Translations ----------

class TestUpdateJoin:

    def test_basic(self, mock_conn):
        sql = ("UPDATE t1 JOIN t2 ON t1.id = t2.t1_id "
               "SET t1.col = t2.col WHERE t2.flag = 1")
        result, _ = translate(sql, mock_conn)
        assert "FROM" in result
        assert "UPDATE" in result
        assert "JOIN" not in result or "FROM" in result

    def test_inner_join(self, mock_conn):
        sql = ("UPDATE orders INNER JOIN users ON orders.user_id = users.id "
               "SET orders.status = 'active' WHERE users.active = true")
        result, _ = translate(sql, mock_conn)
        assert "FROM" in result


class TestDeleteJoin:

    def test_basic(self, mock_conn):
        sql = ("DELETE t1 FROM t1 JOIN t2 ON t1.id = t2.t1_id "
               "WHERE t2.flag = 1")
        result, _ = translate(sql, mock_conn)
        assert "USING" in result
        assert "DELETE FROM" in result


class TestLoadDataInfile:

    def test_basic(self, mock_conn):
        sql = "LOAD DATA INFILE '/tmp/data.csv' INTO TABLE users"
        result, _ = translate(sql, mock_conn)
        assert "COPY" in result
        assert "FROM" in result

    def test_local(self, mock_conn):
        sql = "LOAD DATA LOCAL INFILE '/tmp/data.csv' INTO TABLE users"
        result, _ = translate(sql, mock_conn)
        assert "COPY" in result


class TestSetSqlMode:

    def test_set_sql_mode(self, mock_conn):
        _, is_special = translate("SET sql_mode = 'STRICT_TRANS_TABLES'", mock_conn)
        assert is_special  # treated as no-op

    def test_set_session_sql_mode(self, mock_conn):
        _, is_special = translate("SET SESSION sql_mode = ''", mock_conn)
        assert is_special

    def test_set_session_variable(self, mock_conn):
        _, is_special = translate("SET @@session.wait_timeout = 28800", mock_conn)
        assert is_special


# ---------- Date Format Conversion ----------

class TestDateFormatConversion:

    def test_full_datetime(self):
        result = _convert_date_format('%Y-%m-%d %H:%i:%s')
        assert result == 'YYYY-MM-DD HH24:MI:SS'

    def test_date_only(self):
        result = _convert_date_format('%Y-%m-%d')
        assert result == 'YYYY-MM-DD'

    def test_month_name(self):
        result = _convert_date_format('%M %d, %Y')
        assert 'Month' in result
        assert 'YYYY' in result

    def test_abbreviated(self):
        result = _convert_date_format('%b %e, %Y')
        assert 'Mon' in result
        assert 'FMDD' in result

    def test_time_12h(self):
        result = _convert_date_format('%h:%i %p')
        assert 'HH12' in result
        assert 'AM' in result

    def test_microseconds(self):
        result = _convert_date_format('%H:%i:%s.%f')
        assert 'US' in result


# ---------- Combined / Complex Queries ----------

class TestCombinedTranslations:
    """Test that multiple translations work together in a single query."""

    def test_multiple_functions(self, mock_conn):
        result, _ = translate(
            "SELECT IFNULL(name, 'unknown'), RAND(), CURDATE() FROM users",
            mock_conn
        )
        assert "COALESCE" in result
        assert "RANDOM()" in result
        assert "CURRENT_DATE" in result

    def test_date_functions_in_where(self, mock_conn):
        result, _ = translate(
            "SELECT * FROM events WHERE YEAR(created_at) = 2024 AND MONTH(created_at) = 1",
            mock_conn
        )
        assert "EXTRACT(YEAR FROM" in result
        assert "EXTRACT(MONTH FROM" in result

    def test_function_with_backticks(self, mock_conn):
        result, _ = translate(
            "SELECT GROUP_CONCAT(`name` SEPARATOR ',') FROM `users`",
            mock_conn
        )
        assert "STRING_AGG" in result
        assert '"name"' in result
        assert '"users"' in result

    def test_limit_with_regexp(self, mock_conn):
        result, _ = translate(
            "SELECT * FROM users WHERE name REGEXP '^A' LIMIT 20, 10",
            mock_conn
        )
        assert "~*" in result
        assert "LIMIT 10 OFFSET 20" in result

    def test_insert_with_functions(self, mock_conn):
        result, _ = translate(
            "INSERT INTO events (created, random_val) "
            "VALUES (CURDATE(), RAND())",
            mock_conn
        )
        assert "CURRENT_DATE" in result
        assert "RANDOM()" in result

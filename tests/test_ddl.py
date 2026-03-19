"""Tests for mysqlpg.ddl — DDL reconstruction and type mapping."""

import pytest
from mysqlpg.ddl import (
    PG_TO_MYSQL_TYPE, map_pg_type_to_mysql, clean_default, get_enum_values,
)


class TestPgToMySQLTypeMapping:
    """Test PostgreSQL → MySQL type mapping dictionary coverage."""

    @pytest.mark.parametrize("pg_type,expected", [
        ("integer", "int(11)"),
        ("int", "int(11)"),
        ("int4", "int(11)"),
        ("smallint", "smallint(6)"),
        ("int2", "smallint(6)"),
        ("bigint", "bigint(20)"),
        ("int8", "bigint(20)"),
        ("serial", "int(11)"),
        ("bigserial", "bigint(20)"),
        ("smallserial", "smallint(6)"),
        ("boolean", "tinyint(1)"),
        ("bool", "tinyint(1)"),
        ("real", "float"),
        ("float4", "float"),
        ("double precision", "double"),
        ("float8", "double"),
        ("text", "text"),
        ("bytea", "blob"),
        ("timestamp without time zone", "datetime"),
        ("timestamp with time zone", "datetime"),
        ("date", "date"),
        ("time without time zone", "time"),
        ("time with time zone", "time"),
        ("json", "json"),
        ("jsonb", "json"),
        ("uuid", "char(36)"),
        ("xml", "text"),
        ("inet", "varchar(45)"),
        ("cidr", "varchar(45)"),
        ("macaddr", "varchar(17)"),
        ("tsvector", "text"),
        ("tsquery", "text"),
        ("money", "decimal(19,2)"),
        ("interval", "varchar(255)"),
        ("oid", "int(11) unsigned"),
        ("name", "varchar(64)"),
    ])
    def test_direct_mapping(self, pg_type, expected):
        assert PG_TO_MYSQL_TYPE[pg_type] == expected


class TestMapPgTypeToMySQL:
    """Test the map_pg_type_to_mysql function with various inputs."""

    def test_varchar_with_length(self):
        assert map_pg_type_to_mysql("character varying", 255) == "varchar(255)"

    def test_varchar_no_length(self):
        assert map_pg_type_to_mysql("character varying") == "text"

    def test_char_with_length(self):
        assert map_pg_type_to_mysql("character", 36) == "char(36)"

    def test_char_no_length(self):
        assert map_pg_type_to_mysql("character") == "char(1)"

    def test_numeric_with_precision(self):
        result = map_pg_type_to_mysql("numeric", numeric_precision=10, numeric_scale=2)
        assert result == "decimal(10,2)"

    def test_numeric_no_precision(self):
        result = map_pg_type_to_mysql("numeric")
        assert result == "decimal(10,0)"

    def test_bit_with_length(self):
        result = map_pg_type_to_mysql("bit", character_maximum_length=8)
        assert result == "bit(8)"

    def test_bit_no_length(self):
        result = map_pg_type_to_mysql("bit")
        assert result == "bit(1)"

    def test_array_type(self):
        result = map_pg_type_to_mysql("ARRAY", udt_name="_int4")
        assert "array" in result.lower()

    def test_user_defined_type(self):
        result = map_pg_type_to_mysql("USER-DEFINED", udt_name="my_enum")
        assert "varchar(255)" in result or "enum" in result.lower()

    def test_user_defined_enum_with_conn(self, mock_conn):
        # Without actual ENUM detection (mock doesn't have pg_enum)
        result = map_pg_type_to_mysql("USER-DEFINED", udt_name="status_type", conn=mock_conn)
        # Mock conn won't have enum data, should fall back to varchar
        assert "varchar" in result.lower() or "enum" in result.lower()

    def test_integer(self):
        assert map_pg_type_to_mysql("integer") == "int(11)"

    def test_boolean(self):
        assert map_pg_type_to_mysql("boolean") == "tinyint(1)"

    def test_unknown_type_fallback(self):
        result = map_pg_type_to_mysql("some_exotic_type")
        assert result == "some_exotic_type"

    def test_none_type(self):
        result = map_pg_type_to_mysql(None)
        # Should not crash
        assert isinstance(result, str)

    def test_empty_type(self):
        result = map_pg_type_to_mysql("")
        assert isinstance(result, str)

    def test_bpchar(self):
        result = map_pg_type_to_mysql("bpchar", 10)
        assert result == "char(10)"


class TestCleanDefault:
    """Test PostgreSQL default value cleaning."""

    def test_none(self):
        assert clean_default(None) is None

    def test_nextval_auto_increment(self):
        result = clean_default("nextval('users_id_seq'::regclass)")
        assert result == "__AUTO_INCREMENT__"

    def test_simple_number(self):
        result = clean_default("0")
        assert result == "0"

    def test_string_with_cast(self):
        result = clean_default("'hello'::character varying")
        assert result == "'hello'"

    def test_integer_with_cast(self):
        result = clean_default("0::integer")
        assert result == "0"

    def test_boolean_true(self):
        result = clean_default("true")
        assert result == "true"

    def test_boolean_false(self):
        result = clean_default("false")
        assert result == "false"

    def test_null_value(self):
        result = clean_default("NULL")
        assert result == "NULL"

    def test_complex_cast(self):
        result = clean_default("'2024-01-01'::timestamp without time zone")
        assert "'2024-01-01'" in result

    def test_empty_string(self):
        result = clean_default("")
        assert result is None

    def test_current_timestamp(self):
        result = clean_default("CURRENT_TIMESTAMP")
        assert result == "CURRENT_TIMESTAMP"

    def test_double_cast(self):
        result = clean_default("'value'::text::character varying")
        assert "'value'" in result


class TestGetEnumValues:
    """Test ENUM value retrieval."""

    def test_with_mock_conn(self, mock_conn):
        # Mock doesn't have pg_enum, returns empty
        result = get_enum_values(mock_conn, "nonexistent_type")
        # Should not crash, returns empty list or values
        assert isinstance(result, list)

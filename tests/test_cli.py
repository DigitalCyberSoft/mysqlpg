"""Tests for mysqlpg.cli — argument parsing and main orchestration."""

import pytest
from unittest.mock import patch, MagicMock
from mysqlpg.cli import build_parser, parse_password_arg


class TestPasswordArgParsing:
    """Test MySQL-style -pSECRET password handling."""

    def test_password_inline(self):
        result = parse_password_arg(["-pmypass"])
        assert result == ["--password", "mypass"]

    def test_password_separate(self):
        result = parse_password_arg(["-p", "mypass"])
        assert result == ["-p", "mypass"]

    def test_password_flag_only(self):
        result = parse_password_arg(["-p"])
        assert result == ["-p"]

    def test_long_password(self):
        result = parse_password_arg(["--password", "secret"])
        assert result == ["--password", "secret"]

    def test_no_password(self):
        result = parse_password_arg(["-u", "root", "-h", "localhost"])
        assert result == ["-u", "root", "-h", "localhost"]

    def test_mixed_args(self):
        result = parse_password_arg(["-u", "root", "-pmypass", "-h", "db.local"])
        assert "--password" in result
        assert "mypass" in result
        assert "-u" in result

    def test_port_not_confused_with_password(self):
        # -P is port, not password; should not be modified
        result = parse_password_arg(["-P", "3306"])
        assert result == ["-P", "3306"]


class TestBuildParser:
    """Test argparse argument definitions."""

    def test_default_host(self):
        parser = build_parser()
        args = parser.parse_args([])
        assert args.host == "localhost"

    def test_default_port(self):
        parser = build_parser()
        args = parser.parse_args([])
        assert args.port == 5432

    def test_user_flag(self):
        parser = build_parser()
        args = parser.parse_args(["-u", "myuser"])
        assert args.user == "myuser"

    def test_host_flag(self):
        parser = build_parser()
        args = parser.parse_args(["-h", "db.example.com"])
        assert args.host == "db.example.com"

    def test_port_flag(self):
        parser = build_parser()
        args = parser.parse_args(["-P", "5433"])
        assert args.port == 5433

    def test_database_flag(self):
        parser = build_parser()
        args = parser.parse_args(["-D", "mydb"])
        assert args.database == "mydb"

    def test_positional_database(self):
        parser = build_parser()
        args = parser.parse_args(["mydb"])
        assert args.dbname == "mydb"

    def test_execute_flag(self):
        parser = build_parser()
        args = parser.parse_args(["-e", "SHOW DATABASES"])
        assert args.execute == "SHOW DATABASES"

    def test_batch_flag(self):
        parser = build_parser()
        args = parser.parse_args(["-B"])
        assert args.batch is True

    def test_skip_column_names(self):
        parser = build_parser()
        args = parser.parse_args(["-N"])
        assert args.skip_column_names is True

    def test_table_flag(self):
        parser = build_parser()
        args = parser.parse_args(["-t"])
        assert args.table is True

    def test_silent_flag(self):
        parser = build_parser()
        args = parser.parse_args(["-s"])
        assert args.silent is True

    def test_force_flag(self):
        parser = build_parser()
        args = parser.parse_args(["-f"])
        assert args.force is True

    def test_verbose_flag(self):
        parser = build_parser()
        args = parser.parse_args(["-v"])
        assert args.verbose is True

    def test_delimiter(self):
        parser = build_parser()
        args = parser.parse_args(["--delimiter", "//"])
        assert args.delimiter == "//"

    def test_vertical_flag(self):
        parser = build_parser()
        args = parser.parse_args(["--vertical"])
        assert args.vertical is True

    def test_auto_vertical(self):
        parser = build_parser()
        args = parser.parse_args(["--auto-vertical-output"])
        assert args.auto_vertical_output is True

    def test_no_auto_rehash(self):
        parser = build_parser()
        args = parser.parse_args(["-A"])
        assert args.no_auto_rehash is True

    def test_safe_updates(self):
        parser = build_parser()
        args = parser.parse_args(["-U"])
        assert args.safe_updates is True

    def test_version(self):
        parser = build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["-V"])

    def test_help(self):
        parser = build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["--help"])

    def test_password_prompt_mode(self):
        parser = build_parser()
        args = parser.parse_args(["--password"])
        assert args.password == "__PROMPT__"

    def test_password_with_value(self):
        parser = build_parser()
        args = parser.parse_args(["--password", "secret"])
        assert args.password == "secret"

    def test_combined_flags(self):
        parser = build_parser()
        args = parser.parse_args([
            "-u", "root", "-h", "db.local", "-P", "5433",
            "-e", "SELECT 1", "-B", "-N"
        ])
        assert args.user == "root"
        assert args.host == "db.local"
        assert args.port == 5433
        assert args.execute == "SELECT 1"
        assert args.batch is True
        assert args.skip_column_names is True


class TestDatabaseResolution:
    """Test database name resolution from various sources."""

    def test_d_flag_takes_precedence(self):
        parser = build_parser()
        args = parser.parse_args(["-D", "fromflag", "frompositional"])
        # -D flag should be preferred
        database = args.database or args.dbname
        assert database == "fromflag"

    def test_positional_fallback(self):
        parser = build_parser()
        args = parser.parse_args(["frompositional"])
        database = args.database or args.dbname
        assert database == "frompositional"

    def test_no_database(self):
        parser = build_parser()
        args = parser.parse_args([])
        database = args.database or args.dbname
        assert database is None

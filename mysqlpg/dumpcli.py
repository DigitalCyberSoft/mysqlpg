"""mysqldumppg — mysqldump-compatible dump tool for PostgreSQL."""

import argparse
import datetime
import getpass
import os
import re
import signal
import sys
import time

from mysqlpg import __version__
from mysqlpg.connection import Connection
from mysqlpg.ddl import show_create_table, map_pg_type_to_mysql, get_enum_values


def build_parser():
    parser = argparse.ArgumentParser(
        prog="mysqldumppg",
        description="mysqldump-compatible dump tool for PostgreSQL",
        add_help=False,
    )

    # Connection
    parser.add_argument("-u", "--user",
                        default=os.environ.get("PGUSER", os.environ.get("USER", "postgres")))
    parser.add_argument("-p", "--password", nargs="?", const="__PROMPT__", default=None)
    parser.add_argument("-h", "--host", default=os.environ.get("PGHOST", "localhost"), dest="host")
    parser.add_argument("-P", "--port", type=int,
                        default=int(os.environ.get("PGPORT", "5432")))
    parser.add_argument("-S", "--socket", default=None)

    # Database/table selection
    parser.add_argument("positional", nargs="*", help="database [table1 table2 ...]")
    parser.add_argument("--databases", "-B", action="store_true",
                        help="Treat all positional args as database names")
    parser.add_argument("--all-databases", action="store_true")
    parser.add_argument("--tables", nargs="*", default=None)
    parser.add_argument("--ignore-table", action="append", default=[])

    # DDL options
    parser.add_argument("--add-drop-database", action="store_true")
    parser.add_argument("--add-drop-table", action="store_true", default=None)
    parser.add_argument("--no-create-db", "-n", action="store_true")
    parser.add_argument("--no-create-info", "-t", action="store_true")
    parser.add_argument("--create-options", action="store_true", default=None)
    parser.add_argument("--if-not-exists", action="store_true")

    # Data options
    parser.add_argument("--no-data", "-d", action="store_true")
    parser.add_argument("--complete-insert", "-c", action="store_true")
    parser.add_argument("--extended-insert", action="store_true", default=None)
    parser.add_argument("--skip-extended-insert", action="store_true")
    parser.add_argument("--insert-ignore", action="store_true")
    parser.add_argument("--replace", action="store_true")
    parser.add_argument("--hex-blob", action="store_true")
    parser.add_argument("--where", default=None)

    # Locking/consistency
    parser.add_argument("--single-transaction", action="store_true")
    parser.add_argument("--lock-tables", action="store_true", default=None)
    parser.add_argument("--lock-all-tables", action="store_true")
    parser.add_argument("--add-locks", action="store_true", default=None)
    parser.add_argument("--no-autocommit", action="store_true")

    # Output
    parser.add_argument("--result-file", default=None)
    parser.add_argument("--compact", action="store_true")
    parser.add_argument("--opt", action="store_true", default=True)
    parser.add_argument("--skip-opt", action="store_true")
    parser.add_argument("--comments", "-i", action="store_true", default=None)
    parser.add_argument("--skip-comments", action="store_true")
    parser.add_argument("--dump-date", action="store_true", default=None)
    parser.add_argument("--verbose", "-v", action="store_true")
    parser.add_argument("--quote-names", action="store_true", default=None)
    parser.add_argument("--set-charset", action="store_true", default=None)

    # Stored objects
    parser.add_argument("--routines", action="store_true")
    parser.add_argument("--triggers", action="store_true", default=None)
    parser.add_argument("--skip-triggers", action="store_true")
    parser.add_argument("--events", action="store_true")

    # Performance
    parser.add_argument("--quick", action="store_true", default=None)
    parser.add_argument("--disable-keys", action="store_true", default=None)

    # Compat flags (accepted, mostly no-op)
    parser.add_argument("--master-data", type=int, default=None)
    parser.add_argument("--source-data", type=int, default=None)
    parser.add_argument("--flush-logs", action="store_true")
    parser.add_argument("--default-character-set", default="utf8mb4")

    # Force
    parser.add_argument("--force", "-f", action="store_true")

    # pgloader compatibility
    parser.add_argument("--compatible", default=None,
                        help="Output compatibility mode (pgloader, mysql)")

    # Help
    parser.add_argument("--help", action="help", help="Show this help message and exit")

    return parser


class DumpOptions:
    """Resolved dump options with --opt defaults applied."""

    def __init__(self, args):
        # Start with --opt defaults (ON unless --skip-opt)
        opt = not args.skip_opt

        self.add_drop_database = args.add_drop_database
        self.add_drop_table = args.add_drop_table if args.add_drop_table is not None else opt
        self.no_create_db = args.no_create_db
        self.no_create_info = args.no_create_info
        self.create_options = args.create_options if args.create_options is not None else opt
        self.if_not_exists = args.if_not_exists
        self.no_data = args.no_data
        self.complete_insert = args.complete_insert
        self.extended_insert = (not args.skip_extended_insert) and \
                               (args.extended_insert if args.extended_insert is not None else opt)
        self.insert_ignore = args.insert_ignore
        self.replace = args.replace
        self.hex_blob = args.hex_blob
        self.where = args.where
        self.single_transaction = args.single_transaction
        self.lock_tables = args.lock_tables if args.lock_tables is not None else opt
        self.lock_all_tables = args.lock_all_tables
        self.add_locks = args.add_locks if args.add_locks is not None else opt
        self.no_autocommit = args.no_autocommit
        self.compact = args.compact
        self.comments = (not args.skip_comments) and \
                        (args.comments if args.comments is not None else True)
        self.dump_date = args.dump_date if args.dump_date is not None else True
        self.verbose = args.verbose
        self.quote_names = args.quote_names if args.quote_names is not None else True
        self.set_charset = args.set_charset if args.set_charset is not None else opt
        self.routines = args.routines
        self.triggers = (not args.skip_triggers) and \
                        (args.triggers if args.triggers is not None else True)
        self.quick = args.quick if args.quick is not None else opt
        self.disable_keys = args.disable_keys if args.disable_keys is not None else opt
        self.charset = args.default_character_set
        self.force = args.force
        self.compatible = getattr(args, 'compatible', None)

        if self.compact:
            self.comments = False
            self.add_locks = False
            self.set_charset = False
            self.disable_keys = False


def parse_password_arg(argv):
    """Handle -pSECRET style."""
    new_argv = []
    for arg in argv:
        if arg.startswith("-p") and len(arg) > 2 and not arg.startswith("--"):
            new_argv.extend(["--password", arg[2:]])
        else:
            new_argv.append(arg)
    return new_argv


def main():
    try:
        signal.signal(signal.SIGPIPE, signal.SIG_DFL)
    except (AttributeError, OSError):
        pass

    argv = parse_password_arg(sys.argv[1:])
    parser = build_parser()
    args = parser.parse_args(argv)

    password = args.password
    if password == "__PROMPT__":
        password = getpass.getpass("Enter password: ")

    opts = DumpOptions(args)

    # Determine databases and tables to dump
    if args.all_databases:
        databases = None  # Will be resolved after connect
        tables_filter = None
    elif args.databases:
        databases = args.positional
        tables_filter = None
    else:
        if args.positional:
            databases = [args.positional[0]]
            tables_filter = args.positional[1:] if len(args.positional) > 1 else None
        else:
            print("Usage: mysqldumppg [OPTIONS] database [tables]", file=sys.stderr)
            print("       mysqldumppg [OPTIONS] --databases [OPTIONS] DB1 [DB2 ...]", file=sys.stderr)
            print("       mysqldumppg [OPTIONS] --all-databases [OPTIONS]", file=sys.stderr)
            sys.exit(1)

    if args.tables:
        tables_filter = args.tables

    ignore_tables = set()
    for ign in args.ignore_table:
        # Format: db.table
        if "." in ign:
            ignore_tables.add(ign.split(".", 1)[1])
        else:
            ignore_tables.add(ign)

    # Output destination
    out = sys.stdout
    if args.result_file:
        try:
            out = open(args.result_file, "w")
        except IOError as e:
            print(f"ERROR: {e}", file=sys.stderr)
            sys.exit(1)

    # Connect to first database
    first_db = databases[0] if databases else None

    try:
        conn = Connection(
            host=args.host,
            port=args.port,
            user=args.user,
            password=password,
            database=first_db,
        )
    except Exception as e:
        print(f"ERROR: Can't connect to PostgreSQL server ({e})", file=sys.stderr)
        sys.exit(1)

    if args.all_databases:
        databases = conn.get_databases()

    if args.events:
        print("-- Warning: --events is not applicable for PostgreSQL (ignored)",
              file=sys.stderr)
    if args.master_data or args.source_data:
        print("-- Note: --master-data/--source-data not applicable; "
              "PostgreSQL uses WAL-based replication", file=sys.stderr)
    if args.flush_logs:
        print("-- Note: --flush-logs is a no-op for PostgreSQL", file=sys.stderr)

    try:
        dumper = Dumper(conn, out, opts, args)
        dumper.dump(databases, tables_filter, ignore_tables,
                    multi_db=args.all_databases or args.databases)
    except BrokenPipeError:
        pass
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        if not opts.force:
            sys.exit(1)
    finally:
        conn.close()
        if out is not sys.stdout:
            out.close()


class Dumper:
    """Performs the actual database dump."""

    def __init__(self, conn, out, opts, args):
        self.conn = conn
        self.out = out
        self.opts = opts
        self.args = args

    def _sort_tables_by_deps(self, tables):
        """Sort tables so referenced tables come before referencing tables."""
        if not tables:
            return tables
        table_set = set(tables)
        try:
            _, rows, *_ = self.conn.execute("""
                SELECT DISTINCT
                    tc.table_name AS child,
                    ccu.table_name AS parent
                FROM information_schema.table_constraints tc
                JOIN information_schema.constraint_column_usage ccu
                    ON tc.constraint_name = ccu.constraint_name
                    AND tc.table_schema = ccu.table_schema
                WHERE tc.constraint_type = 'FOREIGN KEY'
                  AND tc.table_schema = 'public'
            """)
        except Exception:
            return tables

        # Build dependency graph
        deps = {t: set() for t in tables}
        for child, parent in (rows or []):
            if child in table_set and parent in table_set and child != parent:
                deps[child].add(parent)

        # Topological sort (Kahn's algorithm)
        result = []
        ready = [t for t in tables if not deps[t]]
        ready.sort()
        while ready:
            t = ready.pop(0)
            result.append(t)
            for other in list(deps):
                if t in deps[other]:
                    deps[other].discard(t)
                    if not deps[other] and other not in result:
                        ready.append(other)
                        ready.sort()

        # Add any remaining (circular deps)
        for t in tables:
            if t not in result:
                result.append(t)
        return result

    def w(self, text):
        """Write to output."""
        try:
            self.out.write(text)
        except BrokenPipeError:
            raise

    def log(self, msg):
        """Write verbose message to stderr."""
        if self.opts.verbose:
            print(f"-- {msg}", file=sys.stderr)

    def _get_enum_types(self):
        """Get all ENUM types used in the current database."""
        try:
            _, rows, *_ = self.conn.execute("""
                SELECT DISTINCT t.typname,
                       array_agg(e.enumlabel ORDER BY e.enumsortorder) AS labels
                FROM pg_type t
                JOIN pg_enum e ON e.enumtypid = t.oid
                JOIN pg_namespace n ON n.oid = t.typnamespace
                WHERE n.nspname = 'public'
                GROUP BY t.typname
                ORDER BY t.typname
            """)
            return rows or []
        except Exception:
            return []

    def _emit_enum_types(self, db):
        """Emit ENUM type definitions (for pgloader-compatible output)."""
        q = lambda s: f"`{s}`" if self.opts.quote_names else s
        enums = self._get_enum_types()
        if not enums:
            return

        if self.opts.comments:
            self.w(f"--\n-- ENUM type definitions for database '{db}'\n--\n\n")

        for typname, labels in enums:
            if self.opts.compatible == 'pgloader':
                # Emit PG-native CREATE TYPE for pgloader round-trip
                vals = ", ".join(f"'{v}'" for v in labels)
                self.w(f"CREATE TYPE {q(typname)} AS ENUM ({vals});\n")
            # In standard MySQL mode, ENUMs are inline in column defs
        if enums:
            self.w("\n")

    def dump(self, databases, tables_filter, ignore_tables, multi_db=False):
        """Main dump orchestration."""
        if self.opts.single_transaction:
            self.conn.set_autocommit(False)
            self.conn.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
            self.log("Started single-transaction dump")

        self._emit_header()

        for db in databases:
            if db != self.conn.database:
                try:
                    self.conn.reconnect(database=db)
                except Exception as e:
                    print(f"ERROR: Could not connect to database '{db}': {e}",
                          file=sys.stderr)
                    if not self.opts.force:
                        raise
                    continue

                if self.opts.single_transaction:
                    self.conn.set_autocommit(False)
                    self.conn.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")

            if multi_db:
                self._emit_database_header(db)

            # Emit ENUM types if pgloader-compatible mode
            if self.opts.compatible == 'pgloader':
                self._emit_enum_types(db)

            # Get tables, sorted by FK dependency order
            tables = tables_filter if tables_filter else self.conn.get_tables()
            tables = [t for t in tables if t not in ignore_tables]
            tables = self._sort_tables_by_deps(tables)

            for table in tables:
                self.log(f"Dumping table `{table}`")
                self._dump_table(db, table)

            if self.opts.triggers:
                self._dump_triggers(db, tables)

            if self.opts.routines:
                self._dump_routines(db)

        self._emit_footer()

        if self.opts.single_transaction:
            try:
                self.conn.execute("COMMIT")
            except Exception:
                pass

    def _emit_header(self):
        if self.opts.comments:
            version = self.conn.get_server_version_string()
            self.w(f"-- mysqldumppg (PostgreSQL)  Distrib {__version__}\n")
            self.w("--\n")
            self.w(f"-- Host: {self.conn.host}    Database: {self.conn.database}\n")
            self.w(f"-- Server version\t{version}\n")
            self.w("-- ------------------------------------------------------\n")
            self.w("\n")

        if self.opts.set_charset:
            self.w("/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;\n")
            self.w("/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;\n")
            self.w("/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;\n")
            self.w(f"/*!40101 SET CHARACTER_SET_CLIENT={self.opts.charset} */;\n")
            self.w(f"SET NAMES {self.opts.charset};\n")

        self.w("SET FOREIGN_KEY_CHECKS = 0;\n")
        self.w("\n")

    def _emit_footer(self):
        self.w("\n")
        if self.opts.set_charset:
            self.w("/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;\n")
            self.w("/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;\n")
            self.w("/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;\n")

        if self.opts.comments and self.opts.dump_date:
            now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.w(f"-- Dump completed on {now}\n")

    def _emit_database_header(self, db):
        q = lambda s: f"`{s}`" if self.opts.quote_names else s

        if self.opts.comments:
            self.w(f"--\n-- Current Database: {q(db)}\n--\n\n")

        if not self.opts.no_create_db:
            if self.opts.add_drop_database:
                self.w(f"DROP DATABASE IF EXISTS {q(db)};\n")
            self.w(f"CREATE DATABASE /*!32312 IF NOT EXISTS*/ {q(db)} "
                   f"/*!40100 DEFAULT CHARACTER SET {self.opts.charset} */;\n\n")
            self.w(f"USE {q(db)};\n\n")

    def _dump_table(self, db, table):
        q = lambda s: f"`{s}`" if self.opts.quote_names else s

        # Section comment
        if self.opts.comments:
            self.w(f"--\n-- Table structure for table {q(table)}\n--\n\n")

        # DROP TABLE
        if self.opts.add_drop_table:
            self.w(f"DROP TABLE IF EXISTS {q(table)};\n")

        # CREATE TABLE
        if not self.opts.no_create_info:
            try:
                _, create_ddl = show_create_table(self.conn, table)
                if self.opts.if_not_exists:
                    create_ddl = create_ddl.replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS", 1)
                self.w(create_ddl + ";\n\n")
            except Exception as e:
                self.w(f"-- ERROR: Could not get CREATE TABLE for `{table}`: {e}\n\n")

        # Data
        if not self.opts.no_data:
            self._dump_table_data(db, table)

    def _dump_table_data(self, db, table):
        q = lambda s: f"`{s}`" if self.opts.quote_names else s

        if self.opts.comments:
            self.w(f"--\n-- Dumping data for table {q(table)}\n--\n\n")

        if self.opts.add_locks:
            self.w(f"LOCK TABLES {q(table)} WRITE;\n")

        if self.opts.disable_keys:
            self.w(f"/*!40000 ALTER TABLE {q(table)} DISABLE KEYS */;\n")

        if self.opts.no_autocommit:
            self.w("SET autocommit=0;\n")

        # Build SELECT
        where_clause = f" WHERE {self.opts.where}" if self.opts.where else ""
        select_sql = f'SELECT * FROM "{table}"{where_clause}'

        try:
            # Get column info for complete-insert and type awareness
            col_info = self._get_column_info(table)
            col_names = [ci[0] for ci in col_info]
            col_types = [ci[1] for ci in col_info]

            if self.opts.quick:
                columns, cur, old_ac = self.conn.execute_with_cursor(
                    select_sql, name=f"dump_{table}"
                )
                try:
                    self._stream_rows(table, col_names, col_types, cur)
                finally:
                    self.conn.finish_cursor(cur, old_ac)
            else:
                columns, rows, status, rowcount, elapsed = self.conn.execute(select_sql)
                if rows:
                    self._write_inserts(table, col_names, col_types, rows)
        except Exception as e:
            self.w(f"-- ERROR dumping data for `{table}`: {e}\n")

        if self.opts.disable_keys:
            self.w(f"/*!40000 ALTER TABLE {q(table)} ENABLE KEYS */;\n")

        if self.opts.no_autocommit:
            self.w("COMMIT;\n")

        if self.opts.add_locks:
            self.w("UNLOCK TABLES;\n")

        self.w("\n")

    def _get_column_info(self, table):
        """Get (column_name, data_type) for a table."""
        _, rows, *_ = self.conn.execute("""
            SELECT column_name, data_type, udt_name
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s
            ORDER BY ordinal_position
        """, (table,))
        return [(r[0], r[1], r[2] if len(r) > 2 else None) for r in rows] if rows else []

    def _stream_rows(self, table, col_names, col_types, cursor):
        """Stream rows from server-side cursor and write INSERTs."""
        batch = []
        for row in cursor:
            batch.append(row)
            if len(batch) >= 1000:
                self._write_inserts(table, col_names, col_types, batch)
                batch = []
        if batch:
            self._write_inserts(table, col_names, col_types, batch)

    def _write_inserts(self, table, col_names, col_types, rows):
        """Write INSERT statements for a batch of rows."""
        q = lambda s: f"`{s}`" if self.opts.quote_names else s

        # Determine INSERT prefix
        if self.opts.replace:
            prefix = f"REPLACE INTO {q(table)}"
        elif self.opts.insert_ignore:
            prefix = f"INSERT IGNORE INTO {q(table)}"
        else:
            prefix = f"INSERT INTO {q(table)}"

        if self.opts.complete_insert:
            col_list = ", ".join(q(c) for c in col_names)
            prefix += f" ({col_list})"

        prefix += " VALUES"

        if self.opts.extended_insert:
            # Multi-row INSERT
            value_strings = []
            for row in rows:
                vals = self._format_row(row, col_types)
                value_strings.append(f"({vals})")
                # Flush at reasonable line length
                if len(value_strings) >= 100:
                    self.w(f"{prefix} {','.join(value_strings)};\n")
                    value_strings = []
            if value_strings:
                self.w(f"{prefix} {','.join(value_strings)};\n")
        else:
            # One row per INSERT
            for row in rows:
                vals = self._format_row(row, col_types)
                self.w(f"{prefix} ({vals});\n")

    def _format_row(self, row, col_types):
        """Format a row's values for INSERT."""
        parts = []
        for i, val in enumerate(row):
            dtype = col_types[i][0] if i < len(col_types) else "text"
            parts.append(self._format_value(val, dtype))
        return ",".join(parts)

    def _format_value(self, val, dtype="text"):
        """Format a single value for SQL output."""
        if val is None:
            return "NULL"

        if isinstance(val, bool):
            return "1" if val else "0"

        if isinstance(val, (int, float)):
            return str(val)

        if isinstance(val, (bytes, memoryview)):
            b = bytes(val) if isinstance(val, memoryview) else val
            if self.opts.hex_blob:
                return f"0x{b.hex()}"
            return f"X'{b.hex()}'"

        if isinstance(val, datetime.datetime):
            return f"'{val.strftime('%Y-%m-%d %H:%M:%S')}'"

        if isinstance(val, datetime.date):
            return f"'{val.isoformat()}'"

        if isinstance(val, datetime.time):
            return f"'{val.isoformat()}'"

        if isinstance(val, datetime.timedelta):
            total = int(val.total_seconds())
            hours, remainder = divmod(abs(total), 3600)
            minutes, seconds = divmod(remainder, 60)
            sign = "-" if total < 0 else ""
            return f"'{sign}{hours:02d}:{minutes:02d}:{seconds:02d}'"

        # Everything else: string escape
        s = str(val)
        return "'" + self._escape_string(s) + "'"

    def _escape_string(self, s):
        """Escape a string for SQL."""
        s = s.replace("\\", "\\\\")
        s = s.replace("'", "''")
        s = s.replace("\n", "\\n")
        s = s.replace("\r", "\\r")
        s = s.replace("\t", "\\t")
        s = s.replace("\x00", "\\0")
        s = s.replace("\x1a", "\\Z")
        return s

    def _dump_triggers(self, db, tables):
        """Dump triggers for the given tables."""
        q = lambda s: f"`{s}`" if self.opts.quote_names else s

        has_triggers = False
        for table in tables:
            try:
                _, rows, *_ = self.conn.execute("""
                    SELECT t.tgname, pg_get_triggerdef(t.oid) AS triggerdef,
                           c.relname AS table_name
                    FROM pg_trigger t
                    JOIN pg_class c ON c.oid = t.tgrelid
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE c.relname = %s AND n.nspname = 'public'
                      AND NOT t.tgisinternal
                """, (table,))

                if rows:
                    if not has_triggers:
                        self.w("\n")
                        has_triggers = True

                    for tgname, triggerdef, tbl in rows:
                        if self.opts.comments:
                            self.w(f"--\n-- Trigger {q(tgname)} on table {q(tbl)}\n--\n")
                        self.w(f"/*!50003 {triggerdef} */;\n")
            except Exception as e:
                self.w(f"-- ERROR dumping triggers for `{table}`: {e}\n")

    def _dump_routines(self, db):
        """Dump functions and procedures."""
        q = lambda s: f"`{s}`" if self.opts.quote_names else s

        if self.opts.comments:
            self.w(f"\n--\n-- Dumping routines for database '{db}'\n--\n")

        try:
            _, rows, *_ = self.conn.execute("""
                SELECT p.proname, pg_get_functiondef(p.oid) AS funcdef,
                       p.prokind
                FROM pg_proc p
                JOIN pg_namespace n ON n.oid = p.pronamespace
                WHERE n.nspname = 'public'
                ORDER BY p.proname
            """)

            if rows:
                for proname, funcdef, prokind in rows:
                    kind = "PROCEDURE" if prokind == 'p' else "FUNCTION"
                    if self.opts.comments:
                        self.w(f"\n--\n-- {kind}: {q(proname)}\n--\n")
                    self.w("DELIMITER ;;\n")
                    self.w(f"{funcdef} ;;\n")
                    self.w("DELIMITER ;\n")
        except Exception as e:
            self.w(f"-- ERROR dumping routines: {e}\n")


if __name__ == "__main__":
    main()

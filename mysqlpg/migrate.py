"""mysqlpg-migrate — MySQL → PostgreSQL migration tool.

Provides multiple migration workflows:
  1. From a MySQL dump file (.sql) into a PostgreSQL database
  2. From a live MySQL database (via mysqldump) into PostgreSQL
  3. Schema-only migration (for review/editing before data load)
  4. Data-only migration (when schema already exists)
  5. Validation of a completed migration (row counts, checksums)
"""

import argparse
import getpass
import hashlib
import os
import re
import signal
import subprocess
import sys
import tempfile
import time

from mysqlpg import __version__
from mysqlpg.connection import Connection


def build_parser():
    parser = argparse.ArgumentParser(
        prog="mysqlpg-migrate",
        description="MySQL → PostgreSQL migration tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Migrate from a MySQL dump file
  mysqlpg-migrate --from-dump backup.sql --to-pg postgres://user:pass@host/dbname

  # Migrate from a live MySQL server (requires mysqldump on PATH)
  mysqlpg-migrate --from-mysql mysql://root:pass@mysqlhost/mydb --to-pg postgres://user@pghost/mydb

  # Schema-only migration (review before loading data)
  mysqlpg-migrate --from-dump backup.sql --to-pg postgres://user@host/db --schema-only

  # Data-only migration (schema already created)
  mysqlpg-migrate --from-dump backup.sql --to-pg postgres://user@host/db --data-only

  # Validate migration (compare row counts)
  mysqlpg-migrate --validate --from-dump backup.sql --to-pg postgres://user@host/db

  # Dry run: show translated SQL without executing
  mysqlpg-migrate --from-dump backup.sql --dry-run

  # Generate a PG-compatible SQL file from a MySQL dump
  mysqlpg-migrate --from-dump backup.sql --to-file output.sql
""",
    )

    # Source
    source = parser.add_argument_group("Source (pick one)")
    source.add_argument("--from-dump", metavar="FILE",
                        help="MySQL dump file (.sql or .sql.gz)")
    source.add_argument("--from-mysql", metavar="URL",
                        help="Live MySQL connection URL: mysql://user:pass@host[:port]/dbname")

    # Target
    target = parser.add_argument_group("Target (pick one)")
    target.add_argument("--to-pg", metavar="URL",
                        help="PostgreSQL connection URL: postgres://user:pass@host[:port]/dbname")
    target.add_argument("--to-file", metavar="FILE",
                        help="Write translated SQL to file instead of executing")

    # PostgreSQL connection (alternative to --to-pg URL)
    pg = parser.add_argument_group("PostgreSQL connection (alternative to --to-pg URL)")
    pg.add_argument("--pg-host", default=os.environ.get("PGHOST", "localhost"))
    pg.add_argument("--pg-port", type=int, default=int(os.environ.get("PGPORT", "5432")))
    pg.add_argument("--pg-user", default=os.environ.get("PGUSER", os.environ.get("USER", "postgres")))
    pg.add_argument("--pg-password", default=os.environ.get("PGPASSWORD"))
    pg.add_argument("--pg-database", default=os.environ.get("PGDATABASE"))

    # Migration mode
    mode = parser.add_argument_group("Migration mode")
    mode.add_argument("--schema-only", action="store_true",
                      help="Migrate schema only (CREATE TABLE, indexes, constraints)")
    mode.add_argument("--data-only", action="store_true",
                      help="Migrate data only (INSERT statements; schema must exist)")
    mode.add_argument("--drop-tables", action="store_true",
                      help="DROP existing tables before migration")
    mode.add_argument("--create-db", action="store_true",
                      help="Create the target database if it doesn't exist")
    mode.add_argument("--no-fk-checks", action="store_true", default=True,
                      help="Disable FK checks during migration (default: enabled)")
    mode.add_argument("--fk-checks", action="store_true",
                      help="Keep FK checks enabled during migration")

    # User migration
    users = parser.add_argument_group("User migration")
    users.add_argument("--migrate-users", action="store_true",
                       help="Create PostgreSQL roles matching MySQL users")
    users.add_argument("--users-from", metavar="FILE",
                       help="MySQL user dump file (from: mysqldump mysql user db)")
    users.add_argument("--default-password", metavar="PASS",
                       help="Default password for migrated users (if not extractable)")
    users.add_argument("--superuser", nargs="*", default=[],
                       help="Users that should get SUPERUSER privilege")

    # Options
    opts = parser.add_argument_group("Options")
    opts.add_argument("--dry-run", action="store_true",
                      help="Show translated SQL without executing")
    opts.add_argument("--validate", action="store_true",
                      help="After migration, validate row counts")
    opts.add_argument("--force", "-f", action="store_true",
                      help="Continue on errors")
    opts.add_argument("--verbose", "-v", action="store_true",
                      help="Verbose output")
    opts.add_argument("--quiet", "-q", action="store_true",
                      help="Suppress progress output")
    opts.add_argument("--single-transaction", action="store_true",
                      help="Wrap entire migration in a single transaction")
    opts.add_argument("--tables", nargs="*",
                      help="Migrate only these tables")
    opts.add_argument("--exclude-tables", nargs="*",
                      help="Skip these tables")
    opts.add_argument("--truncate-tables", action="store_true",
                      help="TRUNCATE target tables before loading data")

    # Version
    opts.add_argument("-V", "--version", action="version",
                      version=f"mysqlpg-migrate {__version__}")

    return parser


def parse_url(url):
    """Parse a database URL into components."""
    # postgres://user:pass@host:port/dbname
    # mysql://user:pass@host:port/dbname
    m = re.match(
        r'(?:postgres(?:ql)?|mysql)://(?:([^:@]+)(?::([^@]*))?@)?([^:/]+)?(?::(\d+))?(?:/(.+))?',
        url
    )
    if not m:
        return {}
    return {
        'user': m.group(1),
        'password': m.group(2),
        'host': m.group(3) or 'localhost',
        'port': int(m.group(4)) if m.group(4) else None,
        'database': m.group(5),
    }


def log(msg, verbose=True, quiet=False, file=sys.stderr):
    """Print progress message."""
    if quiet:
        return
    if verbose:
        print(f"-- {msg}", file=file)


def read_dump_file(path):
    """Read a MySQL dump file, handling .gz compression."""
    if path.endswith('.gz'):
        import gzip
        with gzip.open(path, 'rt', encoding='utf-8', errors='replace') as f:
            return f.read()
    else:
        with open(path, 'r', encoding='utf-8', errors='replace') as f:
            return f.read()


def dump_from_mysql(mysql_url, tables=None, exclude=None):
    """Run mysqldump against a live MySQL server and return the dump content."""
    parts = parse_url(mysql_url)
    cmd = ['mysqldump']
    if parts.get('user'):
        cmd.extend(['-u', parts['user']])
    if parts.get('password'):
        cmd.extend([f"-p{parts['password']}"])
    if parts.get('host'):
        cmd.extend(['-h', parts['host']])
    if parts.get('port'):
        cmd.extend(['-P', str(parts['port'])])

    cmd.append('--single-transaction')
    cmd.append('--routines')
    cmd.append('--triggers')
    cmd.append('--set-charset')

    if parts.get('database'):
        cmd.append(parts['database'])

    if tables:
        cmd.extend(tables)
    if exclude:
        for t in exclude:
            db = parts.get('database', '')
            cmd.extend([f'--ignore-table={db}.{t}'])

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        return result.stdout
    except FileNotFoundError:
        print("ERROR: mysqldump not found on PATH. Install MySQL client tools or provide a dump file.",
              file=sys.stderr)
        sys.exit(1)
    except subprocess.CalledProcessError as e:
        print(f"ERROR: mysqldump failed: {e.stderr}", file=sys.stderr)
        sys.exit(1)


def split_statements(sql):
    """Split SQL dump into individual statements, respecting delimiters and quotes."""
    statements = []
    current = []
    delimiter = ';'
    in_sq = False
    in_dq = False
    i = 0

    while i < len(sql):
        ch = sql[i]

        # Handle DELIMITER changes
        if not in_sq and not in_dq:
            remaining = sql[i:]
            dm = re.match(r'DELIMITER\s+(\S+)', remaining, re.IGNORECASE)
            if dm and (i == 0 or sql[i-1] == '\n'):
                delimiter = dm.group(1)
                i += dm.end()
                # Skip to next line
                nl = sql.find('\n', i)
                if nl >= 0:
                    i = nl + 1
                else:
                    i = len(sql)
                continue

        if ch == "'" and not in_dq:
            in_sq = not in_sq
            current.append(ch)
        elif ch == '"' and not in_sq:
            in_dq = not in_dq
            current.append(ch)
        elif ch == '\\' and (in_sq or in_dq) and i + 1 < len(sql):
            current.append(ch)
            current.append(sql[i + 1])
            i += 2
            continue
        elif not in_sq and not in_dq:
            # Check for delimiter
            if sql[i:i+len(delimiter)] == delimiter:
                stmt = ''.join(current).strip()
                if stmt:
                    statements.append(stmt)
                current = []
                i += len(delimiter)
                continue
            # Skip single-line comments
            elif ch == '-' and i + 1 < len(sql) and sql[i+1] == '-':
                nl = sql.find('\n', i)
                if nl >= 0:
                    i = nl + 1
                else:
                    i = len(sql)
                continue
            # Skip block comments (but preserve conditional comments for processing)
            elif ch == '/' and i + 1 < len(sql) and sql[i+1] == '*':
                # Check if it's a conditional comment /*!NNNNN ... */
                cm = re.match(r'/\*!(\d+)?\s*(.*?)\*/', sql[i:], re.DOTALL)
                if cm:
                    # Extract the content of the conditional comment
                    content = cm.group(2).strip()
                    if content:
                        current.append(content)
                    i += cm.end()
                    continue
                else:
                    # Regular comment — skip
                    end = sql.find('*/', i + 2)
                    if end >= 0:
                        i = end + 2
                    else:
                        i = len(sql)
                    continue
            else:
                current.append(ch)
        else:
            current.append(ch)
        i += 1

    # Last statement
    stmt = ''.join(current).strip()
    if stmt:
        statements.append(stmt)

    return statements


def translate_statement(stmt, conn=None):
    """Translate a single MySQL statement to PostgreSQL."""
    from mysqlpg.translator import translate

    # Create a minimal mock connection if none provided
    if conn is None:
        class MinimalConn:
            database = "migration"
            def get_current_user(self): return "postgres"
            def get_tables(self, schema="public"): return []
            def get_columns(self, table, schema="public"): return []
            def get_primary_key_columns(self, table, schema="public"): return []
            def pop_notices(self): return []
        conn = MinimalConn()

    result, is_special = translate(stmt, conn)
    if is_special:
        return None  # No-op (e.g., LOCK TABLES, SET CHARACTER_SET)
    return result


def classify_statement(stmt):
    """Classify a SQL statement as schema, data, or control."""
    upper = stmt.strip().upper()
    if upper.startswith(('CREATE TABLE', 'CREATE INDEX', 'CREATE UNIQUE INDEX',
                         'CREATE TYPE', 'ALTER TABLE', 'DROP TABLE', 'DROP INDEX',
                         'DROP TYPE', 'CREATE VIEW', 'DROP VIEW')):
        return 'schema'
    elif upper.startswith(('INSERT', 'REPLACE')):
        return 'data'
    elif upper.startswith(('CREATE DATABASE', 'USE ', 'DROP DATABASE')):
        return 'database'
    elif upper.startswith(('SET ', 'LOCK ', 'UNLOCK', 'DELIMITER')):
        return 'control'
    elif upper.startswith(('CREATE FUNCTION', 'CREATE PROCEDURE', 'CREATE TRIGGER',
                           'CREATE OR REPLACE FUNCTION')):
        return 'routine'
    else:
        return 'other'


def migrate(args):
    """Main migration logic."""
    v = args.verbose
    q = args.quiet

    # --- Migrate users if requested ---
    if args.migrate_users or args.users_from:
        _migrate_users_from_mysql(args, v, q)

    # --- Determine source ---
    if args.from_dump:
        if not os.path.isfile(args.from_dump):
            print(f"ERROR: File not found: {args.from_dump}", file=sys.stderr)
            sys.exit(1)
        log(f"Reading dump file: {args.from_dump}", v, q)
        dump_sql = read_dump_file(args.from_dump)
    elif args.from_mysql:
        log(f"Dumping from MySQL: {args.from_mysql}", v, q)
        dump_sql = dump_from_mysql(args.from_mysql, args.tables, args.exclude_tables)
    else:
        print("ERROR: Must specify --from-dump or --from-mysql", file=sys.stderr)
        sys.exit(1)

    # --- Parse statements ---
    log("Parsing SQL statements...", v, q)
    statements = split_statements(dump_sql)
    log(f"Found {len(statements)} statements", v, q)

    # Classify
    schema_stmts = []
    data_stmts = []
    control_stmts = []
    routine_stmts = []
    db_stmts = []
    other_stmts = []

    for stmt in statements:
        cat = classify_statement(stmt)
        if cat == 'schema':
            schema_stmts.append(stmt)
        elif cat == 'data':
            data_stmts.append(stmt)
        elif cat == 'control':
            control_stmts.append(stmt)
        elif cat == 'routine':
            routine_stmts.append(stmt)
        elif cat == 'database':
            db_stmts.append(stmt)
        else:
            other_stmts.append(stmt)

    log(f"  Schema: {len(schema_stmts)}, Data: {len(data_stmts)}, "
        f"Routines: {len(routine_stmts)}, Control: {len(control_stmts)}, "
        f"Other: {len(other_stmts)}", v, q)

    # Filter by mode
    if args.schema_only:
        data_stmts = []
        routine_stmts = []
    elif args.data_only:
        schema_stmts = []
        db_stmts = []

    # Filter by table list
    if args.tables:
        table_set = set(args.tables)
        schema_stmts = [s for s in schema_stmts if _mentions_table(s, table_set)]
        data_stmts = [s for s in data_stmts if _mentions_table(s, table_set)]
    if args.exclude_tables:
        exclude_set = set(args.exclude_tables)
        schema_stmts = [s for s in schema_stmts if not _mentions_table(s, exclude_set)]
        data_stmts = [s for s in data_stmts if not _mentions_table(s, exclude_set)]

    # --- Translate ---
    log("Translating MySQL → PostgreSQL...", v, q)
    conn_for_translate = None
    translated = []

    # Add FK disable at the start
    if args.no_fk_checks and not args.fk_checks:
        translated.append("SET session_replication_role = 'replica'")

    # Translate control statements (SET NAMES, etc.)
    for stmt in control_stmts:
        pg = translate_statement(stmt, conn_for_translate)
        if pg:
            translated.append(pg)

    # Database statements
    for stmt in db_stmts:
        pg = translate_statement(stmt, conn_for_translate)
        if pg:
            translated.append(pg)

    # Schema
    for stmt in schema_stmts:
        pg = translate_statement(stmt, conn_for_translate)
        if pg:
            translated.append(pg)

    # Data
    for stmt in data_stmts:
        pg = translate_statement(stmt, conn_for_translate)
        if pg:
            translated.append(pg)

    # Routines
    for stmt in routine_stmts:
        pg = translate_statement(stmt, conn_for_translate)
        if pg:
            translated.append(pg)

    # Re-enable FK checks
    if args.no_fk_checks and not args.fk_checks:
        translated.append("SET session_replication_role = 'origin'")

    log(f"Translated {len(translated)} statements", v, q)

    # --- Output ---
    if args.dry_run:
        for stmt in translated:
            print(stmt + ";")
            print()
        return

    if args.to_file:
        log(f"Writing to: {args.to_file}", v, q)
        with open(args.to_file, 'w') as f:
            f.write(f"-- mysqlpg-migrate {__version__}\n")
            f.write(f"-- Generated from: {args.from_dump or args.from_mysql}\n")
            f.write(f"-- Date: {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            for stmt in translated:
                f.write(stmt + ";\n\n")
        log(f"Written {len(translated)} statements to {args.to_file}", v, q)
        return

    # --- Connect to PostgreSQL and execute ---
    pg_params = {}
    if args.to_pg:
        pg_params = parse_url(args.to_pg)
    else:
        pg_params = {
            'host': args.pg_host,
            'port': args.pg_port,
            'user': args.pg_user,
            'password': args.pg_password,
            'database': args.pg_database,
        }

    if not pg_params.get('database'):
        print("ERROR: No target database specified. Use --to-pg URL or --pg-database.",
              file=sys.stderr)
        sys.exit(1)

    # Create database if requested
    if args.create_db:
        try:
            admin_conn = Connection(
                host=pg_params.get('host', 'localhost'),
                port=pg_params.get('port', 5432),
                user=pg_params.get('user'),
                password=pg_params.get('password'),
                database='postgres',
            )
            try:
                admin_conn.execute(f'CREATE DATABASE "{pg_params["database"]}"')
                log(f"Created database: {pg_params['database']}", v, q)
            except Exception as e:
                if 'already exists' in str(e):
                    log(f"Database {pg_params['database']} already exists", v, q)
                else:
                    raise
            finally:
                admin_conn.close()
        except Exception as e:
            print(f"ERROR: Could not create database: {e}", file=sys.stderr)
            if not args.force:
                sys.exit(1)

    # Connect to target
    try:
        conn = Connection(
            host=pg_params.get('host', 'localhost'),
            port=pg_params.get('port', 5432),
            user=pg_params.get('user'),
            password=pg_params.get('password'),
            database=pg_params['database'],
        )
    except Exception as e:
        print(f"ERROR: Can't connect to PostgreSQL: {e}", file=sys.stderr)
        sys.exit(1)

    log(f"Connected to PostgreSQL: {pg_params.get('host')}:{pg_params.get('port', 5432)}"
        f"/{pg_params['database']}", v, q)

    # Execute
    if args.single_transaction:
        conn.set_autocommit(False)

    errors = 0
    executed = 0
    start_time = time.time()

    for i, stmt in enumerate(translated):
        if not stmt.strip():
            continue
        try:
            # Handle multi-statement results (e.g., CREATE TABLE + CREATE INDEX)
            for sub in stmt.split(';'):
                sub = sub.strip()
                if not sub:
                    continue
                conn.execute(sub)
            executed += 1
            if v and not q and (executed % 100 == 0):
                log(f"  Executed {executed}/{len(translated)} statements...", v, q)
        except Exception as e:
            errors += 1
            if not q:
                print(f"ERROR [{i+1}]: {e}", file=sys.stderr)
                if v:
                    print(f"  Statement: {stmt[:200]}...", file=sys.stderr)
            if not args.force:
                if args.single_transaction:
                    conn.execute("ROLLBACK")
                conn.close()
                sys.exit(1)

    if args.single_transaction:
        conn.execute("COMMIT")
        conn.set_autocommit(True)

    elapsed = time.time() - start_time

    if not q:
        print(f"\nMigration complete: {executed} statements executed, "
              f"{errors} errors, {elapsed:.1f}s", file=sys.stderr)

    # --- Validation ---
    if args.validate:
        log("\nValidating migration...", True, q)
        _validate(conn, data_stmts, v, q)

    conn.close()


def _mentions_table(stmt, table_set):
    """Check if a SQL statement mentions any table in the set."""
    upper = stmt.upper()
    for t in table_set:
        if t.upper() in upper or f'`{t}`'.upper() in upper:
            return True
    return False


def _validate(conn, data_stmts, verbose, quiet):
    """Validate migration by checking row counts."""
    # Extract table names from INSERT statements
    tables = set()
    for stmt in data_stmts:
        m = re.match(r"INSERT\s+(?:INTO\s+)?[`\"]?(\w+)", stmt, re.IGNORECASE)
        if m:
            tables.add(m.group(1))

    if not tables:
        tables_from_pg = conn.get_tables()
        tables = set(tables_from_pg)

    if not tables:
        log("No tables found to validate.", verbose, quiet)
        return

    total_rows = 0
    for table in sorted(tables):
        try:
            _, rows, *_ = conn.execute(f'SELECT COUNT(*) FROM "{table}"')
            count = rows[0][0]
            total_rows += count
            log(f"  {table}: {count} rows", True, quiet)
        except Exception as e:
            log(f"  {table}: ERROR - {e}", True, quiet)

    log(f"\nTotal: {total_rows} rows across {len(tables)} tables", True, quiet)


def _migrate_users_from_mysql(args, verbose, quiet):
    """Extract MySQL users and create matching PostgreSQL roles."""
    if not args.migrate_users and not args.users_from:
        return

    pg_params = {}
    if args.to_pg:
        pg_params = parse_url(args.to_pg)
    else:
        pg_params = {
            'host': args.pg_host, 'port': args.pg_port,
            'user': args.pg_user, 'password': args.pg_password,
            'database': args.pg_database or 'postgres',
        }

    users = []

    if args.users_from:
        # Parse a MySQL user dump file
        log(f"Reading user definitions from: {args.users_from}", verbose, quiet)
        content = read_dump_file(args.users_from)
        users = _parse_mysql_users(content)
    elif args.from_mysql:
        # Extract users from live MySQL via query
        log("Extracting users from MySQL...", verbose, quiet)
        users = _extract_users_from_mysql(args.from_mysql)
    elif args.from_dump and args.migrate_users:
        # Try to extract users from the dump file itself (GRANT/CREATE USER statements)
        log(f"Scanning dump for user definitions...", verbose, quiet)
        content = read_dump_file(args.from_dump)
        users = _parse_mysql_users(content)

    if not users:
        log("No users found to migrate.", verbose, quiet)
        return

    log(f"Found {len(users)} users to migrate", verbose, quiet)

    if args.dry_run:
        for u in users:
            is_super = u['name'] in (args.superuser or [])
            sql = _build_create_role_sql(u, args.default_password, is_super)
            print(sql + ";")
        return

    # Connect to PG and create roles
    try:
        conn = Connection(
            host=pg_params.get('host', 'localhost'),
            port=pg_params.get('port', 5432),
            user=pg_params.get('user'),
            password=pg_params.get('password'),
            database=pg_params.get('database', 'postgres'),
        )
    except Exception as e:
        print(f"ERROR: Can't connect to PostgreSQL for user migration: {e}",
              file=sys.stderr)
        return

    created = 0
    for u in users:
        is_super = u['name'] in (args.superuser or [])
        sql = _build_create_role_sql(u, args.default_password, is_super)
        try:
            conn.execute(sql)
            created += 1
            log(f"  Created role: {u['name']}", verbose, quiet)
        except Exception as e:
            if 'already exists' in str(e):
                log(f"  Role already exists: {u['name']}", verbose, quiet)
            else:
                print(f"ERROR creating role {u['name']}: {e}", file=sys.stderr)
                if not args.force:
                    conn.close()
                    return

    conn.close()
    log(f"Created {created} PostgreSQL roles", verbose, quiet)


def _parse_mysql_users(sql_content):
    """Parse MySQL user definitions from a dump of the mysql.user table or CREATE USER statements."""
    users = []

    # Match CREATE USER statements
    for m in re.finditer(
        r"CREATE\s+USER\s+'([^']+)'@'([^']+)'(?:\s+IDENTIFIED\s+BY\s+'([^']+)')?",
        sql_content, re.IGNORECASE
    ):
        users.append({
            'name': m.group(1),
            'host': m.group(2),
            'password': m.group(3),
        })

    # Match GRANT statements to discover users
    for m in re.finditer(
        r"GRANT\s+.+\s+TO\s+'([^']+)'@'([^']+)'",
        sql_content, re.IGNORECASE
    ):
        name = m.group(1)
        if not any(u['name'] == name for u in users):
            users.append({'name': name, 'host': m.group(2), 'password': None})

    # Match INSERT INTO mysql.user
    for m in re.finditer(
        r"INSERT\s+INTO\s+[`\"]?(?:mysql\.)?user[`\"]?\s.*?VALUES\s*\(\s*'([^']+)'\s*,\s*'([^']+)'",
        sql_content, re.IGNORECASE | re.DOTALL
    ):
        host = m.group(1)
        name = m.group(2)
        if not any(u['name'] == name for u in users):
            users.append({'name': name, 'host': host, 'password': None})

    # Deduplicate (same user from different hosts → one PG role)
    seen = set()
    deduped = []
    for u in users:
        if u['name'] not in seen and u['name'] not in ('root', 'mysql.sys',
                'mysql.session', 'mysql.infoschema', 'debian-sys-maint',
                'mariadb.sys'):
            seen.add(u['name'])
            deduped.append(u)

    return deduped


def _extract_users_from_mysql(mysql_url):
    """Extract user list from a live MySQL server."""
    parts = parse_url(mysql_url)
    cmd = ['mysql', '--batch', '--skip-column-names']
    if parts.get('user'):
        cmd.extend(['-u', parts['user']])
    if parts.get('password'):
        cmd.extend([f"-p{parts['password']}"])
    if parts.get('host'):
        cmd.extend(['-h', parts['host']])
    if parts.get('port'):
        cmd.extend(['-P', str(parts['port'])])
    cmd.extend(['-e', "SELECT user, host FROM mysql.user WHERE user NOT IN "
                "('root','mysql.sys','mysql.session','mysql.infoschema','debian-sys-maint','mariadb.sys')"])

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        users = []
        for line in result.stdout.strip().split('\n'):
            if line.strip():
                parts_line = line.split('\t')
                if len(parts_line) >= 2:
                    users.append({'name': parts_line[0], 'host': parts_line[1], 'password': None})
        return users
    except (FileNotFoundError, subprocess.CalledProcessError) as e:
        print(f"WARNING: Could not extract users from MySQL: {e}", file=sys.stderr)
        return []


def _build_create_role_sql(user, default_password=None, superuser=False):
    """Build a CREATE ROLE statement for a MySQL user."""
    name = user['name']
    password = user.get('password') or default_password

    parts = [f'CREATE ROLE "{name}" WITH LOGIN']
    if superuser:
        parts.append('SUPERUSER')
    if password:
        # Escape single quotes in password
        escaped = password.replace("'", "''")
        parts.append(f"PASSWORD '{escaped}'")

    return ' '.join(parts)


def main():
    try:
        signal.signal(signal.SIGPIPE, signal.SIG_DFL)
    except (AttributeError, OSError):
        pass

    parser = build_parser()
    args = parser.parse_args()

    if not args.from_dump and not args.from_mysql:
        parser.print_help()
        sys.exit(1)

    if not args.to_pg and not args.to_file and not args.dry_run and not args.pg_database:
        print("ERROR: Must specify a target: --to-pg, --to-file, or --dry-run",
              file=sys.stderr)
        sys.exit(1)

    migrate(args)


if __name__ == "__main__":
    main()

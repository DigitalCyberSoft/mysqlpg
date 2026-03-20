"""mysqlpg — MySQL-compatible CLI argument parsing and main orchestration."""

import argparse
import getpass
import os
import signal
import sys

from mysqlpg import __version__
from mysqlpg.connection import Connection
from mysqlpg.formatter import Formatter
from mysqlpg.translator import translate
from mysqlpg.commands import handle_command


def build_parser():
    parser = argparse.ArgumentParser(
        prog="mysqlpg",
        description="MySQL-compatible CLI for PostgreSQL",
        add_help=False,
    )

    # Connection
    parser.add_argument("-u", "--user", default=os.environ.get("PGUSER", os.environ.get("USER", "postgres")),
                        help="PostgreSQL user")
    parser.add_argument("-p", "--password", nargs="?", const="__PROMPT__", default=None,
                        help="Password (prompted if flag given without value)")
    parser.add_argument("-h", "--host", default=os.environ.get("PGHOST", "localhost"),
                        dest="host", help="Server host")
    parser.add_argument("-P", "--port", type=int, default=int(os.environ.get("PGPORT", "5432")),
                        help="Server port (default: 5432)")
    parser.add_argument("-D", "--database", default=None, help="Database to use")
    parser.add_argument("-S", "--socket", default=None, help="Socket file (not used for PG, accepted for compat)")

    # Execution
    parser.add_argument("-e", "--execute", default=None, help="Execute command and quit")
    parser.add_argument("-B", "--batch", action="store_true", help="Batch mode (tab-separated, no borders)")
    parser.add_argument("-N", "--skip-column-names", action="store_true", help="Don't write column names")
    parser.add_argument("-t", "--table", action="store_true", help="Table output format")
    parser.add_argument("-r", "--raw", action="store_true", help="Write fields without conversion")
    parser.add_argument("-s", "--silent", action="store_true", help="Silent mode")
    parser.add_argument("-f", "--force", action="store_true", help="Continue on errors")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose mode")

    # Features
    parser.add_argument("-A", "--no-auto-rehash", action="store_true", help="No auto-rehash")
    parser.add_argument("--delimiter", default=";", help="Statement delimiter")
    parser.add_argument("--pager", default=None, help="Pager command")
    parser.add_argument("--tee", default=None, help="Log output to file")
    parser.add_argument("--prompt", default=None, help="Custom prompt")
    parser.add_argument("--init-command", default=None, help="SQL to execute on connect")
    parser.add_argument("--show-warnings", action="store_true", help="Show warnings after every statement")
    parser.add_argument("-U", "--safe-updates", action="store_true", help="Allow only safe UPDATEs/DELETEs")
    parser.add_argument("--vertical", action="store_true", help="Print results vertically")
    parser.add_argument("--auto-vertical-output", action="store_true", help="Auto-switch to vertical if too wide")

    # Version & help
    parser.add_argument("-V", "--version", action="version", version=f"mysqlpg {__version__}")
    parser.add_argument("--help", action="help", help="Show this help message and exit")

    # Positional database
    parser.add_argument("dbname", nargs="?", default=None, help="Database name")

    return parser


def parse_password_arg(argv):
    """Handle MySQL-style -pSECRET (no space between -p and password)."""
    new_argv = []
    i = 0
    while i < len(argv):
        arg = argv[i]
        if arg.startswith("-p") and len(arg) > 2 and not arg.startswith("--"):
            # -pSECRET → --password SECRET
            new_argv.extend(["--password", arg[2:]])
        else:
            new_argv.append(arg)
        i += 1
    return new_argv


def main():
    # Handle SIGPIPE cleanly
    try:
        signal.signal(signal.SIGPIPE, signal.SIG_DFL)
    except (AttributeError, OSError):
        pass  # Windows

    argv = parse_password_arg(sys.argv[1:])
    parser = build_parser()
    args = parser.parse_args(argv)

    # Resolve database
    database = args.database or args.dbname or os.environ.get("PGDATABASE") or "postgres"

    # Handle password prompt
    password = args.password
    if password == "__PROMPT__":
        password = getpass.getpass("Enter password: ")

    # Connect
    try:
        conn = Connection(
            host=args.host,
            port=args.port,
            user=args.user,
            password=password,
            database=database,
        )
    except Exception as e:
        print(f"ERROR: Can't connect to PostgreSQL server on '{args.host}' ({e})",
              file=sys.stderr)
        sys.exit(1)

    # Build formatter
    is_pipe = not sys.stdin.isatty()
    batch_mode = (args.batch or is_pipe or bool(args.execute)) and not args.table
    formatter = Formatter(
        batch=batch_mode,
        skip_column_names=args.skip_column_names,
        table_mode=args.table,
        raw=args.raw,
        silent=args.silent,
        vertical=args.vertical,
        auto_vertical=args.auto_vertical_output,
        pager=args.pager,
        tee_file=args.tee,
    )

    state = {
        "database": database or conn.get_current_database(),
        "delimiter": args.delimiter,
        "show_warnings": args.show_warnings,
        "force": args.force,
        "exit": False,
        "rehash": False,
    }

    # Init command
    if args.init_command:
        _execute_sql(args.init_command, conn, formatter, state)

    # -e mode: execute and exit
    if args.execute:
        rc = _execute_sql(args.execute, conn, formatter, state)
        formatter.close()
        conn.close()
        sys.exit(rc)

    # Pipe/batch mode: read stdin
    if is_pipe:
        rc = _execute_stdin(conn, formatter, state)
        formatter.close()
        conn.close()
        sys.exit(rc)

    # Interactive mode
    try:
        from mysqlpg.interactive import run_interactive
        run_interactive(conn, formatter, state, args)
    except ImportError as e:
        # Fallback: simple REPL without prompt_toolkit
        print(f"Warning: prompt_toolkit not available ({e}), using basic REPL",
              file=sys.stderr)
        _basic_repl(conn, formatter, state)

    formatter.close()
    conn.close()


def _execute_sql(sql, conn, formatter, state):
    """Execute one or more SQL statements. Returns exit code."""
    delimiter = state.get("delimiter", ";")
    statements = sql.split(delimiter)
    rc = 0

    for stmt in statements:
        stmt = stmt.strip()
        if not stmt:
            continue

        # Skip comment-only lines
        lines = [l for l in stmt.split("\n")
                 if l.strip() and not l.strip().startswith("--")]
        # Strip MySQL conditional comments: /*!...*/ → extract content or skip
        cleaned_lines = []
        for line in lines:
            # Remove /*!NNNNN ... */ conditional comments (treat as no-op)
            import re
            line = re.sub(r'/\*!\d+\s*(.*?)\s*\*/', r'\1', line)
            if line.strip():
                cleaned_lines.append(line)
        stmt = "\n".join(cleaned_lines).strip()
        if not stmt:
            continue

        # Handle \G suffix
        vertical_override = False
        if stmt.endswith("\\G"):
            stmt = stmt[:-2].strip()
            vertical_override = True

        if handle_command(stmt, conn, formatter, state):
            if state.get("exit"):
                return rc
            continue

        try:
            result, is_special = translate(stmt, conn)
            if is_special:
                columns, rows = result
                formatter.print_results(columns, rows, 0.0, vertical_override)
            else:
                # Handle multi-statement translations (e.g., ALTER TABLE CHANGE)
                for sub_sql in result.split(";"):
                    sub_sql = sub_sql.strip()
                    if not sub_sql:
                        continue
                    columns, rows, status, rowcount, elapsed = conn.execute(sub_sql)
                    if columns:
                        formatter.print_results(columns, rows, elapsed, vertical_override)
                    else:
                        formatter.print_status(status, rowcount, elapsed)
        except Exception as e:
            print(f"ERROR: {e}", file=sys.stderr)
            rc = 1
            if not state.get("force"):
                return rc

    return rc


def _execute_stdin(conn, formatter, state):
    """Read and execute SQL from stdin (pipe mode)."""
    content = sys.stdin.read()
    return _execute_sql(content, conn, formatter, state)


def _basic_repl(conn, formatter, state):
    """Minimal REPL fallback when prompt_toolkit is not available."""
    buffer = ""
    delimiter = state.get("delimiter", ";")

    while not state.get("exit"):
        try:
            if buffer:
                prompt = "    -> "
            else:
                prompt = "mysql> "
            line = input(prompt)
        except (EOFError, KeyboardInterrupt):
            print("\nBye")
            break

        buffer += line + "\n"

        if delimiter in buffer:
            parts = buffer.split(delimiter)
            for part in parts[:-1]:
                part = part.strip()
                if part:
                    _execute_sql(part, conn, formatter, state)
                    if state.get("exit"):
                        return
            buffer = parts[-1]


if __name__ == "__main__":
    main()

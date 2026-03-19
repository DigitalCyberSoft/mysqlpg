"""Interactive REPL with prompt_toolkit: autocomplete, history, syntax highlighting."""

import os
import sys
import datetime

from prompt_toolkit import PromptSession
from prompt_toolkit.history import FileHistory
from prompt_toolkit.auto_suggest import AutoSuggestFromHistory
from prompt_toolkit.completion import WordCompleter, Completer, Completion
from prompt_toolkit.lexers import PygmentsLexer
from pygments.lexers.sql import MySqlLexer

from mysqlpg.translator import translate
from mysqlpg.commands import handle_command


# SQL keywords for autocomplete
SQL_KEYWORDS = [
    "SELECT", "FROM", "WHERE", "INSERT", "INTO", "VALUES", "UPDATE", "SET",
    "DELETE", "CREATE", "TABLE", "DROP", "ALTER", "INDEX", "DATABASE",
    "SHOW", "DATABASES", "TABLES", "COLUMNS", "DESC", "DESCRIBE", "EXPLAIN",
    "USE", "STATUS", "SOURCE", "SYSTEM", "TEE", "NOTEE", "PAGER", "NOPAGER",
    "WARNINGS", "NOWARNING", "DELIMITER", "CONNECT", "REHASH", "CLEAR",
    "EXIT", "QUIT", "HELP", "GRANT", "REVOKE", "FLUSH", "PRIVILEGES",
    "JOIN", "LEFT", "RIGHT", "INNER", "OUTER", "ON", "AND", "OR", "NOT",
    "IN", "EXISTS", "BETWEEN", "LIKE", "IS", "NULL", "TRUE", "FALSE",
    "ORDER", "BY", "GROUP", "HAVING", "LIMIT", "OFFSET", "UNION", "ALL",
    "AS", "DISTINCT", "COUNT", "SUM", "AVG", "MIN", "MAX", "IF", "CASE",
    "WHEN", "THEN", "ELSE", "END", "BEGIN", "COMMIT", "ROLLBACK",
    "TRANSACTION", "SAVEPOINT", "PRIMARY", "KEY", "FOREIGN", "REFERENCES",
    "UNIQUE", "CHECK", "DEFAULT", "AUTO_INCREMENT", "NOT", "NULL",
    "VARCHAR", "INT", "INTEGER", "BIGINT", "TEXT", "BOOLEAN", "DATE",
    "DATETIME", "TIMESTAMP", "FLOAT", "DOUBLE", "DECIMAL",
    "SHOW CREATE TABLE", "SHOW DATABASES", "SHOW TABLES", "SHOW PROCESSLIST",
    "SHOW VARIABLES", "SHOW STATUS", "SHOW GRANTS", "SHOW WARNINGS",
    "SHOW ENGINES", "SHOW TABLE STATUS", "SHOW INDEX FROM",
    "SHOW COLUMNS FROM", "SHOW FULL TABLES", "SHOW FULL PROCESSLIST",
    "SHOW FULL COLUMNS FROM", "SHOW CHARACTER SET", "SHOW COLLATION",
    "INSERT IGNORE INTO", "REPLACE INTO", "ON DUPLICATE KEY UPDATE",
    "KILL", "KILL QUERY", "TRUNCATE TABLE", "RENAME TABLE",
    # psql backslash commands
    "\\dt", "\\dt+", "\\d", "\\d+", "\\di", "\\dn", "\\du", "\\dv", "\\ds", "\\df",
    "\\l", "\\l+", "\\x", "\\conninfo", "\\timing", "\\i", "\\o", "\\?",
    # PG-specific SQL
    "EXPLAIN ANALYZE", "EXPLAIN (ANALYZE, BUFFERS)",
    "COPY", "RETURNING", "LATERAL", "MATERIALIZED VIEW",
    "WITH RECURSIVE", "ON CONFLICT", "DO UPDATE SET", "DO NOTHING",
    "FOR UPDATE SKIP LOCKED", "FOR UPDATE NOWAIT", "FOR SHARE",
    "FETCH FIRST", "ROWS ONLY",
]


class MySQLCompleter(Completer):
    """Custom completer with SQL keywords + database objects."""

    def __init__(self):
        self.keywords = sorted(set(SQL_KEYWORDS))
        self.tables = []
        self.columns = {}  # table -> [columns]
        self.databases = []

    def refresh(self, conn):
        """Refresh completion data from the database."""
        try:
            self.databases = conn.get_databases()
        except Exception:
            self.databases = []
        try:
            self.tables = conn.get_tables()
        except Exception:
            self.tables = []
        self.columns = {}
        for table in self.tables:
            try:
                self.columns[table] = conn.get_columns(table)
            except Exception:
                pass

    def get_completions(self, document, complete_event):
        word = document.get_word_before_cursor(WORD=False)
        text = document.text_before_cursor
        word_upper = word.upper()

        if not word:
            return

        # All candidates
        candidates = []

        # Keywords
        for kw in self.keywords:
            if kw.upper().startswith(word_upper):
                candidates.append(kw)

        # Tables
        for t in self.tables:
            if t.lower().startswith(word.lower()):
                candidates.append(t)

        # Databases
        for d in self.databases:
            if d.lower().startswith(word.lower()):
                candidates.append(d)

        # Columns (from all tables or context-aware)
        for table, cols in self.columns.items():
            for c in cols:
                if c.lower().startswith(word.lower()):
                    candidates.append(c)

        seen = set()
        for c in candidates:
            if c not in seen:
                seen.add(c)
                yield Completion(c, start_position=-len(word))


def expand_prompt(template, conn, state):
    """Expand MySQL prompt escape sequences."""
    if not template:
        db = state.get("database", "(none)")
        return f"mysql [{db}]> "

    result = template
    result = result.replace("\\u", conn.user or "")
    result = result.replace("\\h", conn.host or "")
    result = result.replace("\\d", state.get("database", "(none)"))
    result = result.replace("\\p", str(conn.port))
    result = result.replace("\\D", datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    return result


def run_interactive(conn, formatter, state, args):
    """Run the interactive REPL with prompt_toolkit."""
    history_file = os.path.expanduser("~/.mysqlpg_history")

    completer = MySQLCompleter()
    if not args.no_auto_rehash:
        try:
            completer.refresh(conn)
        except Exception:
            pass

    session = PromptSession(
        history=FileHistory(history_file),
        auto_suggest=AutoSuggestFromHistory(),
        completer=completer,
        lexer=PygmentsLexer(MySqlLexer),
        multiline=False,
    )

    # Print welcome
    version = conn.get_server_version_string()
    conn_id = conn.get_connection_id()
    print(f"Welcome to mysqlpg {__import__('mysqlpg').__version__} (PostgreSQL {version})")
    print(f"Connection id:\t\t{conn_id}")
    print(f"Current database:\t{state.get('database', '(none)')}")
    print()
    print('Type "help;" or "\\h" for help. Type "\\c" to clear the current input statement.')
    print()

    buffer = ""
    delimiter = state.get("delimiter", ";")

    while not state.get("exit"):
        try:
            if buffer:
                prompt_str = "    -> "
            else:
                prompt_str = expand_prompt(args.prompt if hasattr(args, 'prompt') else None,
                                           conn, state)

            line = session.prompt(prompt_str)
        except KeyboardInterrupt:
            # Ctrl+C at the prompt (no query running) → exit
            if buffer:
                # If mid-input, just clear buffer
                buffer = ""
                print("\n^C")
                continue
            print("\nBye")
            state["exit"] = True
            return
        except EOFError:
            print("\nBye")
            break

        # Handle rehash
        if state.get("rehash"):
            try:
                completer.refresh(conn)
            except Exception:
                pass
            state["rehash"] = False

        # Handle commands that work without a delimiter (exit, quit, \q, etc.)
        stripped_line = line.strip().rstrip(";").strip()
        stripped_upper = stripped_line.upper()
        if not buffer and stripped_upper in ("EXIT", "QUIT", "\\Q"):
            formatter.print_message("Bye")
            state["exit"] = True
            return
        if not buffer and stripped_upper in ("CLEAR", "\\C"):
            continue
        # Handle USE, STATUS, HELP etc. without delimiter when on a single line
        if not buffer and stripped_line:
            # Check if it's a meta-command that doesn't need delimiter
            if handle_command(stripped_line, conn, formatter, state):
                if state.get("exit"):
                    return
                if state.get("rehash"):
                    try:
                        completer.refresh(conn)
                    except Exception:
                        pass
                    state["rehash"] = False
                continue

        # Check for \G at end of line
        vertical_override = False
        stripped_line = line.rstrip()
        if stripped_line.endswith("\\G"):
            line = stripped_line[:-2]
            vertical_override = True
            # Also treat \G as a delimiter
            buffer += line
            _process_buffer(buffer.strip(), conn, formatter, state, vertical_override)
            buffer = ""
            continue

        # Check for \c (clear buffer)
        if stripped_line.rstrip().endswith("\\c"):
            buffer = ""
            print()
            continue

        buffer += line + "\n"
        delimiter = state.get("delimiter", ";")

        # Check if buffer contains delimiter
        if delimiter in buffer:
            parts = buffer.split(delimiter)
            for part in parts[:-1]:
                part = part.strip()
                if part:
                    _process_buffer(part, conn, formatter, state, False)
                    if state.get("exit"):
                        return
                    # Handle rehash after USE
                    if state.get("rehash"):
                        try:
                            completer.refresh(conn)
                        except Exception:
                            pass
                        state["rehash"] = False
            buffer = parts[-1]
            if buffer.strip() == "":
                buffer = ""


def _process_buffer(sql, conn, formatter, state, vertical_override):
    """Process a complete SQL statement."""
    if not sql:
        return

    # Meta-commands first
    if handle_command(sql, conn, formatter, state):
        return

    try:
        result, is_special = translate(sql, conn)
        if is_special:
            columns, rows = result
            formatter.print_results(columns, rows, 0.0, vertical_override)
        else:
            # Handle multi-statement translations
            for sub_sql in result.split(";"):
                sub_sql = sub_sql.strip()
                if not sub_sql:
                    continue
                columns, rows, status, rowcount, elapsed = conn.execute(sub_sql)
                if columns:
                    formatter.print_results(columns, rows, elapsed, vertical_override)
                else:
                    formatter.print_status(status, rowcount, elapsed)

                # Show warnings if enabled
                if state.get("show_warnings"):
                    notices = conn.pop_notices()
                    for n in notices:
                        formatter.print_message(n)
    except KeyboardInterrupt:
        # Ctrl+C during query execution — cancel and return to prompt
        try:
            conn.conn.cancel()
        except Exception:
            pass
        formatter.print_message("^C — query cancelled")
    except Exception as e:
        formatter.print_message(f"ERROR: {e}")

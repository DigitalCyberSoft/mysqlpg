"""Meta-command handling (USE, STATUS, SOURCE, TEE, etc.)."""

import os
import subprocess
import sys
import re


def handle_command(sql, conn, formatter, state):
    """Try to handle sql as a meta-command.

    Returns True if handled, False if it should be processed as SQL.
    `state` is a dict with mutable session state.
    """
    stripped = sql.strip().rstrip(";").strip()
    upper = stripped.upper()

    # USE database
    m = re.match(r"USE\s+([`\"\w]+)", stripped, re.IGNORECASE)
    if m:
        db = m.group(1).strip("`'\"")
        try:
            conn.reconnect(database=db)
            formatter.print_message("Database changed")
            state["database"] = db
            # Refresh autocomplete
            state["rehash"] = True
        except Exception as e:
            formatter.print_message(f"ERROR: {e}")
        return True

    # STATUS / \s
    if upper in ("STATUS", "\\S"):
        _print_status(conn, formatter)
        return True

    # SOURCE file / \. file
    m = re.match(r"(?:SOURCE|\\.)\s+(.+)", stripped, re.IGNORECASE)
    if m:
        filepath = m.group(1).strip().strip("'\"")
        _source_file(filepath, conn, formatter, state)
        return True

    # SYSTEM cmd / \! cmd
    m = re.match(r"(?:SYSTEM|\\!)\s+(.+)", stripped, re.IGNORECASE)
    if m:
        cmd = m.group(1)
        try:
            subprocess.run(cmd, shell=True)
        except Exception as e:
            formatter.print_message(f"ERROR: {e}")
        return True

    # TEE file
    m = re.match(r"TEE\s+(.+)", stripped, re.IGNORECASE)
    if m:
        filepath = m.group(1).strip().strip("'\"")
        formatter.start_tee(filepath)
        formatter.print_message(f"Logging to file '{filepath}'")
        return True

    # NOTEE
    if upper == "NOTEE" or upper == "\\T":
        formatter.stop_tee()
        formatter.print_message("Outfile disabled.")
        return True

    # PAGER cmd
    m = re.match(r"PAGER\s*(.*)", stripped, re.IGNORECASE)
    if m and upper.startswith("PAGER"):
        cmd = m.group(1).strip()
        if cmd:
            formatter.set_pager(cmd)
            formatter.print_message(f"PAGER set to '{cmd}'")
        else:
            formatter.set_pager("less")
            formatter.print_message("PAGER set to 'less'")
        return True

    # NOPAGER
    if upper == "NOPAGER" or upper == "\\N":
        formatter.clear_pager()
        formatter.print_message("PAGER set to stdout")
        return True

    # WARNINGS
    if upper == "WARNINGS" or upper == "\\W":
        state["show_warnings"] = True
        formatter.print_message("Show warnings enabled.")
        return True

    # NOWARNING
    if upper == "NOWARNING" or upper == "\\W":
        state["show_warnings"] = False
        formatter.print_message("Show warnings disabled.")
        return True

    # DELIMITER str
    m = re.match(r"DELIMITER\s+(\S+)", stripped, re.IGNORECASE)
    if m:
        state["delimiter"] = m.group(1)
        return True

    # CONNECT [db [host]] / \r
    m = re.match(r"(?:CONNECT|\\r)(?:\s+(\S+)(?:\s+(\S+))?)?", stripped, re.IGNORECASE)
    if m and (upper.startswith("CONNECT") or upper.startswith("\\R")):
        db = m.group(1) or conn.database
        host = m.group(2) or conn.host
        try:
            if db:
                conn.database = db
            if host:
                conn.host = host
            conn.reconnect()
            formatter.print_message(
                f"Connection id:    {conn.get_connection_id()}\n"
                f"Current database: {conn.get_current_database()}"
            )
            state["database"] = conn.database
            state["rehash"] = True
        except Exception as e:
            formatter.print_message(f"ERROR: {e}")
        return True

    # REHASH / \#
    if upper in ("REHASH", "\\#"):
        state["rehash"] = True
        return True

    # CLEAR / \c
    if upper in ("CLEAR", "\\C"):
        state["clear_buffer"] = True
        return True

    # EXIT / QUIT / \q
    if upper in ("EXIT", "QUIT", "\\Q"):
        formatter.print_message("Bye")
        state["exit"] = True
        return True

    # HELP / \h / \?
    if upper in ("HELP", "\\H", "\\?") or upper.startswith("HELP"):
        _print_help(formatter)
        return True

    # SET autocommit
    m = re.match(r"SET\s+autocommit\s*=\s*(\S+)", stripped, re.IGNORECASE)
    if m:
        val = m.group(1).strip()
        if val in ("0", "OFF", "off", "false"):
            conn.set_autocommit(False)
            formatter.print_message("Query OK, 0 rows affected")
        else:
            conn.set_autocommit(True)
            formatter.print_message("Query OK, 0 rows affected")
        return True

    return False


def _print_status(conn, formatter):
    """Print MySQL-style STATUS information."""
    try:
        conn_id = conn.get_connection_id()
        db = conn.get_current_database()
        user = conn.get_current_user()
        version = conn.get_server_version_string()
        uptime = conn.get_uptime()
    except Exception as e:
        formatter.print_message(f"ERROR: {e}")
        return

    lines = [
        "--------------",
        "mysqlpg  Ver 0.1.0 for PostgreSQL",
        "",
        f"Connection id:\t\t{conn_id}",
        f"Current database:\t{db}",
        f"Current user:\t\t{user}",
        f"Server version:\t\t{version} (PostgreSQL)",
        f"Protocol version:\t3",
        f"Connection:\t\t{conn.host} via TCP/IP",
        f"Server characterset:\tutf8",
        f"Db     characterset:\tutf8",
        f"Client characterset:\tutf8",
        f"Conn.  characterset:\tutf8",
        f"TCP port:\t\t{conn.port}",
        f"Uptime:\t\t\t{uptime}",
        "--------------",
    ]
    formatter.print_message("\n".join(lines))


def _source_file(filepath, conn, formatter, state):
    """Execute SQL from a file."""
    path = os.path.expanduser(filepath)
    if not os.path.isfile(path):
        formatter.print_message(f"ERROR: Failed to open file '{path}'")
        return

    try:
        with open(path, "r") as f:
            content = f.read()
    except IOError as e:
        formatter.print_message(f"ERROR: {e}")
        return

    delimiter = state.get("delimiter", ";")
    statements = content.split(delimiter)

    from mysqlpg.translator import translate

    for stmt in statements:
        stmt = stmt.strip()
        if not stmt:
            continue

        # Check for meta-commands first
        if handle_command(stmt, conn, formatter, state):
            if state.get("exit"):
                return
            continue

        try:
            result, is_special = translate(stmt, conn)
            if is_special:
                columns, rows = result
                formatter.print_results(columns, rows)
            else:
                columns, rows, status, rowcount, elapsed = conn.execute(result)
                if columns:
                    formatter.print_results(columns, rows, elapsed)
                else:
                    formatter.print_status(status, rowcount, elapsed)
        except Exception as e:
            formatter.print_message(f"ERROR: {e}")
            if not state.get("force"):
                return


def _print_help(formatter):
    """Print help text."""
    text = """List of mysqlpg commands:
?         (\\?) Synonym for 'help'.
clear     (\\c) Clear the current input statement.
connect   (\\r) Reconnect to the server.
delimiter (\\d) Set statement delimiter.
exit      (\\q) Exit mysqlpg. Same as quit.
help      (\\h) Display this help.
nopager   (\\n) Disable pager, print to stdout.
notee     (\\t) Don't write into outfile.
pager     (\\P) Set PAGER. Print results through PAGER.
quit      (\\q) Quit mysqlpg.
rehash    (\\#) Rebuild completion hash.
source    (\\.) Execute an SQL script file.
status    (\\s) Get status information from the server.
system    (\\!) Execute a system shell command.
tee       (\\T) Set outfile. Append everything into given outfile.
use       (\\u) Use another database.
warnings  (\\W) Show warnings after every statement."""
    formatter.print_message(text)

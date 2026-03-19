"""Meta-command handling (USE, STATUS, SOURCE, TEE, etc.)."""

import os
import shlex
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

    # SOURCE file / \. file (literal dot, not any char)
    m = re.match(r"(?:SOURCE|\\\.)\s+(.+)", stripped, re.IGNORECASE)
    if m:
        filepath = m.group(1).strip().strip("'\"")
        _source_file(filepath, conn, formatter, state)
        return True

    # SYSTEM cmd / \! cmd
    m = re.match(r"(?:SYSTEM|\\!)\s+(.+)", stripped, re.IGNORECASE)
    if m:
        cmd = m.group(1)
        try:
            subprocess.run(shlex.split(cmd))
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

    # --- psql backslash commands ---
    if stripped.startswith("\\") and len(stripped) >= 2:
        return _handle_psql_command(stripped, conn, formatter, state)

    return False


def _handle_psql_command(cmd, conn, formatter, state):
    """Handle psql-style backslash commands."""
    import time

    # \x — toggle expanded (vertical) output
    if cmd.strip() in ("\\x", "\\x on", "\\x off"):
        if cmd.strip() == "\\x off":
            formatter.vertical = False
            formatter.print_message("Expanded display is off.")
        elif cmd.strip() == "\\x on":
            formatter.vertical = True
            formatter.print_message("Expanded display is on.")
        else:
            formatter.vertical = not getattr(formatter, 'vertical', False)
            mode = "on" if formatter.vertical else "off"
            formatter.print_message(f"Expanded display is {mode}.")
        return True

    # \l — list databases (= SHOW DATABASES)
    if cmd.strip() == "\\l" or cmd.strip().startswith("\\l+"):
        try:
            cols, rows, status, rc, elapsed = conn.execute(
                "SELECT datname AS \"Name\", pg_catalog.pg_get_userbyid(datdba) AS \"Owner\", "
                "pg_catalog.pg_encoding_to_char(encoding) AS \"Encoding\" "
                "FROM pg_database WHERE datistemplate = false ORDER BY datname"
            )
            formatter.print_results(cols, rows, elapsed)
        except Exception as e:
            formatter.print_message(f"ERROR: {e}")
        return True

    # \dt — list tables
    m = re.match(r"\\dt\+?\s*(.*)", cmd)
    if m:
        pattern = m.group(1).strip()
        verbose = "+" in cmd[:4]
        q = "SELECT tablename AS \"Name\", tableowner AS \"Owner\""
        if verbose:
            q += ", pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS \"Size\""
        q += " FROM pg_tables WHERE schemaname NOT IN ('pg_catalog','information_schema')"
        if pattern:
            q += f" AND tablename LIKE '{pattern.replace('*', '%')}'"
        q += " ORDER BY tablename"
        try:
            cols, rows, status, rc, elapsed = conn.execute(q)
            formatter.print_results(cols, rows, elapsed)
        except Exception as e:
            formatter.print_message(f"ERROR: {e}")
        return True

    # \di — list indexes
    m = re.match(r"\\di\+?\s*(.*)", cmd)
    if m:
        pattern = m.group(1).strip()
        q = ("SELECT indexname AS \"Name\", tablename AS \"Table\", indexdef AS \"Definition\" "
             "FROM pg_indexes WHERE schemaname NOT IN ('pg_catalog','information_schema')")
        if pattern:
            q += f" AND indexname LIKE '{pattern.replace('*', '%')}'"
        q += " ORDER BY tablename, indexname"
        try:
            cols, rows, status, rc, elapsed = conn.execute(q)
            formatter.print_results(cols, rows, elapsed)
        except Exception as e:
            formatter.print_message(f"ERROR: {e}")
        return True

    # \d tablename — describe table (= DESC tablename)
    m = re.match(r"\\d\+?\s+(\S+)", cmd)
    if m:
        table = m.group(1).strip("'\"`;")
        verbose = "+" in cmd[:3]
        try:
            q = f"""
            SELECT c.column_name AS "Column", c.data_type AS "Type",
                   CASE WHEN c.is_nullable = 'YES' THEN 'YES' ELSE 'NO' END AS "Nullable",
                   c.column_default AS "Default"
            """
            if verbose:
                q += """,
                   COALESCE(pgd.description, '') AS "Comment"
                """
            q += f"""
            FROM information_schema.columns c
            """
            if verbose:
                q += f"""
                LEFT JOIN pg_catalog.pg_statio_all_tables st
                    ON st.relname = c.table_name AND st.schemaname = c.table_schema
                LEFT JOIN pg_catalog.pg_description pgd
                    ON pgd.objoid = st.relid AND pgd.objsubid = c.ordinal_position
                """
            q += f"""
            WHERE c.table_name = '{table}' AND c.table_schema NOT IN ('pg_catalog','information_schema')
            ORDER BY c.ordinal_position
            """
            cols, rows, status, rc, elapsed = conn.execute(q)
            if rows:
                formatter.print_results(cols, rows, elapsed)
            else:
                formatter.print_message(f"Did not find any relation named \"{table}\".")
        except Exception as e:
            formatter.print_message(f"ERROR: {e}")
        return True

    # \d (no args) — list tables+views+sequences
    if cmd.strip() in ("\\d", "\\d+"):
        try:
            cols, rows, status, rc, elapsed = conn.execute(
                "SELECT c.relname AS \"Name\", "
                "CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' "
                "WHEN 'S' THEN 'sequence' WHEN 'm' THEN 'materialized view' "
                "WHEN 'f' THEN 'foreign table' WHEN 'p' THEN 'partitioned table' ELSE c.relkind::text END AS \"Type\", "
                "pg_catalog.pg_get_userbyid(c.relowner) AS \"Owner\" "
                "FROM pg_catalog.pg_class c "
                "LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
                "WHERE c.relkind IN ('r','v','S','m','f','p') "
                "AND n.nspname NOT IN ('pg_catalog','information_schema') "
                "ORDER BY c.relname"
            )
            formatter.print_results(cols, rows, elapsed)
        except Exception as e:
            formatter.print_message(f"ERROR: {e}")
        return True

    # \dn — list schemas
    if cmd.strip().startswith("\\dn"):
        try:
            cols, rows, status, rc, elapsed = conn.execute(
                "SELECT schema_name AS \"Name\", schema_owner AS \"Owner\" "
                "FROM information_schema.schemata ORDER BY schema_name"
            )
            formatter.print_results(cols, rows, elapsed)
        except Exception as e:
            formatter.print_message(f"ERROR: {e}")
        return True

    # \du — list roles
    if cmd.strip().startswith("\\du"):
        try:
            cols, rows, status, rc, elapsed = conn.execute(
                "SELECT rolname AS \"Role name\", "
                "CASE WHEN rolsuper THEN 'Superuser' ELSE '' END || "
                "CASE WHEN rolcreaterole THEN ', Create role' ELSE '' END || "
                "CASE WHEN rolcreatedb THEN ', Create DB' ELSE '' END || "
                "CASE WHEN rolcanlogin THEN ', Login' ELSE '' END AS \"Attributes\" "
                "FROM pg_roles WHERE rolname NOT LIKE 'pg_%' ORDER BY rolname"
            )
            formatter.print_results(cols, rows, elapsed)
        except Exception as e:
            formatter.print_message(f"ERROR: {e}")
        return True

    # \dv — list views
    if cmd.strip().startswith("\\dv"):
        try:
            cols, rows, status, rc, elapsed = conn.execute(
                "SELECT viewname AS \"Name\", viewowner AS \"Owner\" "
                "FROM pg_views WHERE schemaname NOT IN ('pg_catalog','information_schema') "
                "ORDER BY viewname"
            )
            formatter.print_results(cols, rows, elapsed)
        except Exception as e:
            formatter.print_message(f"ERROR: {e}")
        return True

    # \ds — list sequences
    if cmd.strip().startswith("\\ds"):
        try:
            cols, rows, status, rc, elapsed = conn.execute(
                "SELECT c.relname AS \"Name\", pg_catalog.pg_get_userbyid(c.relowner) AS \"Owner\" "
                "FROM pg_catalog.pg_class c "
                "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
                "WHERE c.relkind = 'S' AND n.nspname NOT IN ('pg_catalog','information_schema') "
                "ORDER BY c.relname"
            )
            formatter.print_results(cols, rows, elapsed)
        except Exception as e:
            formatter.print_message(f"ERROR: {e}")
        return True

    # \df — list functions
    if cmd.strip().startswith("\\df"):
        try:
            cols, rows, status, rc, elapsed = conn.execute(
                "SELECT p.proname AS \"Name\", "
                "pg_catalog.pg_get_function_result(p.oid) AS \"Result\", "
                "pg_catalog.pg_get_function_arguments(p.oid) AS \"Arguments\" "
                "FROM pg_catalog.pg_proc p "
                "JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace "
                "WHERE n.nspname = 'public' ORDER BY p.proname"
            )
            formatter.print_results(cols, rows, elapsed)
        except Exception as e:
            formatter.print_message(f"ERROR: {e}")
        return True

    # \c dbname — connect to database (psql style)
    m = re.match(r"\\c\s+(\S+)", cmd)
    if m:
        db = m.group(1).strip("'\"`;")
        try:
            conn.reconnect(database=db)
            formatter.print_message(f"You are now connected to database \"{db}\".")
            state["database"] = db
            state["rehash"] = True
        except Exception as e:
            formatter.print_message(f"ERROR: {e}")
        return True

    # \conninfo — connection info
    if cmd.strip() == "\\conninfo":
        db = conn.get_current_database()
        user = conn.get_current_user()
        formatter.print_message(
            f"You are connected to database \"{db}\" as user \"{user}\" "
            f"on host \"{conn.host}\" at port \"{conn.port}\"."
        )
        return True

    # \timing — toggle timing display (no-op for now, we always show timing)
    if cmd.strip().startswith("\\timing"):
        formatter.print_message("Timing is on.")
        return True

    # \i file — include/execute file (= SOURCE)
    m = re.match(r"\\i\s+(.+)", cmd)
    if m:
        filepath = m.group(1).strip().strip("'\"")
        from mysqlpg.commands import _source_file
        _source_file(filepath, conn, formatter, state)
        return True

    # \copy — client-side COPY (pass through to PG)
    if cmd.strip().startswith("\\copy"):
        formatter.print_message("ERROR: \\copy requires psql. Use COPY ... FROM/TO instead.")
        return True

    # \e — edit (not supported)
    if cmd.strip().startswith("\\e"):
        formatter.print_message("ERROR: \\e (edit) is not supported in mysqlpg.")
        return True

    # \o — output to file (similar to TEE)
    m = re.match(r"\\o\s*(.*)", cmd)
    if m:
        filepath = m.group(1).strip()
        if filepath:
            formatter.start_tee(filepath)
            formatter.print_message(f"Output to file '{filepath}'")
        else:
            formatter.stop_tee()
            formatter.print_message("Output to stdout.")
        return True

    # \? — psql help
    if cmd.strip() == "\\?":
        _print_psql_help(formatter)
        return True

    # Unknown backslash command
    formatter.print_message(f"Invalid command: {cmd.split()[0]}. Try \\? for help.")
    return True


def _print_psql_help(formatter):
    """Print psql-compatible backslash command help."""
    text = """Supported psql commands:
  \\l          List databases
  \\dt[+]      List tables (+ for sizes)
  \\di         List indexes
  \\d name     Describe table/view
  \\d          List all relations
  \\dn         List schemas
  \\du         List roles
  \\dv         List views
  \\ds         List sequences
  \\df         List functions
  \\x          Toggle expanded (vertical) display
  \\conninfo   Show connection info
  \\i file     Execute SQL from file
  \\o [file]   Send output to file (no arg = stdout)
  \\timing     Toggle timing display
  \\?          Show this help

MySQL commands also supported:
  SHOW DATABASES / TABLES / CREATE TABLE / COLUMNS / INDEX
  DESC table / USE db / STATUS / SOURCE file
  HELP / EXIT / QUIT
  PAGER / TEE / DELIMITER / WARNINGS"""
    formatter.print_message(text)


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
    text = """MySQL commands:
  ?         (\\?) Synonym for 'help'.
  clear     (\\c) Clear the current input statement.
  connect   (\\r) Reconnect to the server.
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
  warnings  (\\W) Show warnings after every statement.
  SHOW DATABASES / TABLES / CREATE TABLE / INDEX / STATUS
  DESC table / DELIMITER

psql commands:
  \\l          List databases
  \\dt[+]      List tables (+ for sizes)
  \\di         List indexes
  \\d name     Describe table/view (native PG types)
  \\d          List all relations
  \\dn         List schemas
  \\du         List roles
  \\dv         List views
  \\ds         List sequences
  \\df         List functions
  \\c dbname   Connect to database
  \\x          Toggle expanded (vertical) display
  \\conninfo   Show connection info
  \\i file     Execute SQL from file
  \\o [file]   Send output to file (no arg = stdout)
  \\timing     Toggle timing display"""
    formatter.print_message(text)

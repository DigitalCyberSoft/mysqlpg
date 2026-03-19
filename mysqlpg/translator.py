"""MySQL → PostgreSQL SQL translation via regex pattern matching."""

import re
from mysqlpg.ddl import show_create_table


def translate(sql, conn):
    """Translate MySQL-syntax SQL to PostgreSQL-compatible SQL.

    Returns (translated_sql, is_special) where is_special=True means
    the result is already (columns, rows) and should not be executed.
    """
    stripped = sql.strip().rstrip(";").strip()
    if not stripped:
        return sql, False

    # Try each translator in order
    for pattern, handler in _TRANSLATORS:
        m = pattern.match(stripped)
        if m:
            result = handler(m, conn, stripped)
            if result is None:
                break  # Fall through to general translation
            # Apply function translation to non-special results
            if not result[1]:  # not is_special
                translated = _translate_functions(result[0])
                translated = _fix_zero_dates(translated)
                return translated, False
            return result

    # No special translation — full pipeline
    converted = _convert_backticks(sql)
    converted = _translate_functions(converted)
    converted = _fix_zero_dates(converted)
    return converted, False


def _fix_zero_dates(sql):
    """Convert MySQL zero dates to NULL for PostgreSQL compatibility."""
    # '0000-00-00 00:00:00' → NULL
    sql = re.sub(r"'0000-00-00 00:00:00'", "NULL", sql)
    # '0000-00-00' → NULL
    sql = re.sub(r"'0000-00-00'", "NULL", sql)
    return sql


# --- MySQL date format → PG TO_CHAR format mapping ---
_MYSQL_TO_PG_DATE_FORMAT = {
    '%Y': 'YYYY', '%y': 'YY', '%m': 'MM', '%c': 'FMMM',
    '%M': 'Month', '%b': 'Mon', '%d': 'DD', '%e': 'FMDD',
    '%H': 'HH24', '%h': 'HH12', '%I': 'HH12',
    '%i': 'MI', '%s': 'SS', '%S': 'SS', '%p': 'AM',
    '%W': 'Day', '%a': 'Dy', '%j': 'DDD', '%V': 'IW',
    '%f': 'US', '%T': 'HH24:MI:SS', '%r': 'HH12:MI:SS AM',
}


def _convert_date_format(mysql_fmt):
    """Convert MySQL DATE_FORMAT format string to PG TO_CHAR format string."""
    result = mysql_fmt
    # Sort by length descending to avoid partial replacements
    for mysql_spec, pg_spec in sorted(_MYSQL_TO_PG_DATE_FORMAT.items(),
                                       key=lambda x: -len(x[0])):
        result = result.replace(mysql_spec, pg_spec)
    return result


def _find_matching_paren(sql, start):
    """Find the matching closing parenthesis for the one at position start."""
    depth = 0
    in_sq = False
    in_dq = False
    for i in range(start, len(sql)):
        ch = sql[i]
        if ch == "'" and not in_dq:
            in_sq = not in_sq
        elif ch == '"' and not in_sq:
            in_dq = not in_dq
        elif not in_sq and not in_dq:
            if ch == '(':
                depth += 1
            elif ch == ')':
                depth -= 1
                if depth == 0:
                    return i
    return -1


def _translate_functions(sql):
    """Translate MySQL-specific functions and syntax to PostgreSQL equivalents.

    This runs on all SQL that passes through to PG, handling inline function
    calls and MySQL-specific syntax patterns.
    """
    if not sql or not sql.strip():
        return sql

    # --- String functions ---

    # GROUP_CONCAT(col SEPARATOR ',') → STRING_AGG(col::text, ',')
    # GROUP_CONCAT(col ORDER BY x SEPARATOR ',') → STRING_AGG(col::text, ',' ORDER BY x)
    # GROUP_CONCAT(DISTINCT col SEPARATOR ',') → STRING_AGG(DISTINCT col::text, ',')
    def _rewrite_group_concat(m):
        inner = m.group(1).strip()
        # Parse: [DISTINCT] expr [ORDER BY ...] [SEPARATOR 'sep']
        distinct = ''
        if re.match(r'DISTINCT\s+', inner, re.IGNORECASE):
            distinct = 'DISTINCT '
            inner = re.sub(r'^DISTINCT\s+', '', inner, flags=re.IGNORECASE)

        sep = ","  # default separator
        sep_m = re.search(r"\bSEPARATOR\s+'([^']*)'", inner, re.IGNORECASE)
        if sep_m:
            sep = sep_m.group(1)
            inner = inner[:sep_m.start()].strip()

        order_by = ''
        order_m = re.search(r'\bORDER\s+BY\s+(.+)', inner, re.IGNORECASE)
        if order_m:
            order_by = ' ORDER BY ' + order_m.group(1).strip()
            inner = inner[:order_m.start()].strip()

        expr = inner.strip().rstrip(',')
        return f"STRING_AGG({distinct}{expr}::text, '{sep}'{order_by})"

    sql = re.sub(
        r'\bGROUP_CONCAT\s*\(([^)]*(?:\([^)]*\))*[^)]*)\)',
        _rewrite_group_concat, sql, flags=re.IGNORECASE
    )

    # LOCATE(substr, str) → POSITION(substr IN str)
    sql = re.sub(
        r'\bLOCATE\s*\(\s*([^,]+?)\s*,\s*([^,)]+)\s*\)',
        r'POSITION(\1 IN \2)', sql, flags=re.IGNORECASE
    )

    # INSTR(str, substr) → POSITION(substr IN str)  [args reversed]
    sql = re.sub(
        r'\bINSTR\s*\(\s*([^,]+?)\s*,\s*([^,)]+)\s*\)',
        r'POSITION(\2 IN \1)', sql, flags=re.IGNORECASE
    )

    # CHAR(n) → CHR(n) -- only as standalone function call, not in type context (AS CHAR)
    def _rewrite_char(m):
        # Check if preceded by AS (type context like CAST(x AS CHAR(100)))
        start = m.start()
        prefix = sql[max(0, start-4):start].strip().upper()
        if prefix.endswith('AS'):
            return m.group(0)  # keep as type
        return f'CHR({m.group(1)})'
    sql = re.sub(r'\bCHAR\s*\((\s*\d+\s*)\)', _rewrite_char, sql, flags=re.IGNORECASE)

    # SPACE(n) → REPEAT(' ', n)
    sql = re.sub(r'\bSPACE\s*\(([^)]+)\)', r"REPEAT(' ', \1)", sql, flags=re.IGNORECASE)

    # HEX(s) → ENCODE(s::bytea, 'hex')
    sql = re.sub(r'\bHEX\s*\(([^)]+)\)', r"ENCODE((\1)::bytea, 'hex')", sql, flags=re.IGNORECASE)

    # UNHEX(s) → DECODE(s, 'hex')
    sql = re.sub(r'\bUNHEX\s*\(([^)]+)\)', r"DECODE(\1, 'hex')", sql, flags=re.IGNORECASE)

    # --- Numeric functions ---

    # RAND() → RANDOM()
    sql = re.sub(r'\bRAND\s*\(\s*\)', 'RANDOM()', sql, flags=re.IGNORECASE)

    # TRUNCATE(n, d) → TRUNC(n, d)  -- careful: not TRUNCATE TABLE
    sql = re.sub(r'\bTRUNCATE\s*\(([^,]+),\s*([^)]+)\)', r'TRUNC(\1, \2)', sql, flags=re.IGNORECASE)

    # LOG(n) with one arg → LN(n) in MySQL (natural log)
    # LOG(base, n) with two args → LOG(base, n) in PG (same)
    # We only convert single-arg LOG to LN
    def _rewrite_log(m):
        inner = m.group(1)
        # Check if there's a comma (two args)
        if ',' in inner:
            return f'LOG({inner})'  # two-arg: keep as LOG
        return f'LN({inner})'  # one-arg: MySQL LOG = natural log = PG LN
    sql = re.sub(r'\bLOG\s*\(([^)]+)\)', _rewrite_log, sql, flags=re.IGNORECASE)

    # LOG2(n) → LOG(2, n)
    sql = re.sub(r'\bLOG2\s*\(([^)]+)\)', r'LOG(2, \1)', sql, flags=re.IGNORECASE)

    # LOG10(n) → LOG(10, n)  -- PG LOG() is base-10, but explicit is clearer
    sql = re.sub(r'\bLOG10\s*\(([^)]+)\)', r'LOG(10, \1)', sql, flags=re.IGNORECASE)

    # --- Date/Time functions ---

    # DATE_FORMAT(col, 'fmt') → TO_CHAR(col, 'pg_fmt')
    def _rewrite_date_format(m):
        expr = m.group(1).strip()
        fmt = m.group(2)
        pg_fmt = _convert_date_format(fmt)
        return f"TO_CHAR({expr}, '{pg_fmt}')"
    sql = re.sub(
        r"\bDATE_FORMAT\s*\(\s*(.+?)\s*,\s*'([^']*)'\s*\)",
        _rewrite_date_format, sql, flags=re.IGNORECASE
    )

    # STR_TO_DATE(str, fmt) → TO_TIMESTAMP(str, pg_fmt)
    def _rewrite_str_to_date(m):
        expr = m.group(1).strip()
        fmt = m.group(2)
        pg_fmt = _convert_date_format(fmt)
        return f"TO_TIMESTAMP({expr}, '{pg_fmt}')"
    sql = re.sub(
        r"\bSTR_TO_DATE\s*\(\s*(.+?)\s*,\s*'([^']*)'\s*\)",
        _rewrite_str_to_date, sql, flags=re.IGNORECASE
    )

    # CURDATE() → CURRENT_DATE
    sql = re.sub(r'\bCURDATE\s*\(\s*\)', 'CURRENT_DATE', sql, flags=re.IGNORECASE)

    # CURTIME() → CURRENT_TIME
    sql = re.sub(r'\bCURTIME\s*\(\s*\)', 'CURRENT_TIME', sql, flags=re.IGNORECASE)

    # SYSDATE() → CLOCK_TIMESTAMP()
    sql = re.sub(r'\bSYSDATE\s*\(\s*\)', 'CLOCK_TIMESTAMP()', sql, flags=re.IGNORECASE)

    # UNIX_TIMESTAMP() → EXTRACT(EPOCH FROM NOW())
    sql = re.sub(r'\bUNIX_TIMESTAMP\s*\(\s*\)', "EXTRACT(EPOCH FROM NOW())::bigint", sql, flags=re.IGNORECASE)

    # UNIX_TIMESTAMP(col) → EXTRACT(EPOCH FROM col)
    sql = re.sub(
        r'\bUNIX_TIMESTAMP\s*\(([^)]+)\)',
        r'EXTRACT(EPOCH FROM \1)::bigint', sql, flags=re.IGNORECASE
    )

    # FROM_UNIXTIME(n) → TO_TIMESTAMP(n)
    sql = re.sub(r'\bFROM_UNIXTIME\s*\(([^)]+)\)', r'TO_TIMESTAMP(\1)', sql, flags=re.IGNORECASE)

    # DATEDIFF(date1, date2) → (date1::date - date2::date)
    sql = re.sub(
        r'\bDATEDIFF\s*\(\s*([^,]+?)\s*,\s*([^)]+)\s*\)',
        r'((\1)::date - (\2)::date)', sql, flags=re.IGNORECASE
    )

    # DATE_ADD(date, INTERVAL n unit) → (date + INTERVAL 'n unit')
    def _rewrite_date_add(m):
        expr = m.group(1).strip()
        interval = m.group(2).strip()
        return f"({expr} + {interval})"
    sql = re.sub(
        r'\bDATE_ADD\s*\(\s*(.+?)\s*,\s*(INTERVAL\s+.+?)\s*\)',
        _rewrite_date_add, sql, flags=re.IGNORECASE
    )

    # DATE_SUB(date, INTERVAL n unit) → (date - INTERVAL 'n unit')
    def _rewrite_date_sub(m):
        expr = m.group(1).strip()
        interval = m.group(2).strip()
        return f"({expr} - {interval})"
    sql = re.sub(
        r'\bDATE_SUB\s*\(\s*(.+?)\s*,\s*(INTERVAL\s+.+?)\s*\)',
        _rewrite_date_sub, sql, flags=re.IGNORECASE
    )

    # YEAR(col) → EXTRACT(YEAR FROM col)
    # But NOT YEAR(4) as a type definition — skip if arg is a small integer literal
    for unit in ('YEAR', 'MONTH', 'DAY', 'HOUR', 'MINUTE', 'SECOND'):
        def _make_extract_rewriter(u):
            def _rewrite(m):
                arg = m.group(1).strip()
                # Skip if it's a type-width integer (e.g., YEAR(4), DAY(2))
                if re.match(r'^\d{1,2}$', arg):
                    return m.group(0)
                return f'EXTRACT({u} FROM {arg})'
            return _rewrite
        sql = re.sub(
            rf'\b{unit}\s*\(([^)]+)\)',
            _make_extract_rewriter(unit),
            sql, flags=re.IGNORECASE
        )

    # DAYOFWEEK(col) → EXTRACT(DOW FROM col) + 1  (MySQL: 1=Sunday, PG DOW: 0=Sunday)
    sql = re.sub(
        r'\bDAYOFWEEK\s*\(([^)]+)\)',
        r'(EXTRACT(DOW FROM \1) + 1)',
        sql, flags=re.IGNORECASE
    )

    # DAYOFMONTH(col) → EXTRACT(DAY FROM col)
    sql = re.sub(
        r'\bDAYOFMONTH\s*\(([^)]+)\)',
        r'EXTRACT(DAY FROM \1)',
        sql, flags=re.IGNORECASE
    )

    # DAYOFYEAR(col) → EXTRACT(DOY FROM col)
    sql = re.sub(
        r'\bDAYOFYEAR\s*\(([^)]+)\)',
        r'EXTRACT(DOY FROM \1)',
        sql, flags=re.IGNORECASE
    )

    # WEEKOFYEAR(col) / WEEK(col) → EXTRACT(WEEK FROM col)
    sql = re.sub(
        r'\bWEEKOFYEAR\s*\(([^)]+)\)',
        r'EXTRACT(WEEK FROM \1)',
        sql, flags=re.IGNORECASE
    )
    sql = re.sub(
        r'\bWEEK\s*\(([^)]+)\)',
        r'EXTRACT(WEEK FROM \1)',
        sql, flags=re.IGNORECASE
    )

    # LAST_DAY(col) → (DATE_TRUNC('month', col) + INTERVAL '1 month' - INTERVAL '1 day')::date
    sql = re.sub(
        r'\bLAST_DAY\s*\(([^)]+)\)',
        r"(DATE_TRUNC('month', \1) + INTERVAL '1 month' - INTERVAL '1 day')::date",
        sql, flags=re.IGNORECASE
    )

    # DATE(col) → (col)::date
    sql = re.sub(r'\bDATE\s*\(([^)]+)\)', r'(\1)::date', sql, flags=re.IGNORECASE)

    # TIME(col) → (col)::time
    sql = re.sub(r'\bTIME\s*\(([^)]+)\)', r'(\1)::time', sql, flags=re.IGNORECASE)

    # --- Conditional functions ---

    # IF(cond, true, false) → CASE WHEN cond THEN true ELSE false END
    def _rewrite_if(m):
        # Simple case: IF(a, b, c) with no nested parens
        inner = m.group(1)
        parts = _split_args(inner)
        if len(parts) == 3:
            return f"CASE WHEN {parts[0].strip()} THEN {parts[1].strip()} ELSE {parts[2].strip()} END"
        return m.group(0)  # can't parse, leave as-is
    sql = re.sub(
        r'\bIF\s*\((.+?)\)(?=[\s,;)"\']|$)',
        _rewrite_if, sql, flags=re.IGNORECASE
    )

    # ISNULL(col) → (col IS NULL)
    sql = re.sub(r'\bISNULL\s*\(([^)]+)\)', r'(\1 IS NULL)', sql, flags=re.IGNORECASE)

    # --- Information functions ---

    # USER() → current_user
    sql = re.sub(r'\bUSER\s*\(\s*\)', 'current_user', sql, flags=re.IGNORECASE)

    # VERSION() → version()
    sql = re.sub(r'\bVERSION\s*\(\s*\)', 'version()', sql, flags=re.IGNORECASE)

    # LAST_INSERT_ID() → lastval()
    sql = re.sub(r'\bLAST_INSERT_ID\s*\(\s*\)', 'lastval()', sql, flags=re.IGNORECASE)

    # FOUND_ROWS() → strip (no PG equivalent; warn via comment)
    sql = re.sub(r'\bFOUND_ROWS\s*\(\s*\)', '0 /* FOUND_ROWS() not supported */', sql, flags=re.IGNORECASE)

    # SQL_CALC_FOUND_ROWS → strip
    sql = re.sub(r'\bSQL_CALC_FOUND_ROWS\b', '/* SQL_CALC_FOUND_ROWS removed */', sql, flags=re.IGNORECASE)

    # --- Regex operators ---

    # NOT REGEXP → !~* (case-insensitive by default like MySQL)
    sql = re.sub(r'\bNOT\s+REGEXP\b', '!~*', sql, flags=re.IGNORECASE)
    sql = re.sub(r'\bNOT\s+RLIKE\b', '!~*', sql, flags=re.IGNORECASE)

    # REGEXP / RLIKE → ~* (case-insensitive like MySQL default)
    sql = re.sub(r'\bREGEXP\b', '~*', sql, flags=re.IGNORECASE)
    sql = re.sub(r'\bRLIKE\b', '~*', sql, flags=re.IGNORECASE)

    # --- Query modifiers to strip ---

    # STRAIGHT_JOIN → strip (keep as regular JOIN-like query)
    sql = re.sub(r'\bSTRAIGHT_JOIN\b', 'JOIN', sql, flags=re.IGNORECASE)

    # INSERT LOW_PRIORITY/DELAYED/HIGH_PRIORITY → strip modifier
    sql = re.sub(r'\bINSERT\s+(LOW_PRIORITY|DELAYED|HIGH_PRIORITY)\s+', 'INSERT ', sql, flags=re.IGNORECASE)

    # LOCK IN SHARE MODE → FOR SHARE
    sql = re.sub(r'\bLOCK\s+IN\s+SHARE\s+MODE\b', 'FOR SHARE', sql, flags=re.IGNORECASE)

    # --- LIMIT offset,count → LIMIT count OFFSET offset ---
    def _rewrite_limit_comma(m):
        offset = m.group(1).strip()
        count = m.group(2).strip()
        return f"LIMIT {count} OFFSET {offset}"
    sql = re.sub(
        r'\bLIMIT\s+(\d+)\s*,\s*(\d+)',
        _rewrite_limit_comma, sql, flags=re.IGNORECASE
    )

    # IFNULL → COALESCE (also done in _TRANSLATORS but needed here for passthrough)
    sql = re.sub(r'\bIFNULL\s*\(', 'COALESCE(', sql, flags=re.IGNORECASE)

    # DATABASE() → current_database() (also in _TRANSLATORS but needed for inline)
    sql = re.sub(r'\bDATABASE\s*\(\s*\)', 'current_database()', sql, flags=re.IGNORECASE)

    # --- NULL-safe equality ---
    # <=> → IS NOT DISTINCT FROM
    sql = re.sub(r'\s*<=>\s*', ' IS NOT DISTINCT FROM ', sql)

    # --- FROM DUAL → strip ---
    sql = re.sub(r'\bFROM\s+DUAL\b', '', sql, flags=re.IGNORECASE)

    # --- SELECT modifiers to strip ---
    # SQL_NO_CACHE, SQL_CACHE, SQL_BUFFER_RESULT, SQL_SMALL_RESULT, SQL_BIG_RESULT, HIGH_PRIORITY
    for mod in ('SQL_NO_CACHE', 'SQL_CACHE', 'SQL_BUFFER_RESULT',
                'SQL_SMALL_RESULT', 'SQL_BIG_RESULT', 'HIGH_PRIORITY'):
        sql = re.sub(rf'\b{mod}\b\s*', '', sql, flags=re.IGNORECASE)

    # --- CAST type translations ---
    # CAST(x AS UNSIGNED) → CAST(x AS BIGINT)
    sql = re.sub(r'\bCAST\s*\((.+?)\s+AS\s+UNSIGNED\s*\)',
                 r'CAST(\1 AS BIGINT)', sql, flags=re.IGNORECASE)
    sql = re.sub(r'\bCAST\s*\((.+?)\s+AS\s+UNSIGNED\s+INTEGER\s*\)',
                 r'CAST(\1 AS BIGINT)', sql, flags=re.IGNORECASE)
    # CAST(x AS SIGNED) → CAST(x AS INTEGER)
    sql = re.sub(r'\bCAST\s*\((.+?)\s+AS\s+SIGNED\s*\)',
                 r'CAST(\1 AS INTEGER)', sql, flags=re.IGNORECASE)
    sql = re.sub(r'\bCAST\s*\((.+?)\s+AS\s+SIGNED\s+INTEGER\s*\)',
                 r'CAST(\1 AS INTEGER)', sql, flags=re.IGNORECASE)
    # CAST(x AS CHAR) → CAST(x AS TEXT) when no length specified
    sql = re.sub(r'\bCAST\s*\((.+?)\s+AS\s+CHAR\s*\)',
                 r'CAST(\1 AS TEXT)', sql, flags=re.IGNORECASE)
    # CAST(x AS DATETIME) → CAST(x AS TIMESTAMP)
    sql = re.sub(r'\bCAST\s*\((.+?)\s+AS\s+DATETIME\s*\)',
                 r'CAST(\1 AS TIMESTAMP)', sql, flags=re.IGNORECASE)

    # CONVERT(expr USING charset) → just the expr (charset handled by client_encoding)
    # MUST be before CONVERT(expr, type) to avoid mis-matching
    sql = re.sub(
        r'\bCONVERT\s*\(\s*([^)]+?)\s+USING\s+\w+\s*\)',
        r'\1', sql, flags=re.IGNORECASE
    )

    # CONVERT(expr, type) → CAST(expr AS type)
    def _rewrite_convert(m):
        expr = m.group(1).strip()
        typ = m.group(2).strip()
        type_map = {
            'UNSIGNED': 'BIGINT', 'SIGNED': 'INTEGER',
            'CHAR': 'TEXT', 'DATETIME': 'TIMESTAMP',
        }
        pg_type = type_map.get(typ.upper(), typ)
        return f'CAST({expr} AS {pg_type})'
    sql = re.sub(
        r'\bCONVERT\s*\(\s*(.+?)\s*,\s*(\w+)\s*\)',
        _rewrite_convert, sql, flags=re.IGNORECASE
    )

    # --- FIELD() → ARRAY_POSITION() ---
    def _rewrite_field(m):
        inner = m.group(1)
        args = _split_args(inner)
        if len(args) < 2:
            return m.group(0)
        val = args[0].strip()
        elements = ', '.join(a.strip() for a in args[1:])
        return f'ARRAY_POSITION(ARRAY[{elements}], {val})'
    sql = re.sub(
        r'\bFIELD\s*\(([^)]+)\)',
        _rewrite_field, sql, flags=re.IGNORECASE
    )

    # --- ELT(n, s1, s2, ...) → (ARRAY[s1, s2, ...])[n] ---
    def _rewrite_elt(m):
        inner = m.group(1)
        args = _split_args(inner)
        if len(args) < 2:
            return m.group(0)
        idx = args[0].strip()
        elements = ', '.join(a.strip() for a in args[1:])
        return f'(ARRAY[{elements}])[{idx}]'
    sql = re.sub(
        r'\bELT\s*\(([^)]+)\)',
        _rewrite_elt, sql, flags=re.IGNORECASE
    )

    # --- FIND_IN_SET(val, set_expr) → val = ANY(string_to_array(set_expr, ',')) ---
    # Handles both FIND_IN_SET('val', 'a,b,c') and FIND_IN_SET('val', col)
    def _rewrite_find_in_set(m):
        val = m.group(1).strip()
        set_expr = m.group(2).strip()
        return f"{val} = ANY(string_to_array({set_expr}, ','))"
    sql = re.sub(
        r"\bFIND_IN_SET\s*\(\s*(.+?)\s*,\s*([^)]+)\s*\)",
        _rewrite_find_in_set, sql, flags=re.IGNORECASE
    )

    # --- SOUNDS LIKE → SOUNDEX comparison (requires fuzzystrmatch) ---
    sql = re.sub(
        r'\b(\w+)\s+SOUNDS\s+LIKE\s+',
        r'SOUNDEX(\1) = SOUNDEX(', sql, flags=re.IGNORECASE
    )

    # --- MariaDB MINUS → EXCEPT ---
    sql = re.sub(r'\bMINUS\b(?!\s*\()', 'EXCEPT', sql, flags=re.IGNORECASE)

    # --- Index hints: USE INDEX / FORCE INDEX / IGNORE INDEX → strip ---
    sql = re.sub(
        r'\b(?:USE|FORCE|IGNORE)\s+INDEX\s*(?:\s+FOR\s+(?:JOIN|ORDER\s+BY|GROUP\s+BY))?\s*\([^)]*\)',
        '', sql, flags=re.IGNORECASE
    )

    # --- WITH ROLLUP → ROLLUP() ---
    # GROUP BY a, b WITH ROLLUP → GROUP BY ROLLUP(a, b)
    def _rewrite_with_rollup(m):
        cols = m.group(1).strip()
        return f'GROUP BY ROLLUP({cols})'
    sql = re.sub(
        r'\bGROUP\s+BY\s+(.+?)\s+WITH\s+ROLLUP\b',
        _rewrite_with_rollup, sql, flags=re.IGNORECASE
    )

    # --- STRCMP(s1, s2) → CASE comparison ---
    sql = re.sub(
        r'\bSTRCMP\s*\(\s*(.+?)\s*,\s*(.+?)\s*\)',
        r'CASE WHEN \1 < \2 THEN -1 WHEN \1 > \2 THEN 1 ELSE 0 END',
        sql, flags=re.IGNORECASE
    )

    # Backtick conversion
    sql = _convert_backticks(sql)

    return sql


def _split_args(s):
    """Split function arguments respecting parentheses and quotes."""
    parts = []
    depth = 0
    current = []
    in_sq = False
    in_dq = False
    for ch in s:
        if ch == "'" and not in_dq:
            in_sq = not in_sq
            current.append(ch)
        elif ch == '"' and not in_sq:
            in_dq = not in_dq
            current.append(ch)
        elif not in_sq and not in_dq:
            if ch == '(':
                depth += 1
                current.append(ch)
            elif ch == ')':
                depth -= 1
                current.append(ch)
            elif ch == ',' and depth == 0:
                parts.append(''.join(current))
                current = []
            else:
                current.append(ch)
        else:
            current.append(ch)
    if current:
        parts.append(''.join(current))
    return parts


def _convert_backticks(sql):
    """Replace backtick-quoted identifiers with double-quoted identifiers."""
    result = []
    in_single = False
    in_double = False
    i = 0
    while i < len(sql):
        ch = sql[i]
        if ch == "'" and not in_double:
            in_single = not in_single
            result.append(ch)
        elif ch == '"' and not in_single:
            in_double = not in_double
            result.append(ch)
        elif ch == '`' and not in_single and not in_double:
            # Find matching backtick
            end = sql.find('`', i + 1)
            if end >= 0:
                identifier = sql[i + 1:end]
                result.append('"')
                result.append(identifier)
                result.append('"')
                i = end
            else:
                result.append(ch)
        else:
            result.append(ch)
        i += 1
    return "".join(result)


# --- SHOW command handlers ---

def _show_databases(m, conn, sql):
    like = m.group("like")
    q = "SELECT datname AS \"Database\" FROM pg_database WHERE datistemplate = false"
    if like:
        q += f" AND datname LIKE '{like}'"
    q += " ORDER BY datname"
    return q, False


def _show_tables(m, conn, sql):
    full = m.group("full")
    from_db = m.group("from_db")
    like = m.group("like")

    schema = "public"
    schema_filter = "table_schema NOT IN ('pg_catalog','information_schema')"

    if from_db:
        # In PG, "FROM db" doesn't directly map; use schema
        schema_filter = f"table_schema NOT IN ('pg_catalog','information_schema')"

    if full:
        q = (
            f"SELECT table_name AS \"Tables_in_{conn.database or 'db'}\", "
            f"table_type AS \"Table_type\" "
            f"FROM information_schema.tables "
            f"WHERE {schema_filter}"
        )
    else:
        q = (
            f"SELECT table_name AS \"Tables_in_{conn.database or 'db'}\" "
            f"FROM information_schema.tables "
            f"WHERE {schema_filter}"
        )

    if like:
        q += f" AND table_name LIKE '{like}'"
    q += " ORDER BY table_name"
    return q, False


def _desc_table(m, conn, sql):
    table = m.group("table").strip("`'\"")

    q = f"""
    SELECT
        c.column_name AS "Field",
        CASE
            WHEN c.data_type = 'integer' THEN 'int(11)'
            WHEN c.data_type = 'bigint' THEN 'bigint(20)'
            WHEN c.data_type = 'smallint' THEN 'smallint(6)'
            WHEN c.data_type = 'boolean' THEN 'tinyint(1)'
            WHEN c.data_type = 'character varying' THEN 'varchar(' || c.character_maximum_length || ')'
            WHEN c.data_type = 'character' THEN 'char(' || COALESCE(c.character_maximum_length::text, '1') || ')'
            WHEN c.data_type = 'numeric' THEN 'decimal(' || COALESCE(c.numeric_precision::text,'10') || ',' || COALESCE(c.numeric_scale::text,'0') || ')'
            WHEN c.data_type = 'timestamp without time zone' THEN 'datetime'
            WHEN c.data_type = 'timestamp with time zone' THEN 'datetime'
            WHEN c.data_type = 'text' THEN 'text'
            WHEN c.data_type = 'bytea' THEN 'blob'
            WHEN c.data_type = 'double precision' THEN 'double'
            WHEN c.data_type = 'real' THEN 'float'
            WHEN c.data_type = 'json' THEN 'json'
            WHEN c.data_type = 'jsonb' THEN 'json'
            WHEN c.data_type = 'uuid' THEN 'char(36)'
            WHEN c.data_type = 'date' THEN 'date'
            WHEN c.data_type = 'time without time zone' THEN 'time'
            ELSE c.data_type
        END AS "Type",
        CASE WHEN c.is_nullable = 'YES' THEN 'YES' ELSE 'NO' END AS "Null",
        CASE
            WHEN tc.constraint_type = 'PRIMARY KEY' THEN 'PRI'
            WHEN tc.constraint_type = 'UNIQUE' THEN 'UNI'
            WHEN ix.indexname IS NOT NULL THEN 'MUL'
            ELSE ''
        END AS "Key",
        CASE
            WHEN c.column_default LIKE 'nextval%%' THEN 'NULL'
            ELSE COALESCE(
                regexp_replace(c.column_default, '::[a-z_ ]+', '', 'g'),
                'NULL'
            )
        END AS "Default",
        CASE
            WHEN c.column_default LIKE 'nextval%%' THEN 'auto_increment'
            ELSE ''
        END AS "Extra"
    FROM information_schema.columns c
    LEFT JOIN (
        SELECT kcu.column_name, tc.constraint_type
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        WHERE tc.table_name = '{table}'
            AND tc.table_schema = 'public'
            AND tc.constraint_type IN ('PRIMARY KEY', 'UNIQUE')
    ) tc ON tc.column_name = c.column_name
    LEFT JOIN (
        SELECT DISTINCT indexname, unnest(string_to_array(
            regexp_replace(indexdef, '.*\\((.*)\\)', '\\1'), ', '
        )) AS col_name
        FROM pg_indexes
        WHERE tablename = '{table}' AND schemaname = 'public'
    ) ix ON ix.col_name = c.column_name AND tc.constraint_type IS NULL
    WHERE c.table_name = '{table}' AND c.table_schema = 'public'
    ORDER BY c.ordinal_position
    """
    return q, False


def _show_full_columns(m, conn, sql):
    table = m.group("table").strip("`'\"")
    like = m.group("like")

    q = f"""
    SELECT
        c.column_name AS "Field",
        CASE
            WHEN c.data_type = 'integer' THEN 'int(11)'
            WHEN c.data_type = 'character varying' THEN 'varchar(' || c.character_maximum_length || ')'
            WHEN c.data_type = 'text' THEN 'text'
            WHEN c.data_type = 'boolean' THEN 'tinyint(1)'
            WHEN c.data_type = 'timestamp without time zone' THEN 'datetime'
            ELSE c.data_type
        END AS "Type",
        c.collation_name AS "Collation",
        CASE WHEN c.is_nullable = 'YES' THEN 'YES' ELSE 'NO' END AS "Null",
        '' AS "Key",
        COALESCE(c.column_default, 'NULL') AS "Default",
        '' AS "Extra",
        'select,insert,update,references' AS "Privileges",
        COALESCE(pgd.description, '') AS "Comment"
    FROM information_schema.columns c
    LEFT JOIN pg_catalog.pg_statio_all_tables st
        ON st.relname = c.table_name AND st.schemaname = c.table_schema
    LEFT JOIN pg_catalog.pg_description pgd
        ON pgd.objoid = st.relid AND pgd.objsubid = c.ordinal_position
    WHERE c.table_name = '{table}' AND c.table_schema = 'public'
    """
    if like:
        q += f" AND c.column_name LIKE '{like}'"
    q += " ORDER BY c.ordinal_position"
    return q, False


def _show_create_table(m, conn, sql):
    table = m.group("table").strip("`'\"")
    try:
        tname, ddl = show_create_table(conn, table)
        columns = ["Table", "Create Table"]
        rows = [(tname, ddl)]
        return (columns, rows), True
    except Exception as e:
        raise Exception(f"Table '{table}' doesn't exist") from e


def _show_index(m, conn, sql):
    table = m.group("table").strip("`'\"")
    q = f"""
    SELECT
        '{table}' AS "Table",
        CASE WHEN ix.indisunique THEN 0 ELSE 1 END AS "Non_unique",
        i.relname AS "Key_name",
        row_number() OVER (PARTITION BY i.relname ORDER BY a.attnum) AS "Seq_in_index",
        a.attname AS "Column_name",
        NULL AS "Collation",
        0 AS "Cardinality",
        NULL AS "Sub_part",
        NULL AS "Packed",
        CASE WHEN a.attnotnull THEN '' ELSE 'YES' END AS "Null",
        am.amname AS "Index_type",
        '' AS "Comment"
    FROM pg_index ix
    JOIN pg_class t ON t.oid = ix.indrelid
    JOIN pg_class i ON i.oid = ix.indexrelid
    JOIN pg_namespace n ON n.oid = t.relnamespace
    JOIN pg_am am ON am.oid = i.relam
    JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
    WHERE t.relname = '{table}' AND n.nspname = 'public'
    ORDER BY i.relname, a.attnum
    """
    return q, False


def _show_table_status(m, conn, sql):
    like = m.group("like")
    q = """
    SELECT
        c.relname AS "Name",
        'PostgreSQL' AS "Engine",
        NULL AS "Version",
        NULL AS "Row_format",
        c.reltuples::bigint AS "Rows",
        CASE WHEN c.reltuples > 0
            THEN (pg_total_relation_size(c.oid) / GREATEST(c.reltuples::bigint, 1))
            ELSE 0
        END AS "Avg_row_length",
        pg_relation_size(c.oid) AS "Data_length",
        0 AS "Max_data_length",
        pg_indexes_size(c.oid) AS "Index_length",
        0 AS "Data_free",
        COALESCE(
            (SELECT last_value FROM pg_sequences
             WHERE sequencename = pg_get_serial_sequence(c.relname, a.attname)::regclass::text),
            NULL
        ) AS "Auto_increment",
        NULL AS "Create_time",
        NULL AS "Update_time",
        NULL AS "Check_time",
        'utf8mb4_general_ci' AS "Collation",
        NULL AS "Checksum",
        '' AS "Create_options",
        COALESCE(obj_description(c.oid), '') AS "Comment"
    FROM pg_class c
    LEFT JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0
        AND (pg_get_expr(d.adbin, d.adrelid) LIKE 'nextval%%')
    LEFT JOIN pg_attrdef d ON d.adrelid = c.oid AND d.adnum = a.attnum
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relkind IN ('r', 'p')
      AND n.nspname NOT IN ('pg_catalog', 'information_schema')
    """
    if like:
        q += f" AND c.relname LIKE '{like}'"
    q += " ORDER BY c.relname"
    # Simplify - just get basic info
    q = """
    SELECT
        c.relname AS "Name",
        'PostgreSQL' AS "Engine",
        10 AS "Version",
        'Dynamic' AS "Row_format",
        c.reltuples::bigint AS "Rows",
        0 AS "Avg_row_length",
        pg_relation_size(c.oid) AS "Data_length",
        0 AS "Max_data_length",
        pg_indexes_size(c.oid) AS "Index_length",
        0 AS "Data_free",
        NULL AS "Auto_increment",
        NULL AS "Create_time",
        NULL AS "Update_time",
        NULL AS "Check_time",
        'utf8mb4_general_ci' AS "Collation",
        NULL AS "Checksum",
        '' AS "Create_options",
        COALESCE(obj_description(c.oid), '') AS "Comment"
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relkind IN ('r', 'p')
      AND n.nspname NOT IN ('pg_catalog', 'information_schema')
    """
    if like:
        q += f" AND c.relname LIKE '{like}'"
    q += " ORDER BY c.relname"
    return q, False


def _show_processlist(m, conn, sql):
    full = m.group("full")
    info_col = "query" if full else "LEFT(query, 100)"
    q = f"""
    SELECT
        pid AS "Id",
        usename AS "User",
        client_addr::text || ':' || COALESCE(client_port::text, '') AS "Host",
        datname AS "db",
        CASE
            WHEN state = 'active' THEN 'Query'
            WHEN state = 'idle' THEN 'Sleep'
            WHEN state = 'idle in transaction' THEN 'Sleep'
            ELSE COALESCE(state, 'Connect')
        END AS "Command",
        EXTRACT(EPOCH FROM (now() - query_start))::int AS "Time",
        state AS "State",
        {info_col} AS "Info"
    FROM pg_stat_activity
    WHERE pid != pg_backend_pid()
    ORDER BY pid
    """
    return q, False


def _show_variables(m, conn, sql):
    like = m.group("like")
    q = 'SELECT name AS "Variable_name", setting AS "Value" FROM pg_settings'
    if like:
        q += f" WHERE name LIKE '{like}'"
    q += " ORDER BY name"
    return q, False


def _show_status(m, conn, sql):
    global_flag = m.group("global")
    like = m.group("like")

    if global_flag:
        q = """
        SELECT 'Uptime' AS "Variable_name",
               EXTRACT(EPOCH FROM (now() - pg_postmaster_start_time()))::bigint::text AS "Value"
        UNION ALL
        SELECT 'Threads_connected',
               count(*)::text FROM pg_stat_activity
        UNION ALL
        SELECT 'Questions',
               COALESCE(sum(xact_commit + xact_rollback)::text, '0')
               FROM pg_stat_database
        """
    else:
        q = """
        SELECT s.key AS "Variable_name", s.value AS "Value"
        FROM (
            SELECT 'Uptime' AS key,
                   EXTRACT(EPOCH FROM (now() - pg_postmaster_start_time()))::bigint::text AS value
            UNION ALL
            SELECT 'Threads_connected', count(*)::text FROM pg_stat_activity
        ) s
        """

    if like:
        q = f"SELECT * FROM ({q}) sub WHERE \"Variable_name\" LIKE '{like}'"
    return q, False


def _show_grants(m, conn, sql):
    user = m.group("user")
    if user:
        user = user.strip("'\"").split("@")[0].strip("'\"")
    else:
        user = conn.get_current_user()

    q = f"""
    SELECT
        'GRANT ' || privilege_type || ' ON ' || table_schema || '.' || table_name
        || ' TO ' || grantee AS "Grants for {user}"
    FROM information_schema.role_table_grants
    WHERE grantee = '{user}'
    ORDER BY table_schema, table_name, privilege_type
    """
    return q, False


def _show_warnings(m, conn, sql):
    notices = conn.pop_notices()
    if notices:
        columns = ["Level", "Code", "Message"]
        rows = [("Warning", "0", n) for n in notices]
        return (columns, rows), True
    else:
        columns = ["Level", "Code", "Message"]
        rows = []
        return (columns, rows), True


def _show_engines(m, conn, sql):
    columns = ["Engine", "Support", "Comment", "Transactions", "XA", "Savepoints"]
    rows = [("PostgreSQL", "DEFAULT", "PostgreSQL storage engine", "YES", "NO", "YES")]
    return (columns, rows), True


def _show_engine_status(m, conn, sql):
    q = """
    SELECT
        'PostgreSQL' AS "Type",
        '' AS "Name",
        'Active connections: ' || (SELECT count(*) FROM pg_stat_activity)
        || E'\\nLocks: ' || (SELECT count(*) FROM pg_locks)
        AS "Status"
    """
    return q, False


def _show_charset(m, conn, sql):
    q = """
    SELECT
        character_set_name AS "Charset",
        default_collate_name AS "Description",
        default_collate_name AS "Default collation",
        1 AS "Maxlen"
    FROM information_schema.character_sets
    ORDER BY character_set_name
    """
    return q, False


def _show_collation(m, conn, sql):
    like = m.group("like")
    q = """
    SELECT
        collname AS "Collation",
        '' AS "Charset",
        0 AS "Id",
        'Yes' AS "Default",
        'Yes' AS "Compiled",
        1 AS "Sortlen"
    FROM pg_collation
    WHERE collnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'pg_catalog')
    """
    if like:
        q += f" AND collname LIKE '{like}'"
    q += " ORDER BY collname"
    return q, False


def _show_create_database(m, conn, sql):
    db = m.group("db").strip("`'\"")
    q = f"""
    SELECT
        datname,
        'CREATE DATABASE `' || datname || '` /*!40100 DEFAULT CHARACTER SET utf8mb4 */'
    FROM pg_database
    WHERE datname = '{db}'
    """
    return q, False


# --- DML translations ---

def _insert_ignore(m, conn, sql):
    table = m.group("table").strip("`'\"")
    rest = m.group("rest")
    translated = f"INSERT INTO {table} {rest} ON CONFLICT DO NOTHING"
    return _convert_backticks(translated), False


def _on_duplicate_key(m, conn, sql):
    table_match = re.match(
        r"INSERT\s+INTO\s+([`\"\w.]+)\s*(.*?)\s*ON\s+DUPLICATE\s+KEY\s+UPDATE\s+(.*)",
        sql.strip().rstrip(";"), re.IGNORECASE | re.DOTALL
    )
    if not table_match:
        return _convert_backticks(sql), False

    table = table_match.group(1).strip("`'\"")
    insert_part = table_match.group(2)
    update_part = table_match.group(3)

    # Get PK columns
    pk_cols = conn.get_primary_key_columns(table)
    if not pk_cols:
        return _convert_backticks(sql), False

    conflict_cols = ", ".join(pk_cols)

    # Replace VALUES(col) with EXCLUDED.col
    update_translated = re.sub(
        r"VALUES\s*\(\s*([`\"\w]+)\s*\)",
        lambda mm: f"EXCLUDED.{mm.group(1).strip('`' + chr(34))}",
        update_part,
        flags=re.IGNORECASE
    )

    result = f"INSERT INTO \"{table}\" {insert_part} ON CONFLICT ({conflict_cols}) DO UPDATE SET {update_translated}"
    return _convert_backticks(result), False


def _replace_into(m, conn, sql):
    table = m.group("table").strip("`'\"")
    rest = m.group("rest")

    # Get PK and all columns
    pk_cols = conn.get_primary_key_columns(table)
    if not pk_cols:
        # No PK, just do a regular insert
        return _convert_backticks(f"INSERT INTO \"{table}\" {rest}"), False

    # Get all columns to build SET clause for non-PK columns
    all_cols = conn.get_columns(table)
    non_pk = [c for c in all_cols if c not in pk_cols]

    if not non_pk:
        conflict_cols = ", ".join(pk_cols)
        return _convert_backticks(
            f"INSERT INTO \"{table}\" {rest} ON CONFLICT ({conflict_cols}) DO NOTHING"
        ), False

    conflict_cols = ", ".join(pk_cols)
    set_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in non_pk)
    result = f"INSERT INTO \"{table}\" {rest} ON CONFLICT ({conflict_cols}) DO UPDATE SET {set_clause}"
    return _convert_backticks(result), False


# --- DDL translations ---

def _create_database(m, conn, sql):
    db = m.group("db").strip("`'\"")
    # Strip CHARACTER SET / COLLATE clauses
    result = f'CREATE DATABASE "{db}"'
    charset_match = re.search(r"CHARACTER\s+SET\s+\S+", sql, re.IGNORECASE)
    collate_match = re.search(r"COLLATE\s+\S+", sql, re.IGNORECASE)
    if charset_match or collate_match:
        result += " ENCODING 'UTF8'"
    return result, False


def _alter_table_modify(m, conn, sql):
    table = m.group("table").strip("`'\"")
    column = m.group("column").strip("`'\"")
    new_type = m.group("type").strip()
    return _convert_backticks(
        f'ALTER TABLE "{table}" ALTER COLUMN "{column}" TYPE {new_type}'
    ), False


def _alter_table_change(m, conn, sql):
    table = m.group("table").strip("`'\"")
    old_col = m.group("old_col").strip("`'\"")
    new_col = m.group("new_col").strip("`'\"")
    new_type = m.group("type").strip()
    stmts = [
        f'ALTER TABLE "{table}" RENAME COLUMN "{old_col}" TO "{new_col}"',
        f'ALTER TABLE "{table}" ALTER COLUMN "{new_col}" TYPE {new_type}',
    ]
    return "; ".join(stmts), False


def _alter_table_add_index(m, conn, sql):
    table = m.group("table").strip("`'\"")
    unique = m.group("unique") or ""
    idx_name = m.group("idx_name").strip("`'\"")
    columns = m.group("columns")
    unique_kw = "UNIQUE " if unique.strip().upper() == "UNIQUE" else ""
    return _convert_backticks(
        f'CREATE {unique_kw}INDEX "{idx_name}" ON "{table}" ({columns})'
    ), False


def _alter_table_drop_index(m, conn, sql):
    idx_name = m.group("idx_name").strip("`'\"")
    return f'DROP INDEX "{idx_name}"', False


def _rename_table(m, conn, sql):
    old = m.group("old").strip("`'\"")
    new = m.group("new").strip("`'\"")
    return f'ALTER TABLE "{old}" RENAME TO "{new}"', False


def _truncate_table(m, conn, sql):
    table = m.group("table").strip("`'\"")
    return f'TRUNCATE TABLE "{table}" RESTART IDENTITY', False


# --- Function translations ---

def _select_database(m, conn, sql):
    result = re.sub(r'\bDATABASE\s*\(\s*\)', 'current_database()', sql, flags=re.IGNORECASE)
    return result, False


def _ifnull(m, conn, sql):
    # Replace IFNULL with COALESCE throughout
    result = re.sub(r'\bIFNULL\s*\(', 'COALESCE(', sql, flags=re.IGNORECASE)
    return _convert_backticks(result), False


# --- User/privilege management ---

def _create_user(m, conn, sql):
    user = m.group("user").strip("'\"")
    password = m.group("password")
    host = m.group("host")
    result = f"CREATE ROLE \"{user}\" WITH LOGIN"
    if password:
        result += f" PASSWORD '{password.strip(chr(39))}'"
    notice = ""
    if host and host.strip("'\"") != "%":
        notice = f"/* Note: host restriction '{host}' not applied — use pg_hba.conf for host-based access control */"
    return f"{result}; {notice}".strip(), False


def _drop_user(m, conn, sql):
    user = m.group("user").strip("'\"")
    return f'DROP ROLE "{user}"', False


def _alter_user(m, conn, sql):
    user = m.group("user").strip("'\"")
    password = m.group("password").strip("'\"")
    return f"ALTER ROLE \"{user}\" WITH PASSWORD '{password}'", False


def _grant(m, conn, sql):
    privs = m.group("privs").strip()
    user = m.group("user").strip("'\"")
    if privs.upper() == "ALL PRIVILEGES" or privs.upper() == "ALL":
        return _convert_backticks(
            f'GRANT ALL ON ALL TABLES IN SCHEMA public TO "{user}"; '
            f'GRANT USAGE ON SCHEMA public TO "{user}"'
        ), False
    return _convert_backticks(
        f'GRANT {privs} ON ALL TABLES IN SCHEMA public TO "{user}"'
    ), False


def _revoke(m, conn, sql):
    privs = m.group("privs").strip()
    user = m.group("user").strip("'\"")
    if privs.upper() == "ALL PRIVILEGES" or privs.upper() == "ALL":
        return _convert_backticks(
            f'REVOKE ALL ON ALL TABLES IN SCHEMA public FROM "{user}"'
        ), False
    return _convert_backticks(
        f'REVOKE {privs} ON ALL TABLES IN SCHEMA public FROM "{user}"'
    ), False


def _flush_privileges(m, conn, sql):
    columns = ["Message"]
    rows = [("Query OK (PostgreSQL does not require FLUSH PRIVILEGES)",)]
    return (columns, rows), True


# --- Process/admin translations ---

def _kill(m, conn, sql):
    query_flag = m.group("query")
    pid = m.group("pid")
    if query_flag:
        return f"SELECT pg_cancel_backend({pid})", False
    return f"SELECT pg_terminate_backend({pid})", False


def _set_global(m, conn, sql):
    var = m.group("var")
    val = m.group("val")
    return f"ALTER SYSTEM SET {var} = {val}", False


def _set_autocommit(m, conn, sql):
    val = m.group("val").strip()
    # Handled specially in commands.py
    return sql, False


# --- Statement-level translations ---

def _update_join(m, conn, sql):
    """Translate MySQL UPDATE ... JOIN ... SET to PG UPDATE ... FROM."""
    table1 = m.group("table1").strip("`'\"")
    table2 = m.group("table2").strip("`'\"")
    on_clause = m.group("on_clause").strip()
    set_clause = m.group("set_clause").strip()
    where = m.group("where")

    set_clean = re.sub(rf'\b{re.escape(table1)}\s*\.', '', set_clause)
    result = f'UPDATE "{table1}" SET {set_clean} FROM "{table2}" WHERE {on_clause}'
    if where:
        result += f' AND {where.strip()}'
    return _convert_backticks(result), False


def _delete_join(m, conn, sql):
    """Translate MySQL DELETE ... JOIN to PG DELETE ... USING."""
    table1 = m.group("table1").strip("`'\"")
    table2 = m.group("table2").strip("`'\"")
    on_clause = m.group("on_clause").strip()
    where = m.group("where")

    result = f'DELETE FROM "{table1}" USING "{table2}" WHERE {on_clause}'
    if where:
        result += f' AND {where.strip()}'
    return _convert_backticks(result), False


def _load_data_infile(m, conn, sql):
    """Translate LOAD DATA INFILE → COPY ... FROM."""
    filepath = m.group("filepath")
    table = m.group("table").strip("`'\"")
    return f"COPY \"{table}\" FROM '{filepath}' WITH (FORMAT csv, HEADER)", False


def _set_sql_mode(m, conn, sql):
    """SET sql_mode → no-op (PG is always strict)."""
    return _noop_ok(m, conn, sql)


def _set_session_var(m, conn, sql):
    """Handle various SET SESSION/GLOBAL variables as no-ops."""
    return _noop_ok(m, conn, sql)


def _insert_set_syntax(m, conn, sql):
    """Translate MySQL INSERT ... SET col=val to standard INSERT INTO ... VALUES."""
    table = m.group("table").strip("`'\"")
    set_clause = m.group("set_clause").strip()

    # Parse SET col1=val1, col2=val2
    cols = []
    vals = []
    for part in _split_args(set_clause):
        part = part.strip()
        eq_m = re.match(r'([`\"\w]+)\s*=\s*(.*)', part, re.DOTALL)
        if eq_m:
            cols.append(eq_m.group(1).strip("`'\""))
            vals.append(eq_m.group(2).strip())

    if not cols:
        return _convert_backticks(sql), False

    col_list = ', '.join(f'"{c}"' for c in cols)
    val_list = ', '.join(vals)
    return _convert_backticks(f'INSERT INTO "{table}" ({col_list}) VALUES ({val_list})'), False


def _do_statement(m, conn, sql):
    """Translate MySQL DO expr → SELECT expr (discard result)."""
    expr = m.group("expr").strip()
    # Common DO usages: DO SLEEP(n) → SELECT pg_sleep(n)
    expr = re.sub(r'\bSLEEP\s*\(', 'pg_sleep(', expr, flags=re.IGNORECASE)
    return f"SELECT {expr}", False


def _use_index_hint(m, conn, sql):
    """Strip MySQL/MariaDB USE/FORCE/IGNORE INDEX hints."""
    # These appear inline in FROM clause: ... FROM t USE INDEX (idx) WHERE ...
    # Strip the hint, keep everything else
    return None  # Fall through to general translation


# --- pgloader compatibility ---

def _create_type_enum(m, conn, sql):
    """CREATE TYPE ... AS ENUM (...) — pass through to PG."""
    return _convert_backticks(sql), False


def _drop_type(m, conn, sql):
    """DROP TYPE ... — pass through to PG."""
    return _convert_backticks(sql), False


def _alter_table_trigger(m, conn, sql):
    """ALTER TABLE ... DISABLE/ENABLE TRIGGER ALL — pass through to PG."""
    return _convert_backticks(sql), False


def _copy_from_stdin(m, conn, sql):
    """COPY ... FROM STDIN — pass through to PG (used by pgloader)."""
    return _convert_backticks(sql), False


def _set_session(m, conn, sql):
    """SET session_replication_role / SET ... — pass through to PG."""
    return sql, False


def _create_index_passthrough(m, conn, sql):
    """CREATE [UNIQUE] INDEX ... — pass through to PG (pgloader emits these)."""
    return _convert_backticks(sql), False


def _alter_table_add_constraint(m, conn, sql):
    """ALTER TABLE ... ADD CONSTRAINT ... — pass through (pgloader FK creation)."""
    return _convert_backticks(sql), False


def _select_setval(m, conn, sql):
    """SELECT setval(...) — pass through (pgloader sequence reset)."""
    return sql, False


# --- MySQL dump boilerplate / no-op translations ---

def _noop_ok(m, conn, sql):
    """No-op that returns Query OK as a special result."""
    # Return as "special" with None columns → triggers status output
    return (["Status"], [("OK",)]), True


def _set_names(m, conn, sql):
    """SET NAMES → SET client_encoding."""
    charset = m.group("charset").strip("'\"")
    encoding_map = {
        "utf8": "UTF8", "utf8mb4": "UTF8", "latin1": "LATIN1",
        "ascii": "SQL_ASCII", "binary": "UTF8",
    }
    enc = encoding_map.get(charset.lower(), "UTF8")
    return f"SET client_encoding = '{enc}'", False


def _set_foreign_key_checks(m, conn, sql):
    # Extract value
    val_m = re.search(r"=\s*(\d+)", sql)
    if val_m and val_m.group(1) == "0":
        return "SET session_replication_role = 'replica'", False
    elif val_m and val_m.group(1) == "1":
        return "SET session_replication_role = 'origin'", False
    return _noop_ok(m, conn, sql)


def _lock_tables(m, conn, sql):
    return _noop_ok(m, conn, sql)


def _unlock_tables(m, conn, sql):
    return _noop_ok(m, conn, sql)


def _disable_enable_keys(m, conn, sql):
    return _noop_ok(m, conn, sql)


def _mysql_create_table(m, conn, sql):
    """Translate MySQL CREATE TABLE to PG-compatible DDL."""
    full_sql = m.group(0)
    # This is a MySQL-format CREATE TABLE - translate to PG
    result = _translate_mysql_ddl(full_sql)
    return _convert_backticks(result), False


def _split_top_level(s):
    """Split string by commas at the top level (not inside parens)."""
    parts = []
    depth = 0
    current = []
    for ch in s:
        if ch == '(':
            depth += 1
            current.append(ch)
        elif ch == ')':
            depth -= 1
            current.append(ch)
        elif ch == ',' and depth == 0:
            parts.append("".join(current))
            current = []
        else:
            current.append(ch)
    if current:
        parts.append("".join(current))
    return parts


def _translate_mysql_ddl(sql):
    """Convert MySQL CREATE TABLE DDL to PostgreSQL-compatible DDL."""
    # Extract table name
    m = re.match(
        r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([`\"\w\-\.]+)\s*\(",
        sql, re.IGNORECASE
    )
    if not m:
        return sql
    table_name = m.group(1).strip("`'\"")

    # Find the column/constraint definitions between first ( and last )
    paren_start = sql.index("(")
    # Find matching closing paren (skip nested parens)
    depth = 0
    paren_end = -1
    for i in range(paren_start, len(sql)):
        if sql[i] == '(':
            depth += 1
        elif sql[i] == ')':
            depth -= 1
            if depth == 0:
                paren_end = i
                break

    if paren_end < 0:
        return sql

    inner = sql[paren_start + 1:paren_end]
    # table options after the closing paren
    # Don't need them for PG

    # Parse column definitions
    # Split by commas at top level (not inside parens)
    defs = _split_top_level(inner)

    pg_defs = []
    pg_post = []  # Post-CREATE statements (indexes, etc.)
    has_serial = set()

    for d in defs:
        d = d.strip()
        if not d:
            continue

        upper = d.upper().lstrip()

        # PRIMARY KEY
        if upper.startswith("PRIMARY KEY"):
            pg_defs.append(_convert_backticks(d))
            continue

        # UNIQUE KEY
        if upper.startswith("UNIQUE KEY") or upper.startswith("UNIQUE INDEX"):
            # UNIQUE KEY `name` (`col`) → UNIQUE (col)  -- inline
            um = re.match(r"UNIQUE\s+(?:KEY|INDEX)\s+[`\"\w]+\s*\(([^)]+)\)", d, re.IGNORECASE)
            if um:
                cols = um.group(1)
                pg_defs.append(f"UNIQUE ({_convert_backticks(cols)})")
            else:
                pg_defs.append(_convert_backticks(d))
            continue

        # FULLTEXT KEY → skip (PG needs GIN/tsvector, not a simple index)
        if upper.startswith("FULLTEXT"):
            continue

        # SPATIAL KEY → skip (PG needs PostGIS/GIST index)
        if upper.startswith("SPATIAL"):
            continue

        # KEY (non-unique index) → handled as CREATE INDEX after
        if upper.startswith("KEY ") or upper.startswith("INDEX "):
            km = re.match(r"(?:KEY|INDEX)\s+([`\"\w]+)\s*\(([^)]+)\)", d, re.IGNORECASE)
            if km:
                idx_name = km.group(1).strip("`'\"")
                cols = km.group(2)
                pg_post.append(
                    f'CREATE INDEX "{idx_name}" ON "{table_name}" ({_convert_backticks(cols)})'
                )
            continue

        # CONSTRAINT ... FOREIGN KEY
        if upper.startswith("CONSTRAINT"):
            pg_defs.append(_convert_backticks(d))
            continue

        # Column definition
        pg_col = _translate_mysql_column(d)
        if pg_col:
            # Check for AUTO_INCREMENT → SERIAL
            if "AUTO_INCREMENT" in d.upper():
                col_name = re.match(r"[`\"\w]+", d.strip()).group(0).strip("`'\"")
                has_serial.add(col_name)
            pg_defs.append(pg_col)

    # Build PG CREATE TABLE
    if_not_exists = "IF NOT EXISTS " if "IF NOT EXISTS" in sql.upper() else ""
    result = f'CREATE TABLE {if_not_exists}"{table_name}" (\n'
    result += ",\n".join("  " + d for d in pg_defs)
    result += "\n)"

    # Add post-create statements
    for post in pg_post:
        result += ";\n" + post

    return result


def _translate_mysql_column(col_def):
    """Translate a single MySQL column definition to PostgreSQL."""
    col_def = col_def.strip()
    if not col_def:
        return None

    # Extract column name
    m = re.match(r"([`\"\w]+)\s+(.*)", col_def, re.DOTALL)
    if not m:
        return _convert_backticks(col_def)

    col_name = m.group(1).strip("`'\"")
    rest = m.group(2).strip()

    # Extract type
    type_m = re.match(r"(\w+(?:\([^)]*\))?(?:\s+unsigned)?)", rest, re.IGNORECASE)
    if not type_m:
        return f'"{col_name}" {_convert_backticks(rest)}'

    mysql_type = type_m.group(1)
    after_type = rest[type_m.end():].strip()

    # Map MySQL type to PG type
    pg_type = _map_mysql_type_to_pg(mysql_type)

    # Check for AUTO_INCREMENT
    is_auto = bool(re.search(r"\bAUTO_INCREMENT\b", after_type, re.IGNORECASE))

    if is_auto:
        # Use SERIAL/BIGSERIAL
        if "bigint" in mysql_type.lower():
            pg_type = "BIGSERIAL"
        elif "smallint" in mysql_type.lower():
            pg_type = "SMALLSERIAL"
        else:
            pg_type = "SERIAL"
        after_type = re.sub(r"\bAUTO_INCREMENT\b", "", after_type, flags=re.IGNORECASE).strip()

    # Clean up remaining MySQL-isms from after_type
    # Remove CHARACTER SET / COLLATE
    after_type = re.sub(r"\bCHARACTER\s+SET\s+\S+", "", after_type, flags=re.IGNORECASE)
    after_type = re.sub(r"\bCOLLATE\s+\S+", "", after_type, flags=re.IGNORECASE)
    # Remove COMMENT '...'
    after_type = re.sub(r"\bCOMMENT\s+'[^']*'", "", after_type, flags=re.IGNORECASE)
    # Remove ON UPDATE CURRENT_TIMESTAMP
    after_type = re.sub(r"\bON\s+UPDATE\s+CURRENT_TIMESTAMP\b", "", after_type, flags=re.IGNORECASE)

    # Fix boolean defaults for integer types
    if pg_type in ("SMALLINT", "INTEGER", "BIGINT"):
        after_type = re.sub(r"\bDEFAULT\s+true\b", "DEFAULT 1", after_type, flags=re.IGNORECASE)
        after_type = re.sub(r"\bDEFAULT\s+false\b", "DEFAULT 0", after_type, flags=re.IGNORECASE)

    after_type = " ".join(after_type.split())  # normalize whitespace

    parts = [f'"{col_name}"', pg_type]
    if after_type:
        parts.append(after_type)
    return " ".join(parts)


def _map_mysql_type_to_pg(mysql_type):
    """Map a MySQL type string to PostgreSQL type."""
    t = mysql_type.lower().strip()

    # Remove UNSIGNED
    unsigned = "unsigned" in t
    t = t.replace("unsigned", "").strip()

    # int(N) → INTEGER
    m = re.match(r"(tiny|small|medium|big)?int(?:eger)?\s*(?:\(\d+\))?", t)
    if m:
        prefix = m.group(1) or ""
        if prefix == "tiny":
            return "SMALLINT"
        elif prefix == "small":
            return "SMALLINT"
        elif prefix == "medium":
            return "INTEGER"
        elif prefix == "big":
            return "BIGINT"
        return "INTEGER"

    if t.startswith("tinyint"):
        return "SMALLINT"

    # float/double
    if re.match(r"float\b", t):
        return "REAL"
    if re.match(r"double\b", t):
        return "DOUBLE PRECISION"

    # decimal/numeric
    dm = re.match(r"(?:decimal|numeric)\s*(\([^)]+\))?", t)
    if dm:
        prec = dm.group(1) or "(10,0)"
        return f"NUMERIC{prec}"

    # varchar/char
    vm = re.match(r"varchar\s*(\([^)]+\))?", t)
    if vm:
        size = vm.group(1) or "(255)"
        return f"VARCHAR{size}"

    cm = re.match(r"char\s*(\([^)]+\))?", t)
    if cm:
        size = cm.group(1) or "(1)"
        return f"CHAR{size}"

    # text variants
    if t in ("tinytext", "mediumtext", "longtext", "text"):
        return "TEXT"

    # blob variants
    if t in ("tinyblob", "mediumblob", "longblob", "blob"):
        return "BYTEA"

    # date/time
    if t == "datetime":
        return "TIMESTAMP"
    if t == "timestamp":
        return "TIMESTAMP"
    if t == "date":
        return "DATE"
    if t == "time":
        return "TIME"
    if t == "year":
        return "SMALLINT"

    # json
    if t == "json":
        return "JSONB"

    # enum → TEXT (PG has native ENUM but requires CREATE TYPE first)
    if t.startswith("enum"):
        # Extract values for potential CREATE TYPE usage
        return "TEXT"
    if t.startswith("set"):
        return "TEXT"

    # bit
    bm = re.match(r"bit\s*(\([^)]+\))?", t)
    if bm:
        return f"BIT{bm.group(1) or '(1)'}"

    # binary/varbinary
    if t.startswith("binary") or t.startswith("varbinary"):
        return "BYTEA"

    # Fallback
    return t.upper()


# --- Build translator list ---

_LIKE_PAT = r"(?:\s+LIKE\s+'(?P<like>[^']*)')?"
_LIKE_PAT2 = r"(?:\s+LIKE\s+'(?P<like>[^']*)')?"

_i = re.IGNORECASE

_TRANSLATORS = [
    # --- pgloader compatibility (must be early) ---

    # CREATE TYPE ... AS ENUM (...)
    (re.compile(r"CREATE\s+TYPE\s+", _i), _create_type_enum),

    # DROP TYPE [IF EXISTS] ...
    (re.compile(r"DROP\s+TYPE\s+", _i), _drop_type),

    # ALTER TABLE ... DISABLE/ENABLE TRIGGER ALL
    (re.compile(r"ALTER\s+TABLE\s+\S+\s+(?:DISABLE|ENABLE)\s+TRIGGER\s+ALL", _i),
     _alter_table_trigger),

    # COPY ... FROM STDIN (pgloader bulk load)
    (re.compile(r"COPY\s+\S+\s+.*FROM\s+STDIN", _i | re.DOTALL), _copy_from_stdin),

    # CREATE [UNIQUE] INDEX ... ON ... (pgloader emits these directly)
    (re.compile(r"CREATE\s+(?:UNIQUE\s+)?INDEX\s+\S+\s+ON\s+", _i),
     _create_index_passthrough),

    # ALTER TABLE ... ADD CONSTRAINT ... FOREIGN KEY (pgloader FK creation)
    (re.compile(r"ALTER\s+TABLE\s+\S+\s+ADD\s+CONSTRAINT\s+\S+\s+FOREIGN\s+KEY", _i),
     _alter_table_add_constraint),

    # SELECT setval(...) (pgloader sequence reset)
    (re.compile(r"SELECT\s+setval\s*\(", _i), _select_setval),

    # SET session_replication_role (pgloader uses this)
    (re.compile(r"SET\s+session_replication_role\s*=", _i), _set_session),

    # --- MySQL dump boilerplate (must be early to catch before passthrough) ---

    # SET NAMES charset
    (re.compile(r"SET\s+NAMES\s+(?P<charset>\S+)\s*$", _i), _set_names),

    # SET FOREIGN_KEY_CHECKS
    (re.compile(r"SET\s+FOREIGN_KEY_CHECKS\s*=\s*\d+", _i), _set_foreign_key_checks),

    # SET @OLD_... / SET CHARACTER_SET_CLIENT / SET CHARACTER_SET_RESULTS / SET COLLATION_CONNECTION
    (re.compile(r"SET\s+@?\w*CHARACTER_SET\w*\s*=", _i), _noop_ok),
    (re.compile(r"SET\s+@?\w*COLLATION\w*\s*=", _i), _noop_ok),

    # SET @saved_... = ... (MySQL dump checkpoint variables)
    (re.compile(r"SET\s+@\w+\s*=\s*@@", _i), _noop_ok),

    # LOCK TABLES
    (re.compile(r"LOCK\s+TABLES\s+", _i), _lock_tables),

    # UNLOCK TABLES
    (re.compile(r"UNLOCK\s+TABLES", _i), _unlock_tables),

    # ALTER TABLE ... DISABLE/ENABLE KEYS
    (re.compile(r"ALTER\s+TABLE\s+\S+\s+(?:DISABLE|ENABLE)\s+KEYS", _i), _disable_enable_keys),

    # MySQL-style CREATE TABLE with MySQL types (detected by ENGINE= or AUTO_INCREMENT)
    (re.compile(
        r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?[`\"\w\-\.]+\s*\(.*\)\s*"
        r"(?:ENGINE\s*=|AUTO_INCREMENT\s*=|DEFAULT\s+CHARSET)",
        _i | re.DOTALL
    ), _mysql_create_table),

    # SHOW DATABASES
    (re.compile(r"SHOW\s+DATABASES" + _LIKE_PAT + r"\s*$", _i), _show_databases),

    # SHOW [FULL] TABLES [FROM db] [LIKE '...']
    (re.compile(
        r"SHOW\s+(?P<full>FULL\s+)?TABLES"
        r"(?:\s+(?:FROM|IN)\s+(?P<from_db>\S+))?"
        + _LIKE_PAT + r"\s*$", _i
    ), _show_tables),

    # SHOW CREATE TABLE x
    (re.compile(r"SHOW\s+CREATE\s+TABLE\s+(?P<table>[`\"\w.]+)\s*$", _i), _show_create_table),

    # DESC / DESCRIBE / EXPLAIN (when followed by table name, not SELECT)
    (re.compile(
        r"(?:DESC(?:RIBE)?|EXPLAIN)\s+(?!SELECT|INSERT|UPDATE|DELETE|WITH)(?P<table>[`\"\w.]+)\s*$",
        _i
    ), _desc_table),

    # SHOW [FULL] COLUMNS FROM x [LIKE '...']
    (re.compile(
        r"SHOW\s+(?P<full>FULL\s+)?COLUMNS\s+FROM\s+(?P<table>[`\"\w.]+)"
        + _LIKE_PAT + r"\s*$", _i
    ), _show_full_columns if True else _desc_table),

    # SHOW INDEX FROM x
    (re.compile(r"SHOW\s+(?:INDEX|INDEXES|KEYS)\s+FROM\s+(?P<table>[`\"\w.]+)\s*$", _i),
     _show_index),

    # SHOW TABLE STATUS [LIKE '...']
    (re.compile(r"SHOW\s+TABLE\s+STATUS" + _LIKE_PAT + r"\s*$", _i), _show_table_status),

    # SHOW [FULL] PROCESSLIST
    (re.compile(r"SHOW\s+(?P<full>FULL\s+)?PROCESSLIST\s*$", _i), _show_processlist),

    # SHOW [GLOBAL] VARIABLES [LIKE '...']
    (re.compile(r"SHOW\s+(?:GLOBAL\s+)?VARIABLES" + _LIKE_PAT + r"\s*$", _i), _show_variables),

    # SHOW [GLOBAL] STATUS [LIKE '...']
    (re.compile(
        r"SHOW\s+(?P<global>GLOBAL\s+)?STATUS" + _LIKE_PAT + r"\s*$", _i
    ), _show_status),

    # SHOW GRANTS [FOR user]
    (re.compile(
        r"SHOW\s+GRANTS(?:\s+FOR\s+(?P<user>['\"][^'\"]+['\"](?:@['\"][^'\"]+['\"])?))?",
        _i
    ), _show_grants),

    # SHOW WARNINGS
    (re.compile(r"SHOW\s+WARNINGS\s*$", _i), _show_warnings),

    # SHOW ENGINES
    (re.compile(r"SHOW\s+ENGINES?\s*$", _i), _show_engines),

    # SHOW ENGINE INNODB STATUS
    (re.compile(r"SHOW\s+ENGINE\s+INNODB\s+STATUS\s*$", _i), _show_engine_status),

    # SHOW CHARACTER SET
    (re.compile(r"SHOW\s+(?:CHARACTER\s+SET|CHARSET)" + _LIKE_PAT + r"\s*$", _i), _show_charset),

    # SHOW COLLATION
    (re.compile(r"SHOW\s+COLLATION" + _LIKE_PAT + r"\s*$", _i), _show_collation),

    # SHOW CREATE DATABASE
    (re.compile(r"SHOW\s+CREATE\s+DATABASE\s+(?P<db>[`\"\w]+)\s*$", _i), _show_create_database),

    # --- Statement-level translations ---

    # UPDATE ... JOIN ... SET ... WHERE → UPDATE ... SET ... FROM ... WHERE
    (re.compile(
        r"UPDATE\s+(?P<table1>[`\"\w.]+)\s+"
        r"(?:(?P<join_type>INNER|LEFT|RIGHT|CROSS)\s+)?JOIN\s+(?P<table2>[`\"\w.]+)\s+"
        r"ON\s+(?P<on_clause>.+?)\s+"
        r"SET\s+(?P<set_clause>.+?)"
        r"(?:\s+WHERE\s+(?P<where>.+))?$",
        _i | re.DOTALL
    ), _update_join),

    # DELETE t1 FROM t1 JOIN t2 ON ... WHERE → DELETE FROM t1 USING t2 WHERE
    (re.compile(
        r"DELETE\s+[`\"\w.]+\s+FROM\s+(?P<table1>[`\"\w.]+)\s+"
        r"(?:(?:INNER|LEFT|RIGHT|CROSS)\s+)?JOIN\s+(?P<table2>[`\"\w.]+)\s+"
        r"ON\s+(?P<on_clause>.+?)"
        r"(?:\s+WHERE\s+(?P<where>.+))?$",
        _i | re.DOTALL
    ), _delete_join),

    # LOAD DATA INFILE
    (re.compile(
        r"LOAD\s+DATA\s+(?:LOCAL\s+)?INFILE\s+'(?P<filepath>[^']+)'\s+"
        r"INTO\s+TABLE\s+(?P<table>[`\"\w.]+)",
        _i
    ), _load_data_infile),

    # SET sql_mode → no-op
    (re.compile(r"SET\s+(?:SESSION\s+|GLOBAL\s+)?sql_mode\s*=", _i), _set_sql_mode),

    # SET SESSION/@@session variables → no-op
    (re.compile(r"SET\s+@@(?:session|global)\.\w+\s*=", _i), _set_session_var),

    # SET wait_timeout / interactive_timeout / net_* → no-op
    (re.compile(r"SET\s+(?:SESSION\s+)?(?:wait_timeout|interactive_timeout|net_\w+)\s*=", _i),
     _set_session_var),

    # INSERT ... SET syntax (MySQL/MariaDB only) → standard INSERT
    (re.compile(
        r"INSERT\s+INTO\s+(?P<table>[`\"\w.]+)\s+SET\s+(?P<set_clause>.+)",
        _i | re.DOTALL
    ), _insert_set_syntax),

    # INSERT IGNORE
    (re.compile(
        r"INSERT\s+IGNORE\s+INTO\s+(?P<table>[`\"\w.]+)\s+(?P<rest>.*)",
        _i | re.DOTALL
    ), _insert_ignore),

    # INSERT ... ON DUPLICATE KEY UPDATE
    (re.compile(r"INSERT\s+INTO\s+.*ON\s+DUPLICATE\s+KEY\s+UPDATE", _i | re.DOTALL),
     _on_duplicate_key),

    # REPLACE INTO
    (re.compile(
        r"REPLACE\s+INTO\s+(?P<table>[`\"\w.]+)\s+(?P<rest>.*)",
        _i | re.DOTALL
    ), _replace_into),

    # CREATE DATABASE
    (re.compile(
        r"CREATE\s+DATABASE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?P<db>[`\"\w]+)",
        _i
    ), _create_database),

    # ALTER TABLE ... MODIFY COLUMN
    (re.compile(
        r"ALTER\s+TABLE\s+(?P<table>[`\"\w.]+)\s+MODIFY\s+(?:COLUMN\s+)?"
        r"(?P<column>[`\"\w]+)\s+(?P<type>.+)",
        _i
    ), _alter_table_modify),

    # ALTER TABLE ... CHANGE
    (re.compile(
        r"ALTER\s+TABLE\s+(?P<table>[`\"\w.]+)\s+CHANGE\s+(?:COLUMN\s+)?"
        r"(?P<old_col>[`\"\w]+)\s+(?P<new_col>[`\"\w]+)\s+(?P<type>.+)",
        _i
    ), _alter_table_change),

    # ALTER TABLE ... ADD [UNIQUE] INDEX
    (re.compile(
        r"ALTER\s+TABLE\s+(?P<table>[`\"\w.]+)\s+ADD\s+(?P<unique>UNIQUE\s+)?"
        r"(?:INDEX|KEY)\s+(?P<idx_name>[`\"\w]+)\s*\((?P<columns>[^)]+)\)",
        _i
    ), _alter_table_add_index),

    # ALTER TABLE ... DROP INDEX
    (re.compile(
        r"ALTER\s+TABLE\s+(?P<table>[`\"\w.]+)\s+DROP\s+(?:INDEX|KEY)\s+"
        r"(?P<idx_name>[`\"\w]+)",
        _i
    ), _alter_table_drop_index),

    # RENAME TABLE
    (re.compile(
        r"RENAME\s+TABLE\s+(?P<old>[`\"\w.]+)\s+TO\s+(?P<new>[`\"\w.]+)",
        _i
    ), _rename_table),

    # TRUNCATE TABLE
    (re.compile(r"TRUNCATE\s+(?:TABLE\s+)?(?P<table>[`\"\w.]+)\s*$", _i), _truncate_table),

    # SELECT DATABASE()
    (re.compile(r"SELECT\s+.*DATABASE\s*\(\s*\)", _i), _select_database),

    # IFNULL
    (re.compile(r".*\bIFNULL\s*\(", _i), _ifnull),

    # CREATE USER
    (re.compile(
        r"CREATE\s+USER\s+'(?P<user>[^']+)'@'(?P<host>[^']+)'"
        r"(?:\s+IDENTIFIED\s+BY\s+'(?P<password>[^']+)')?",
        _i
    ), _create_user),

    # DROP USER
    (re.compile(r"DROP\s+USER\s+'(?P<user>[^']+)'(?:@'[^']*')?", _i), _drop_user),

    # ALTER USER ... IDENTIFIED BY
    (re.compile(
        r"ALTER\s+USER\s+'(?P<user>[^']+)'(?:@'[^']*')?\s+IDENTIFIED\s+BY\s+'(?P<password>[^']+)'",
        _i
    ), _alter_user),

    # GRANT ... ON db.* TO user
    (re.compile(
        r"GRANT\s+(?P<privs>[\w\s,]+)\s+ON\s+\S+\.\*\s+TO\s+'(?P<user>[^']+)'(?:@'[^']*')?",
        _i
    ), _grant),

    # REVOKE ... ON db.* FROM user
    (re.compile(
        r"REVOKE\s+(?P<privs>[\w\s,]+)\s+ON\s+\S+\.\*\s+FROM\s+'(?P<user>[^']+)'(?:@'[^']*')?",
        _i
    ), _revoke),

    # FLUSH PRIVILEGES
    (re.compile(r"FLUSH\s+PRIVILEGES\s*$", _i), _flush_privileges),

    # DO statement (MySQL: DO expr → PG: SELECT expr)
    (re.compile(r"DO\s+(?P<expr>.+)", _i), _do_statement),

    # KILL [QUERY] pid
    (re.compile(r"KILL\s+(?P<query>QUERY\s+)?(?P<pid>\d+)\s*$", _i), _kill),

    # SET GLOBAL var = val
    (re.compile(r"SET\s+GLOBAL\s+(?P<var>\w+)\s*=\s*(?P<val>.+)", _i), _set_global),

    # SET autocommit
    (re.compile(r"SET\s+autocommit\s*=\s*(?P<val>\S+)", _i), _set_autocommit),
]

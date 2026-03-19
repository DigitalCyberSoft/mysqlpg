"""DDL reconstruction from PostgreSQL metadata — produces MySQL-style CREATE TABLE."""

import re


# PostgreSQL type → MySQL display type mapping
PG_TO_MYSQL_TYPE = {
    "integer": "int(11)",
    "int": "int(11)",
    "int4": "int(11)",
    "smallint": "smallint(6)",
    "int2": "smallint(6)",
    "bigint": "bigint(20)",
    "int8": "bigint(20)",
    "serial": "int(11)",
    "bigserial": "bigint(20)",
    "smallserial": "smallint(6)",
    "boolean": "tinyint(1)",
    "bool": "tinyint(1)",
    "real": "float",
    "float4": "float",
    "double precision": "double",
    "float8": "double",
    "numeric": "decimal",
    "decimal": "decimal",
    "money": "decimal(19,2)",
    "text": "text",
    "character varying": "varchar",
    "varchar": "varchar",
    "character": "char",
    "char": "char",
    "bpchar": "char",
    "bytea": "blob",
    "timestamp without time zone": "datetime",
    "timestamp with time zone": "datetime",
    "timestamp": "datetime",
    "timestamptz": "datetime",
    "date": "date",
    "time without time zone": "time",
    "time with time zone": "time",
    "time": "time",
    "timetz": "time",
    "interval": "varchar(255)",
    "json": "json",
    "jsonb": "json",
    "uuid": "char(36)",
    "xml": "text",
    "inet": "varchar(45)",
    "cidr": "varchar(45)",
    "macaddr": "varchar(17)",
    "point": "point",
    "line": "linestring",
    "polygon": "polygon",
    "circle": "varchar(255)",
    "bit": "bit",
    "bit varying": "bit",
    "varbit": "bit",
    "tsvector": "text",
    "tsquery": "text",
    "oid": "int(11) unsigned",
    "name": "varchar(64)",
}


def get_enum_values(conn, type_name):
    """Get ENUM values for a PostgreSQL ENUM type."""
    try:
        _, rows, *_ = conn.execute("""
            SELECT e.enumlabel
            FROM pg_enum e
            JOIN pg_type t ON e.enumtypid = t.oid
            WHERE t.typname = %s
            ORDER BY e.enumsortorder
        """, (type_name,))
        return [r[0] for r in rows] if rows else []
    except Exception:
        return []


def map_pg_type_to_mysql(data_type, character_maximum_length=None,
                         numeric_precision=None, numeric_scale=None,
                         udt_name=None, conn=None):
    """Map a PostgreSQL column type to MySQL display type."""
    dt_lower = data_type.lower() if data_type else ""
    udt_lower = (udt_name or "").lower()

    # Check for array types (PG uses _ prefix for array types AND data_type='ARRAY')
    if dt_lower == "array" or (udt_lower.startswith("_") and dt_lower == "array"):
        base = udt_lower[1:] if udt_lower.startswith("_") else udt_lower
        mapped = PG_TO_MYSQL_TYPE.get(base, base)
        return f"{mapped} /* array */"

    # USER-DEFINED (enum, composite)
    if dt_lower == "user-defined":
        # Check if it's an ENUM type
        if conn and udt_lower:
            enum_vals = get_enum_values(conn, udt_lower)
            if enum_vals:
                vals_str = ",".join(f"'{v}'" for v in enum_vals)
                return f"enum({vals_str})"
        return f"varchar(255) /* {udt_lower} */"

    # character varying(N)
    if dt_lower in ("character varying", "varchar"):
        if character_maximum_length:
            return f"varchar({character_maximum_length})"
        return "text"

    # character(N) / char(N)
    if dt_lower in ("character", "char", "bpchar"):
        if character_maximum_length:
            return f"char({character_maximum_length})"
        return "char(1)"

    # numeric/decimal with precision
    if dt_lower in ("numeric", "decimal"):
        if numeric_precision is not None:
            scale = numeric_scale if numeric_scale is not None else 0
            return f"decimal({numeric_precision},{scale})"
        return "decimal(10,0)"

    # bit(N)
    if dt_lower in ("bit", "bit varying", "varbit"):
        if character_maximum_length:
            return f"bit({character_maximum_length})"
        return "bit(1)"

    # Direct mapping
    mapped = PG_TO_MYSQL_TYPE.get(dt_lower)
    if mapped:
        return mapped

    # Try udt_name
    mapped = PG_TO_MYSQL_TYPE.get(udt_lower)
    if mapped:
        return mapped

    # Fallback: pass through
    return dt_lower if dt_lower else "varchar(255)"


def clean_default(default_value, data_type=None):
    """Clean a PostgreSQL default value for MySQL-style display.

    - Converts nextval() to AUTO_INCREMENT marker
    - Strips ::type casts
    - Cleans up quoting
    """
    if default_value is None:
        return None

    val = default_value.strip()

    # nextval → AUTO_INCREMENT
    if "nextval(" in val:
        return "__AUTO_INCREMENT__"

    # Strip ::type casts
    # e.g., 'hello'::character varying → 'hello'
    # e.g., 0::integer → 0
    while "::" in val:
        idx = val.index("::")
        # Find where the cast type ends
        rest = val[idx + 2:].strip()
        # Type might be something like "character varying" or "integer"
        # Find the end of the type name
        paren_depth = 0
        end = 0
        for i, ch in enumerate(rest):
            if ch == '(':
                paren_depth += 1
            elif ch == ')':
                if paren_depth > 0:
                    paren_depth -= 1
                else:
                    end = i
                    break
            elif ch in (' ', ',', ')') and paren_depth == 0:
                # Check if this is part of a multi-word type like "character varying"
                remaining = rest[i:].strip()
                if remaining.lower().startswith("varying"):
                    continue
                elif remaining.lower().startswith("without") or remaining.lower().startswith("with"):
                    continue
                elif remaining.lower().startswith("time zone"):
                    continue
                end = i
                break
        else:
            end = len(rest)
        val = val[:idx] + rest[end:]
        val = val.strip()

    # Clean up: remove trailing type names left over
    # If it's a simple quoted string like 'value', or a number, return it
    return val if val else None


def show_create_table(conn, table, schema="public"):
    """Build a MySQL-style CREATE TABLE statement from PG metadata.

    Returns (table_name, create_statement) tuple.
    """
    # 1. Get columns
    cols, rows, *_ = conn.execute("""
        SELECT column_name, data_type, character_maximum_length,
               numeric_precision, numeric_scale, is_nullable,
               column_default, udt_name, ordinal_position
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
    """, (schema, table))

    if not rows:
        raise Exception(f"Table '{table}' doesn't exist")

    lines = []
    auto_increment_col = None

    for row in rows:
        col_name, data_type, char_max_len, num_prec, num_scale, \
            is_nullable, col_default, udt_name, ordinal = row

        mysql_type = map_pg_type_to_mysql(data_type, char_max_len,
                                          num_prec, num_scale, udt_name, conn=conn)

        parts = [f"  `{col_name}`", mysql_type]

        # Nullable
        if is_nullable == "NO":
            parts.append("NOT NULL")

        # Default / AUTO_INCREMENT
        cleaned = clean_default(col_default, data_type)
        if cleaned == "__AUTO_INCREMENT__":
            parts.append("AUTO_INCREMENT")
            auto_increment_col = col_name
        elif cleaned is not None:
            parts.append(f"DEFAULT {cleaned}")
        elif is_nullable == "YES" and col_default is None:
            parts.append("DEFAULT NULL")

        lines.append(" ".join(parts))

    # 2. Get constraints
    _c, constraint_rows, *_ = conn.execute("""
        SELECT c.conname, c.contype,
               array_agg(a.attname ORDER BY array_position(c.conkey, a.attnum)) AS columns,
               c.confrelid::regclass AS ref_table,
               array_agg(af.attname ORDER BY array_position(c.confkey, af.attnum)) AS ref_columns
        FROM pg_constraint c
        JOIN pg_attribute a ON a.attnum = ANY(c.conkey) AND a.attrelid = c.conrelid
        LEFT JOIN pg_attribute af ON af.attnum = ANY(c.confkey) AND af.attrelid = c.confrelid
        WHERE c.conrelid = (
            SELECT oid FROM pg_class
            WHERE relname = %s
              AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = %s)
        )
        GROUP BY c.conname, c.contype, c.confrelid, c.conkey
        ORDER BY c.contype, c.conname
    """, (table, schema))

    pk_cols = []
    for crow in (constraint_rows or []):
        cname, ctype, ccols, ref_table, ref_cols = crow
        col_list = ", ".join(f"`{c}`" for c in ccols)

        if ctype == "p":  # PRIMARY KEY
            pk_cols = list(ccols)
            lines.append(f"  PRIMARY KEY ({col_list})")
        elif ctype == "u":  # UNIQUE
            lines.append(f"  UNIQUE KEY `{cname}` ({col_list})")
        elif ctype == "f":  # FOREIGN KEY
            ref_col_list = ", ".join(f"`{c}`" for c in (ref_cols or []) if c)
            ref_tbl = str(ref_table).replace('"', '').split('.')[-1]
            lines.append(
                f"  CONSTRAINT `{cname}` FOREIGN KEY ({col_list}) "
                f"REFERENCES `{ref_tbl}` ({ref_col_list})"
            )

    # 3. Get non-unique indexes (skip pkey and unique, already handled)
    _c, idx_rows, *_ = conn.execute("""
        SELECT indexname, indexdef
        FROM pg_indexes
        WHERE schemaname = %s AND tablename = %s
    """, (schema, table))

    existing_constraint_names = set()
    for crow in (constraint_rows or []):
        existing_constraint_names.add(crow[0])

    for irow in (idx_rows or []):
        idx_name, idx_def = irow
        if idx_name in existing_constraint_names:
            continue
        # Skip primary key indexes
        if "UNIQUE" in idx_def.upper():
            continue
        # Extract columns from indexdef
        # Format: CREATE INDEX idx ON schema.table USING btree (col1, col2)
        paren_start = idx_def.rfind("(")
        paren_end = idx_def.rfind(")")
        if paren_start >= 0 and paren_end >= 0:
            idx_cols_str = idx_def[paren_start + 1:paren_end]
            # Clean up column names
            idx_col_parts = []
            for part in idx_cols_str.split(","):
                part = part.strip()
                if part:
                    idx_col_parts.append(f"`{part}`")
            if idx_col_parts:
                lines.append(f"  KEY `{idx_name}` ({', '.join(idx_col_parts)})")

    # Build final DDL
    col_defs = ",\n".join(lines)

    # Auto-increment value
    auto_inc_str = ""
    if auto_increment_col:
        try:
            _c2, ai_rows, *_ = conn.execute("""
                SELECT last_value FROM pg_sequences
                WHERE sequencename = (
                    SELECT pg_get_serial_sequence(%s, %s)::regclass::text
                )
            """, (f"{schema}.{table}", auto_increment_col))
            if ai_rows and ai_rows[0][0]:
                auto_inc_str = f" AUTO_INCREMENT={ai_rows[0][0] + 1}"
        except Exception:
            pass

    ddl = (
        f"CREATE TABLE `{table}` (\n"
        f"{col_defs}\n"
        f") ENGINE=PostgreSQL{auto_inc_str} DEFAULT CHARSET=utf8mb4"
    )

    return table, ddl

# mysqlpg & mysqldumppg

MySQL-compatible CLI tools for PostgreSQL. Drop-in replacements for `mysql` and `mysqldump` that talk to PostgreSQL, translating MySQL syntax on the fly.

## Why?

If you know MySQL's CLI tools but your databases are PostgreSQL, these tools let you use familiar commands (`SHOW DATABASES`, `DESC table`, `USE db`, `-u root -p`, etc.) without learning psql's backslash-command syntax.

They also enable round-trip compatibility with MySQL dump files and pgloader migrations.

## Installation

```bash
pip install -e .

# With test dependencies
pip install -e ".[test]"
```

Requires Python 3.8+ and a reachable PostgreSQL server.

### Dependencies

- **psycopg2-binary** >= 2.9 - PostgreSQL driver
- **prompt_toolkit** >= 3.0 - Interactive REPL with autocomplete
- **Pygments** >= 2.0 - SQL syntax highlighting

## Quick Start

```bash
# Connect to PostgreSQL (port defaults to 5432)
mysqlpg -u postgres -h localhost mydb

# Execute a query and exit
mysqlpg -u postgres -e "SHOW TABLES" mydb

# Pipe mode (tab-separated output)
echo "SHOW DATABASES" | mysqlpg -u postgres

# Dump a database in mysqldump format
mysqldumppg -u postgres mydb > backup.sql

# Restore from dump
mysqlpg -u postgres mydb < backup.sql

# Round-trip: dump and restore to another database
mysqldumppg -u postgres sourcedb | mysqlpg -u postgres targetdb
```

---

## mysqlpg

### CLI Flags

| Flag | Long | Description |
|------|------|-------------|
| `-u` | `--user` | PostgreSQL user (default: `$PGUSER` or `$USER`) |
| `-p` | `--password` | Password (prompted if flag given without value; `-pSECRET` supported) |
| `-h` | `--host` | Server host (default: `localhost`) |
| `-P` | `--port` | Server port (default: `5432`) |
| `-D` | `--database` | Database to use |
| `-e` | `--execute` | Execute command and quit |
| `-B` | `--batch` | Batch mode (tab-separated, no borders) |
| `-N` | `--skip-column-names` | Suppress column headers |
| `-t` | `--table` | Force table output format |
| `-s` | `--silent` | Suppress row counts and status messages |
| `-f` | `--force` | Continue on errors |
| `-v` | `--verbose` | Verbose mode |
| `-A` | `--no-auto-rehash` | Disable autocomplete cache on connect |
| `-V` | `--version` | Show version |
| | `--delimiter` | Statement delimiter (default: `;`) |
| | `--vertical` | Print results vertically (`\G` style) |
| | `--auto-vertical-output` | Auto-switch to vertical if output exceeds terminal width |
| | `--pager` | Pipe output through a pager (e.g., `less`) |
| | `--tee` | Log all output to a file |
| | `--prompt` | Custom prompt (supports `\u`, `\h`, `\d`, `\p`, `\D` escapes) |
| | `--init-command` | SQL to execute immediately after connecting |
| | `--show-warnings` | Show PostgreSQL NOTICE messages after each statement |
| `-U` | `--safe-updates` | Require WHERE clause for UPDATE/DELETE |

### Supported MySQL Commands

#### SHOW Commands

| MySQL | PostgreSQL Translation |
|-------|----------------------|
| `SHOW DATABASES [LIKE '...']` | `pg_database` catalog query |
| `SHOW [FULL] TABLES [FROM db] [LIKE '...']` | `information_schema.tables` |
| `SHOW CREATE TABLE t` | Reconstructed DDL from PG metadata |
| `DESC t` / `DESCRIBE t` | `information_schema.columns` with MySQL-style type names |
| `SHOW [FULL] COLUMNS FROM t [LIKE '...']` | Extended column info with privileges, comments |
| `SHOW INDEX FROM t` | `pg_index` / `pg_attribute` join |
| `SHOW TABLE STATUS [LIKE '...']` | `pg_class` with size info |
| `SHOW [FULL] PROCESSLIST` | `pg_stat_activity` |
| `SHOW [GLOBAL] VARIABLES [LIKE '...']` | `pg_settings` |
| `SHOW [GLOBAL] STATUS [LIKE '...']` | `pg_stat_database` stats |
| `SHOW GRANTS [FOR user]` | `information_schema.role_table_grants` |
| `SHOW WARNINGS` | Collected PostgreSQL NOTICE messages |
| `SHOW ENGINES` | Returns `PostgreSQL` as the single engine |
| `SHOW ENGINE INNODB STATUS` | `pg_stat_activity` + `pg_locks` summary |
| `SHOW CHARACTER SET` | `information_schema.character_sets` |
| `SHOW COLLATION [LIKE '...']` | `pg_collation` |
| `SHOW CREATE DATABASE db` | Reconstructed from `pg_database` |

#### DML Translation

| MySQL | PostgreSQL |
|-------|-----------|
| `INSERT IGNORE INTO t ...` | `INSERT INTO t ... ON CONFLICT DO NOTHING` |
| `INSERT INTO t ... ON DUPLICATE KEY UPDATE col = VALUES(col)` | `INSERT INTO t ... ON CONFLICT (pk) DO UPDATE SET col = EXCLUDED.col` |
| `REPLACE INTO t ...` | `INSERT INTO t ... ON CONFLICT (pk) DO UPDATE SET ...` |

#### DDL Translation

| MySQL | PostgreSQL |
|-------|-----------|
| `CREATE DATABASE db [CHARACTER SET ...]` | `CREATE DATABASE db [ENCODING 'UTF8']` |
| `ALTER TABLE t MODIFY COLUMN col TYPE` | `ALTER TABLE t ALTER COLUMN col TYPE TYPE` |
| `ALTER TABLE t CHANGE old new TYPE` | `RENAME COLUMN` + `ALTER COLUMN TYPE` |
| `ALTER TABLE t ADD [UNIQUE] INDEX idx (col)` | `CREATE [UNIQUE] INDEX idx ON t (col)` |
| `ALTER TABLE t DROP INDEX idx` | `DROP INDEX idx` |
| `RENAME TABLE old TO new` | `ALTER TABLE old RENAME TO new` |
| `TRUNCATE TABLE t` | `TRUNCATE TABLE t RESTART IDENTITY` |
| MySQL-format `CREATE TABLE` with ENGINE/CHARSET | Full DDL translation to PG types |

#### Function Translation

| MySQL | PostgreSQL |
|-------|-----------|
| `DATABASE()` | `current_database()` |
| `IFNULL(a, b)` | `COALESCE(a, b)` |

#### User Management

| MySQL | PostgreSQL |
|-------|-----------|
| `CREATE USER 'user'@'host' IDENTIFIED BY 'pass'` | `CREATE ROLE user WITH LOGIN PASSWORD 'pass'` |
| `DROP USER 'user'@'host'` | `DROP ROLE user` |
| `ALTER USER 'user'@'host' IDENTIFIED BY 'pass'` | `ALTER ROLE user WITH PASSWORD 'pass'` |
| `GRANT ... ON db.* TO 'user'@'host'` | `GRANT ... ON ALL TABLES IN SCHEMA public TO user` |
| `REVOKE ... ON db.* FROM 'user'@'host'` | `REVOKE ... ON ALL TABLES IN SCHEMA public FROM user` |
| `FLUSH PRIVILEGES` | No-op (PG doesn't need this) |

#### Admin Commands

| MySQL | PostgreSQL |
|-------|-----------|
| `KILL pid` | `SELECT pg_terminate_backend(pid)` |
| `KILL QUERY pid` | `SELECT pg_cancel_backend(pid)` |
| `SET GLOBAL var = val` | `ALTER SYSTEM SET var = val` |

#### Meta-Commands

| Command | Action |
|---------|--------|
| `USE db` | Reconnect to different database |
| `STATUS` / `\s` | Show connection info |
| `SOURCE file` / `\. file` | Execute SQL from file |
| `SYSTEM cmd` / `\! cmd` | Run shell command |
| `TEE file` / `NOTEE` | Start/stop output logging |
| `PAGER cmd` / `NOPAGER` | Set/clear output pager |
| `WARNINGS` / `NOWARNING` | Toggle NOTICE display |
| `DELIMITER str` | Change statement delimiter |
| `REHASH` / `\#` | Rebuild autocomplete cache |
| `CLEAR` / `\c` | Clear input buffer |
| `EXIT` / `QUIT` / `\q` | Exit |
| `HELP` / `\h` / `\?` | Show help |

### Interactive Features

- SQL syntax highlighting (via Pygments MySqlLexer)
- Tab completion for SQL keywords, table names, column names, database names
- Command history (saved to `~/.mysqlpg_history`)
- Auto-suggest from history
- Multi-line input (accumulates until delimiter)
- `\G` suffix for vertical output
- Custom prompt with `\u` (user), `\h` (host), `\d` (database), `\p` (port), `\D` (datetime)

### MySQL Dump Boilerplate Handling

mysqlpg can load MySQL dump files directly. The following MySQL dump statements are handled:

| Dump Statement | Handling |
|---------------|----------|
| `SET NAMES charset` | Translated to `SET client_encoding` |
| `SET FOREIGN_KEY_CHECKS = 0/1` | Translated to `SET session_replication_role = 'replica'/'origin'` |
| `SET CHARACTER_SET_CLIENT/RESULTS/CONNECTION` | No-op |
| `SET COLLATION_CONNECTION` | No-op |
| `LOCK TABLES ... WRITE` | No-op |
| `UNLOCK TABLES` | No-op |
| `ALTER TABLE ... DISABLE/ENABLE KEYS` | No-op |
| `/*!40101 ... */` conditional comments | Stripped in pre-processing |
| MySQL-format `CREATE TABLE` with ENGINE/AUTO_INCREMENT | Full DDL translation to PG |
| Zero dates (`'0000-00-00'`, `'0000-00-00 00:00:00'`) | Converted to `NULL` |
| Backtick-quoted identifiers | Converted to double-quoted PG identifiers |

---

## mysqldumppg

### CLI Flags

**Connection:** `-u`, `-p`, `-h`, `-P`, `-S` (same as mysqlpg)

**Database/Table Selection:**

| Flag | Description |
|------|-------------|
| (positional) | `mysqldumppg db [table1 table2 ...]` |
| `-B` / `--databases` | Treat all args as database names; emit CREATE DATABASE + USE |
| `--all-databases` | Dump every database |
| `--tables` | Override `--databases`, dump specific tables |
| `--ignore-table=db.table` | Exclude specific tables |

**DDL Options:**

| Flag | Description |
|------|-------------|
| `--add-drop-database` | Emit `DROP DATABASE IF EXISTS` |
| `--add-drop-table` | Emit `DROP TABLE IF EXISTS` (default: ON via `--opt`) |
| `-n` / `--no-create-db` | Suppress CREATE DATABASE |
| `-t` / `--no-create-info` | Suppress CREATE TABLE (data only) |
| `--create-options` | Include engine/charset in DDL (default: ON) |
| `--if-not-exists` | Add IF NOT EXISTS to CREATE TABLE |

**Data Options:**

| Flag | Description |
|------|-------------|
| `-d` / `--no-data` | Schema only, no data (structure dump) |
| `-c` / `--complete-insert` | Include column names in INSERT |
| `--extended-insert` | Multi-row INSERT (default: ON via `--opt`) |
| `--skip-extended-insert` | One INSERT per row |
| `--insert-ignore` | Use INSERT IGNORE |
| `--replace` | Use REPLACE INTO |
| `--hex-blob` | Hex-encode bytea/binary columns |
| `--where=condition` | Filter rows with WHERE clause |

**Locking/Consistency:**

| Flag | Description |
|------|-------------|
| `--single-transaction` | Wrap dump in SERIALIZABLE transaction |
| `--lock-tables` | Lock tables during dump (default: ON) |
| `--add-locks` | Emit LOCK/UNLOCK around data (default: ON) |
| `--no-autocommit` | Wrap INSERTs in SET autocommit=0 / COMMIT |

**Output:**

| Flag | Description |
|------|-------------|
| `--result-file=file` | Write to file instead of stdout |
| `--compact` | Minimal output (no comments, locks, charset) |
| `--opt` | Enable all optimization defaults (default: ON) |
| `--skip-opt` | Disable all `--opt` defaults |
| `--skip-comments` | Suppress header/footer comments |
| `--quote-names` | Backtick-quote identifiers (default: ON) |
| `--compatible=MODE` | Output compatibility mode (`pgloader`, `mysql`) |

**Stored Objects:**

| Flag | Description |
|------|-------------|
| `--routines` | Dump functions and procedures |
| `--triggers` | Dump triggers (default: ON) |
| `--skip-triggers` | Skip triggers |
| `--events` | Ignored (not applicable for PG) |

### Output Format

mysqldumppg produces output matching the mysqldump format:

```sql
-- mysqldumppg (PostgreSQL)  Distrib 0.1.0
--
-- Host: localhost    Database: mydb
-- Server version	16.2
-- ------------------------------------------------------

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

--
-- Table structure for table `users`
--

DROP TABLE IF EXISTS `users`;
CREATE TABLE `users` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=PostgreSQL DEFAULT CHARSET=utf8mb4;

--
-- Dumping data for table `users`
--

LOCK TABLES `users` WRITE;
INSERT INTO `users` VALUES (1,'Alice'),(2,'Bob');
UNLOCK TABLES;

-- Dump completed on 2026-03-19 12:00:00
```

### Features

- **FK-aware table ordering** - Tables are topologically sorted by foreign key dependencies
- **Server-side cursor streaming** - Large tables are streamed with configurable batch size (`--quick`)
- **ENUM type detection** - PG ENUM types are output as MySQL `ENUM('val1','val2')` syntax
- **MySQL type mapping** - PG types are mapped to their MySQL equivalents in DDL
- **AUTO_INCREMENT detection** - PG sequences (`nextval()`) are shown as AUTO_INCREMENT
- **Trigger and routine dumping** - PG triggers and functions included in output

---

## pgloader Compatibility

Both tools are compatible with pgloader migration workflows:

### pgloader -> mysqlpg (post-migration)

After pgloader migrates a MySQL database to PostgreSQL, mysqlpg handles pgloader-generated SQL:

| pgloader SQL | mysqlpg Handling |
|-------------|-----------------|
| `CREATE TYPE ... AS ENUM (...)` | Pass-through to PG |
| `DROP TYPE [IF EXISTS] ...` | Pass-through to PG |
| `ALTER TABLE ... DISABLE/ENABLE TRIGGER ALL` | Pass-through to PG |
| `COPY ... FROM STDIN` | Pass-through to PG |
| `CREATE [UNIQUE] INDEX ... ON ...` | Pass-through to PG |
| `ALTER TABLE ... ADD CONSTRAINT ... FOREIGN KEY ...` | Pass-through to PG |
| `SELECT setval(...)` | Pass-through to PG |
| `SET session_replication_role = ...` | Pass-through to PG |

### mysqldumppg -> pgloader (re-migration)

Dumps produced by mysqldumppg can be used in pgloader workflows:

```bash
# Standard round-trip
mysqldumppg -u postgres mydb > backup.sql
mysqlpg -u postgres newdb < backup.sql

# pgloader-compatible dump (includes CREATE TYPE for ENUMs)
mysqldumppg -u postgres --compatible=pgloader mydb > backup.sql
```

### Zero-Date Handling

MySQL allows `'0000-00-00'` and `'0000-00-00 00:00:00'` as date values, which PostgreSQL rejects. mysqlpg automatically converts these to `NULL` during SQL translation.

---

## Testing

```bash
# Run all tests
python -m pytest tests/

# Run with verbose output
python -m pytest tests/ -v

# Run with coverage
python -m pytest tests/ --cov=mysqlpg --cov-report=term-missing

# Run specific test module
python -m pytest tests/test_translator.py

# Run only unit tests (no live PG required)
python -m pytest tests/ -k "not Live and not RoundTrip and not Enum"
```

### Test Structure

| Module | Tests | Description |
|--------|-------|-------------|
| `test_translator.py` | SQL translation | SHOW commands, DML, DDL, functions, user mgmt, pgloader compat, type mapping |
| `test_formatter.py` | Output formatting | Table, batch, vertical modes; cell formatting; tee/pager |
| `test_cli.py` | CLI arg parsing | Password handling, flag combinations, database resolution |
| `test_commands.py` | Meta-commands | USE, STATUS, SOURCE, TEE, PAGER, DELIMITER, EXIT, etc. |
| `test_ddl.py` | DDL reconstruction | PG->MySQL type mapping, default cleaning, ENUM detection |
| `test_connection.py` | Connection mgmt | Connect, execute, reconnect, notices (mock + live PG) |
| `test_dumpcli.py` | Dump tool | Parser, options, value formatting, table sorting, INSERT generation |
| `test_interactive.py` | REPL features | Autocomplete, prompt expansion, keyword list |
| `test_pgloader.py` | pgloader compat | CREATE TYPE, DISABLE TRIGGER, COPY, setval, zero dates |
| `test_roundtrip.py` | Integration | Full dump->restore workflows (requires live PG) |

### Test Requirements

- **Unit tests** (350+): Run without a database, using mock connections
- **Integration tests** (100+): Require a running PostgreSQL with a `testdb` database
  - Set `PGHOST`, `PGPORT`, `PGUSER`, `PGDATABASE` env vars to configure
  - Skipped automatically if PostgreSQL is unreachable

---

## Architecture

```
mysqlpg/
├── __init__.py          # Version string
├── __main__.py          # python -m mysqlpg entry point
├── cli.py               # mysqlpg CLI argument parsing + main loop
├── connection.py        # psycopg2 connection wrapper (shared)
├── translator.py        # MySQL -> PG SQL translation engine (regex-based)
├── ddl.py               # DDL reconstruction from PG metadata (shared)
├── formatter.py         # Output formatting (table/batch/vertical)
├── commands.py          # Meta-command handling (USE, SOURCE, TEE, etc.)
├── interactive.py       # prompt_toolkit REPL with autocomplete
└── dumpcli.py           # mysqldumppg dump tool
```

### Key Design Decisions

1. **Port defaults to 5432** (not 3306) - connecting to PostgreSQL, not MySQL
2. **Regex-based translation** - SHOW commands have predictable syntax; no SQL parser needed
3. **autocommit=True** by default - matches MySQL CLI behavior
4. **USE reconnects** - PG requires a new connection to switch databases
5. **Cosmetic type mapping** - `int(11)` display width doesn't exist in PG but MySQL users expect it
6. **Custom table formatter** - exact MySQL `+---+` bordered output; no external library
7. **Prompt says `mysql>`** - maximum muscle-memory compatibility

---

## Limitations

### SQL Translation

- **Regex-based, not a full SQL parser** - Complex or unusual SQL syntax may not be recognized. The translator handles common patterns but not every MySQL SQL variation.
- **No stored procedure body translation** - Stored procedure and function bodies are dumped as-is from PostgreSQL. PL/pgSQL and MySQL stored procedure languages differ significantly.
- **No VIEW translation** - `CREATE VIEW` statements are not translated between MySQL and PG syntax.
- **No subquery translation** - SQL within subqueries is not recursively translated; only top-level statements are matched.
- **Single-schema assumption** - Most translations assume the `public` schema. Multi-schema databases may need manual adjustment.

### Type Mapping

- **Display widths are cosmetic** - MySQL's `int(11)` display width has no meaning in PostgreSQL. The mapping is for visual compatibility only.
- **ENUM types** - PG ENUM types are detected and mapped to MySQL ENUM syntax, but the reverse (MySQL ENUM -> PG) creates a TEXT column, not a CREATE TYPE. Use `--compatible=pgloader` for proper PG ENUM output.
- **SET type** - MySQL's SET type is mapped to TEXT (PG has no native SET equivalent).
- **Spatial types** - Basic mapping exists but PG's PostGIS types don't map cleanly to MySQL spatial types.
- **Unsigned integers** - PG has no unsigned integer types. The `unsigned` keyword is stripped during translation.

### MySQL Features Not Supported in PostgreSQL

- **`@'host'` in user management** - MySQL's user-host pairing has no PG equivalent. The host part is stripped with a notice pointing to `pg_hba.conf`.
- **`DISABLE KEYS`** - PG has no equivalent index-disabling mechanism. Accepted as a no-op.
- **`FLUSH PRIVILEGES`** - Not needed in PG. Returns "Query OK" as a no-op.
- **MySQL conditional comments (`/*!40101 ... */`)** - Stripped during pre-processing. Content inside is preserved if it looks like valid SQL.
- **`HANDLER` statements** - Not supported.
- **`LOAD DATA INFILE`** - Not translated. Use `COPY` directly or `\copy` in psql.

### Dump Limitations

- **No partial column dumps** - mysqldumppg always dumps all columns; there's no column selection.
- **DELIMITER in routines** - Routine dumps use `DELIMITER ;;` syntax which requires the custom delimiter support in mysqlpg for round-trip.
- **Event scheduler** - MySQL events have no PG equivalent. `--events` flag is accepted but ignored.
- **Replication flags** - `--master-data`, `--source-data`, `--flush-logs` are accepted for compatibility but produce only informational comments.
- **Character set mapping** - All output uses UTF-8. Character set conversions between MySQL charsets are not performed.
- **Partitioned tables** - Partition definitions in CREATE TABLE are not reconstructed in MySQL syntax.

### pgloader Compatibility Limitations

- **COPY FROM STDIN** - While the statement is recognized and passed through, the actual binary/text data stream following COPY requires the psycopg2 copy protocol, which mysqlpg does not handle in pipe mode. Use pgloader directly for bulk loading.
- **pgloader configuration files** - mysqlpg does not read or generate pgloader `.load` configuration files.
- **Custom type transformations** - pgloader's 24+ built-in transformation functions (e.g., `tinyint-to-boolean`, `zero-dates-to-null`) are not replicated. Zero-date conversion is built-in; others require manual handling.

### Connection Limitations

- **SSL/TLS** - Not configurable via CLI flags. Use `PGSSLMODE` and related environment variables.
- **Connection pooling** - No built-in connection pooling. Each reconnect (e.g., `USE db`) creates a new connection.
- **Kerberos/GSSAPI** - Not tested. Should work if psycopg2 and PG are configured for it.

### Interactive REPL Limitations

- **No mouse support** - Terminal-only interaction via prompt_toolkit.
- **No syntax validation** - SQL is sent to the translator/PG as-is; no client-side syntax checking.
- **Single-database context** - Unlike MySQL's cross-database queries (`db.table`), PG requires reconnection to switch databases.

---

## Environment Variables

| Variable | Description |
|----------|-------------|
| `PGHOST` | Default PostgreSQL host |
| `PGPORT` | Default PostgreSQL port |
| `PGUSER` | Default PostgreSQL user |
| `PGDATABASE` | Default database |
| `PGPASSWORD` | PostgreSQL password (insecure; prefer `-p` prompt) |
| `PGSSLMODE` | SSL mode (`disable`, `require`, `verify-full`, etc.) |

---

## License

MIT

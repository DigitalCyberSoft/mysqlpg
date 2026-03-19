"""MySQL-style output formatting: bordered tables, batch, vertical."""

import os
import shlex
import subprocess
import sys


class Formatter:
    """Handles all output formatting modes."""

    def __init__(self, batch=False, skip_column_names=False, table_mode=False,
                 raw=False, silent=False, vertical=False, auto_vertical=False,
                 pager=None, tee_file=None):
        self.batch = batch
        self.skip_column_names = skip_column_names
        self.table_mode = table_mode
        self.raw = raw
        self.silent = silent
        self.vertical = vertical
        self.auto_vertical = auto_vertical
        self.pager_cmd = pager
        self.tee_fp = None
        if tee_file:
            self.start_tee(tee_file)

    def start_tee(self, path):
        """Start logging output to a file."""
        try:
            self.tee_fp = open(path, "a")
        except IOError as e:
            print(f"ERROR: Could not open tee file: {e}", file=sys.stderr)

    def stop_tee(self):
        """Stop logging."""
        if self.tee_fp:
            self.tee_fp.close()
            self.tee_fp = None

    def set_pager(self, cmd):
        self.pager_cmd = cmd

    def clear_pager(self):
        self.pager_cmd = None

    def _output(self, text, file=None):
        """Write text, duplicating to tee file if active."""
        target = file or sys.stdout
        try:
            target.write(text)
            target.flush()
        except BrokenPipeError:
            pass
        if self.tee_fp and target is sys.stdout:
            try:
                self.tee_fp.write(text)
                self.tee_fp.flush()
            except IOError:
                pass

    def format_results(self, columns, rows, elapsed=0.0, vertical_override=False):
        """Format query results based on current mode settings."""
        use_vertical = vertical_override or self.vertical

        if use_vertical:
            return self._format_vertical(columns, rows, elapsed)

        if self.batch or (not sys.stdout.isatty() and not self.table_mode):
            return self._format_batch(columns, rows, elapsed)

        # Check auto-vertical
        if self.auto_vertical and columns and rows:
            table_output = self._build_table(columns, rows)
            try:
                term_width = os.get_terminal_size().columns
            except OSError:
                term_width = 80
            max_line = max(len(line) for line in table_output.split("\n")) if table_output else 0
            if max_line > term_width:
                return self._format_vertical(columns, rows, elapsed)

        return self._format_table(columns, rows, elapsed)

    def _format_table(self, columns, rows, elapsed=0.0):
        """MySQL-style bordered table output."""
        if not columns:
            return ""

        output = self._build_table(columns, rows)
        row_count = len(rows) if rows else 0
        if not self.silent:
            if row_count == 0:
                output += f"\nEmpty set ({elapsed:.2f} sec)\n"
            elif row_count == 1:
                output += f"\n1 row in set ({elapsed:.2f} sec)\n"
            else:
                output += f"\n{row_count} rows in set ({elapsed:.2f} sec)\n"
        return output

    def _build_table(self, columns, rows):
        """Build MySQL-style +---+---+ bordered table."""
        if not columns:
            return ""

        # Convert everything to strings
        str_rows = []
        for row in (rows or []):
            str_rows.append([self._format_cell(v) for v in row])

        # Compute column widths
        widths = [len(str(c)) for c in columns]
        for row in str_rows:
            for i, cell in enumerate(row):
                widths[i] = max(widths[i], len(cell))

        # Build separator
        sep = "+" + "+".join("-" * (w + 2) for w in widths) + "+"

        # Build header
        header = "|" + "|".join(
            f" {str(c).ljust(w)} " for c, w in zip(columns, widths)
        ) + "|"

        lines = [sep, header, sep]

        # Build data rows
        for row in str_rows:
            data_line = "|" + "|".join(
                f" {cell.ljust(w)} " for cell, w in zip(row, widths)
            ) + "|"
            lines.append(data_line)

        lines.append(sep)
        return "\n".join(lines)

    def _format_batch(self, columns, rows, elapsed=0.0):
        """Tab-separated output for batch/pipe mode."""
        lines = []

        if columns and not self.skip_column_names:
            lines.append("\t".join(str(c) for c in columns))

        for row in (rows or []):
            cells = []
            for v in row:
                if v is None:
                    cells.append("NULL")
                else:
                    cells.append(str(v))
            lines.append("\t".join(cells))

        return "\n".join(lines) + "\n" if lines else ""

    def _format_vertical(self, columns, rows, elapsed=0.0):
        """Vertical \\G output format."""
        if not columns or not rows:
            return "Empty set ({:.2f} sec)\n".format(elapsed)

        max_col_width = max(len(str(c)) for c in columns)
        lines = []

        for i, row in enumerate(rows, 1):
            lines.append(f"*************************** {i}. row ***************************")
            for col, val in zip(columns, row):
                display_val = "NULL" if val is None else str(val)
                lines.append(f"{str(col).rjust(max_col_width)}: {display_val}")

        row_count = len(rows)
        if not self.silent:
            if row_count == 1:
                lines.append(f"1 row in set ({elapsed:.2f} sec)")
            else:
                lines.append(f"{row_count} rows in set ({elapsed:.2f} sec)")

        return "\n".join(lines) + "\n"

    def format_status(self, status_message, rowcount, elapsed=0.0):
        """Format non-SELECT result (INSERT, UPDATE, etc.)."""
        if self.silent:
            return ""
        if rowcount >= 0:
            return f"Query OK, {rowcount} rows affected ({elapsed:.2f} sec)\n"
        return f"Query OK ({elapsed:.2f} sec)\n"

    def _format_cell(self, value):
        """Format a single cell value for display."""
        if value is None:
            return "NULL"
        if isinstance(value, bool):
            return "1" if value else "0"
        if isinstance(value, bytes):
            return value.hex()
        if isinstance(value, memoryview):
            return bytes(value).hex()
        return str(value)

    def print_results(self, columns, rows, elapsed=0.0, vertical_override=False):
        """Format and print results, using pager if configured."""
        text = self.format_results(columns, rows, elapsed, vertical_override)
        if not text:
            return

        if self.pager_cmd and sys.stdout.isatty():
            try:
                proc = subprocess.Popen(
                    shlex.split(self.pager_cmd), stdin=subprocess.PIPE,
                    encoding="utf-8"
                )
                proc.communicate(input=text)
                return
            except Exception:
                pass

        self._output(text)

    def print_status(self, status_message, rowcount, elapsed=0.0):
        """Print non-SELECT status line."""
        text = self.format_status(status_message, rowcount, elapsed)
        if text:
            self._output(text)

    def print_message(self, msg):
        """Print an informational message."""
        self._output(msg + "\n")

    def close(self):
        self.stop_tee()

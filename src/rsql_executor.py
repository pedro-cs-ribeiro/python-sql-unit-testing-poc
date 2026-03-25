"""
RSQL file executor.

Replicates the execution pattern used in production pipelines:
  1. Read the .rsql file from disk
  2. Apply template substitution (replace placeholders with runtime values)
  3. Apply Redshift-to-PostgreSQL compatibility transformations
  4. Execute the SQL statements against the database

In production, step 3 is not needed because the rsql CLI connects directly
to Redshift. In testing, we replace the rsql CLI with psycopg2 and add the
compatibility layer so .rsql files can run against a PostgreSQL test container.
"""

import os
import re

import psycopg2
from psycopg2.extensions import connection as PgConnection

from redshift_compat import convert_redshift_to_postgres, apply_template_substitution


def execute_rsql_file(
    conn: PgConnection,
    filepath: str,
    template_vars: dict[str, str] | None = None,
    skip_compatibility: bool = False,
) -> None:
    """
    Execute a .rsql file against a PostgreSQL connection.

    This mirrors the production flow where execute_sql_scripts.py reads
    an .rsql file and passes it to the rsql CLI tool. Here we use psycopg2
    instead, with an optional Redshift compatibility layer.

    Args:
        conn: An open psycopg2 connection.
        filepath: Path to the .rsql file to execute.
        template_vars: Optional dict of template variables to substitute
                       before execution (e.g., {"PREV_DATE": "2024-01-01"}).
        skip_compatibility: If True, skip Redshift-to-PostgreSQL transformations.
                            Useful if the SQL is already PostgreSQL-compatible.

    Raises:
        FileNotFoundError: If the .rsql file does not exist.
        psycopg2.Error: If any SQL statement fails to execute.
    """
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"File {filepath} doesn't exist")

    with open(filepath, 'r', encoding='utf-8') as f:
        sql_content = f.read()

    execute_rsql_string(
        conn=conn,
        sql=sql_content,
        template_vars=template_vars,
        skip_compatibility=skip_compatibility,
    )


def execute_rsql_string(
    conn: PgConnection,
    sql: str,
    template_vars: dict[str, str] | None = None,
    skip_compatibility: bool = False,
) -> None:
    """
    Execute raw SQL content against a PostgreSQL connection.

    Useful when you want to test SQL without writing it to a file first.

    Args:
        conn: An open psycopg2 connection.
        sql: Raw SQL string to execute.
        template_vars: Optional dict of template variables to substitute.
        skip_compatibility: If True, skip Redshift-to-PostgreSQL transformations.

    Raises:
        psycopg2.Error: If any SQL statement fails to execute.
    """
    if template_vars:
        sql = apply_template_substitution(sql, template_vars)

    if not skip_compatibility:
        sql = convert_redshift_to_postgres(sql)

    _execute_statements(conn, sql)


def _execute_statements(conn: PgConnection, sql: str) -> None:
    """
    Split SQL into individual statements and execute them sequentially.

    Handles the fact that .rsql files typically contain multiple statements
    separated by semicolons. Empty statements (from removed ANALYZE/COPY/UNLOAD
    commands) are skipped.

    Args:
        conn: An open psycopg2 connection.
        sql: SQL string potentially containing multiple statements.
    """
    # Split on semicolons, but respect quoted strings and dollar-quoted blocks
    statements = _split_statements(sql)

    with conn.cursor() as cur:
        for stmt in statements:
            stmt = stmt.strip()
            if stmt:
                cur.execute(stmt)
    conn.commit()


def _split_statements(sql: str) -> list[str]:
    """
    Split SQL into individual statements by semicolons, respecting:
    - Single-quoted strings ('...')
    - Double-quoted identifiers ("...")
    - Dollar-quoted strings ($$...$$)
    - Single-line comments (--)
    - Block comments (/* ... */)

    Args:
        sql: Full SQL string with potentially multiple statements.

    Returns:
        List of individual SQL statements (without trailing semicolons).
    """
    statements = []
    current = []
    i = 0
    length = len(sql)
    in_single_quote = False
    in_double_quote = False
    in_dollar_quote = False
    dollar_tag = ""
    in_line_comment = False
    in_block_comment = False

    while i < length:
        char = sql[i]

        # Handle line comments
        if in_line_comment:
            current.append(char)
            if char == '\n':
                in_line_comment = False
            i += 1
            continue

        # Handle block comments
        if in_block_comment:
            current.append(char)
            if char == '*' and i + 1 < length and sql[i + 1] == '/':
                current.append('/')
                in_block_comment = False
                i += 2
            else:
                i += 1
            continue

        # Handle dollar quoting
        if in_dollar_quote:
            current.append(char)
            if char == '$':
                # Check if we're closing the dollar quote
                end_tag = sql[i:i + len(dollar_tag)]
                if end_tag == dollar_tag:
                    current.extend(list(dollar_tag[1:]))  # already appended first $
                    in_dollar_quote = False
                    i += len(dollar_tag)
                else:
                    i += 1
            else:
                i += 1
            continue

        # Handle single-quoted strings
        if in_single_quote:
            current.append(char)
            if char == "'" and i + 1 < length and sql[i + 1] == "'":
                current.append("'")
                i += 2
            elif char == "'":
                in_single_quote = False
                i += 1
            else:
                i += 1
            continue

        # Handle double-quoted identifiers
        if in_double_quote:
            current.append(char)
            if char == '"':
                in_double_quote = False
            i += 1
            continue

        # Detect start of comments
        if char == '-' and i + 1 < length and sql[i + 1] == '-':
            in_line_comment = True
            current.append(char)
            i += 1
            continue

        if char == '/' and i + 1 < length and sql[i + 1] == '*':
            in_block_comment = True
            current.append(char)
            current.append('*')
            i += 2
            continue

        # Detect start of quotes
        if char == "'":
            in_single_quote = True
            current.append(char)
            i += 1
            continue

        if char == '"':
            in_double_quote = True
            current.append(char)
            i += 1
            continue

        if char == '$':
            # Check for dollar-quoting ($$, $tag$, etc.)
            match = re.match(r'\$[a-zA-Z_]*\$', sql[i:])
            if match:
                dollar_tag = match.group(0)
                in_dollar_quote = True
                current.extend(list(dollar_tag))
                i += len(dollar_tag)
                continue

        # Statement separator
        if char == ';':
            statements.append(''.join(current))
            current = []
            i += 1
            continue

        current.append(char)
        i += 1

    # Don't forget the last statement (may not end with ;)
    remaining = ''.join(current).strip()
    if remaining:
        statements.append(remaining)

    return statements

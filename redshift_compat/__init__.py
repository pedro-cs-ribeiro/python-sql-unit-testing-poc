"""
Redshift SQL compatibility layer for PostgreSQL.

Uses sqlglot to parse Redshift SQL into an AST and generate PostgreSQL-compatible
SQL. This replaces hand-written regex transformations with a proper SQL transpiler
that correctly handles nested expressions, quoting, and dialect differences.

A small number of regex pre-processing steps handle syntax that sqlglot's Redshift
parser does not cover (inline DISTKEY keyword, INTERLEAVED SORTKEY) and statements
that should be stripped entirely for testing (ANALYZE, COPY FROM S3, UNLOAD).
"""

import re

import sqlglot
from sqlglot import exp


def convert_redshift_to_postgres(sql: str) -> str:
    """
    Convert Redshift-specific SQL to PostgreSQL-compatible SQL.

    Uses sqlglot to transpile from the Redshift dialect to PostgreSQL,
    handling function replacements (DECODE, NVL, SYSDATE, GETDATE, CHARINDEX),
    and stripping Redshift-specific DDL properties (DISTKEY, SORTKEY, DISTSTYLE,
    ENCODE). Infrastructure commands (ANALYZE, COPY, UNLOAD) are removed via
    regex pre-processing since they are not relevant to logic testing.

    Args:
        sql: Raw SQL string from a .rsql file.

    Returns:
        Transformed SQL string compatible with PostgreSQL.
    """
    sql = _pre_process(sql)

    results = []
    for statement in sqlglot.parse(sql, read="redshift"):
        if statement is None:
            continue
        _strip_redshift_properties(statement)
        results.append(statement.sql(dialect="postgres"))

    return ";\n".join(results)


def apply_template_substitution(sql: str, variables: dict[str, str]) -> str:
    """
    Apply template variable substitution to SQL content.

    Replaces placeholders in the SQL with provided values. Supports both
    @VARIABLE@ style (used by some pipelines) and $VARIABLE style
    (used by other pipelines).

    Args:
        sql: Raw SQL string that may contain template placeholders.
        variables: Dictionary mapping placeholder names to replacement values.
                   Keys should NOT include the @ or $ delimiters.

    Returns:
        SQL string with placeholders replaced.
    """
    for key, value in variables.items():
        sql = sql.replace(f"@{key}@", value)
        sql = sql.replace(f"${key}", value)
    return sql


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _pre_process(sql: str) -> str:
    """
    Regex pre-processing for syntax that sqlglot's Redshift parser does not
    handle, and for commands that should be stripped entirely in tests.
    """
    # Inline DISTKEY keyword (e.g., 'PRIMARY KEY DISTKEY') — not a function call
    sql = re.sub(r"\bDISTKEY\b(?!\s*\()", "", sql, flags=re.IGNORECASE)
    # INTERLEAVED SORTKEY — sqlglot only handles plain and COMPOUND SORTKEY
    sql = re.sub(
        r"\bINTERLEAVED\s+SORTKEY\s*\([^)]*\)", "", sql, flags=re.IGNORECASE
    )
    # ANALYZE — valid in PostgreSQL but unnecessary overhead in tests
    sql = re.sub(
        r"^\s*ANALYZE\b[^;]*;", "", sql, flags=re.IGNORECASE | re.MULTILINE
    )
    # COPY ... FROM 's3://...' — Redshift S3 loading, not testable locally
    sql = re.sub(
        r"^\s*COPY\b[^;]*FROM\s+'s3://[^;]*;",
        "",
        sql,
        flags=re.IGNORECASE | re.MULTILINE,
    )
    # UNLOAD (...) TO 's3://...' — Redshift S3 export, not testable locally
    sql = re.sub(
        r"^\s*UNLOAD\s*\(.*?\)\s*TO\b[^;]*;",
        "",
        sql,
        flags=re.IGNORECASE | re.DOTALL | re.MULTILINE,
    )
    return sql


def _strip_redshift_properties(statement: exp.Expression) -> None:
    """
    Remove Redshift-specific DDL properties from a parsed AST node.

    Strips DISTKEY, SORTKEY, DISTSTYLE (table-level) and ENCODE (column-level)
    properties that have no PostgreSQL equivalent.
    """
    for node in list(statement.walk()):
        if isinstance(
            node,
            (exp.DistKeyProperty, exp.SortKeyProperty, exp.DistStyleProperty),
        ):
            node.pop()
        if isinstance(node, exp.EncodeColumnConstraint):
            node.parent.pop()

"""
Redshift SQL compatibility layer for PostgreSQL.

Transforms Redshift-specific SQL syntax into PostgreSQL-compatible SQL,
enabling .rsql files to be tested against a standard PostgreSQL instance.
"""

import re


def convert_redshift_to_postgres(sql: str) -> str:
    """
    Convert Redshift-specific SQL to PostgreSQL-compatible SQL.

    Applies a series of transformations to handle syntax differences between
    Amazon Redshift and standard PostgreSQL. This allows .rsql files written
    for Redshift to execute against a PostgreSQL test database.

    Args:
        sql: Raw SQL string from a .rsql file.

    Returns:
        Transformed SQL string compatible with PostgreSQL.
    """
    sql = _remove_distkey(sql)
    sql = _remove_sortkey(sql)
    sql = _remove_diststyle(sql)
    sql = _remove_encode(sql)
    sql = _replace_decode(sql)
    sql = _replace_nvl(sql)
    sql = _replace_sysdate(sql)
    sql = _replace_charindex(sql)
    sql = _replace_getdate(sql)
    sql = _remove_analyze(sql)
    sql = _remove_copy_commands(sql)
    sql = _remove_unload_commands(sql)
    return sql


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
        # Support @VAR@ style placeholders
        sql = sql.replace(f"@{key}@", value)
        # Support $VAR style placeholders
        sql = sql.replace(f"${key}", value)
    return sql


# ---------------------------------------------------------------------------
# Internal transformation functions
# ---------------------------------------------------------------------------


def _remove_distkey(sql: str) -> str:
    """Remove DISTKEY(...) clauses from CREATE TABLE and column definitions."""
    # Remove standalone DISTKEY(column) clauses
    sql = re.sub(r'\bDISTKEY\s*\([^)]*\)', '', sql, flags=re.IGNORECASE)
    # Remove DISTKEY keyword when used as column constraint (e.g., PRIMARY KEY DISTKEY)
    sql = re.sub(r'\bDISTKEY\b', '', sql, flags=re.IGNORECASE)
    return sql


def _remove_sortkey(sql: str) -> str:
    """Remove SORTKEY(...) and COMPOUND SORTKEY(...) / INTERLEAVED SORTKEY(...)."""
    sql = re.sub(
        r'\b(COMPOUND\s+|INTERLEAVED\s+)?SORTKEY\s*\([^)]*\)',
        '',
        sql,
        flags=re.IGNORECASE,
    )
    return sql


def _remove_diststyle(sql: str) -> str:
    """Remove DISTSTYLE KEY|ALL|EVEN clauses."""
    sql = re.sub(
        r'\bDISSTYLE\s+(KEY|ALL|EVEN|AUTO)\b', '', sql, flags=re.IGNORECASE
    )
    sql = re.sub(
        r'\bDISTSTYLE\s+(KEY|ALL|EVEN|AUTO)\b', '', sql, flags=re.IGNORECASE
    )
    return sql


def _remove_encode(sql: str) -> str:
    """Remove ENCODE compression_type from column definitions."""
    sql = re.sub(
        r'\bENCODE\s+(RAW|AZ64|BYTEDICT|DELTA|DELTA32K|LZO|MOSTLY8|MOSTLY16|MOSTLY32|RUNLENGTH|TEXT255|TEXT32K|ZSTD)\b',
        '',
        sql,
        flags=re.IGNORECASE,
    )
    return sql


def _replace_decode(sql: str) -> str:
    """
    Replace Redshift DECODE(expr, val1, result1, ..., default) with
    PostgreSQL CASE WHEN expressions.

    DECODE is equivalent to:
        CASE WHEN expr = val1 THEN result1
             WHEN expr = val2 THEN result2
             ...
             ELSE default
        END
    """

    def _decode_to_case(match: re.Match) -> str:
        inner = match.group(1)
        args = _split_sql_args(inner)
        if len(args) < 3:
            return match.group(0)  # Not a valid DECODE, leave as-is

        expr = args[0].strip()
        pairs = args[1:]
        parts = ["CASE"]
        i = 0
        while i + 1 < len(pairs):
            parts.append(f" WHEN {expr} = {pairs[i].strip()} THEN {pairs[i+1].strip()}")
            i += 2
        if i < len(pairs):
            parts.append(f" ELSE {pairs[i].strip()}")
        parts.append(" END")
        return "".join(parts)

    # Match DECODE(...) but not other functions ending in "decode"
    sql = re.sub(
        r'\bDECODE\s*\((.+?)\)(?=\s*(?:AS\b|,|\s|$|\)))',
        _decode_to_case,
        sql,
        flags=re.IGNORECASE | re.DOTALL,
    )
    return sql


def _replace_nvl(sql: str) -> str:
    """Replace NVL(a, b) with COALESCE(a, b)."""
    sql = re.sub(r'\bNVL\s*\(', 'COALESCE(', sql, flags=re.IGNORECASE)
    return sql


def _replace_sysdate(sql: str) -> str:
    """Replace SYSDATE with CURRENT_TIMESTAMP."""
    sql = re.sub(r'\bSYSDATE\b', 'CURRENT_TIMESTAMP', sql, flags=re.IGNORECASE)
    return sql


def _replace_charindex(sql: str) -> str:
    """
    Replace CHARINDEX(substr, str) with POSITION(substr IN str).

    Redshift: CHARINDEX(substring, string)
    PostgreSQL: POSITION(substring IN string)
    """
    def _charindex_to_position(match: re.Match) -> str:
        args = _split_sql_args(match.group(1))
        if len(args) != 2:
            return match.group(0)
        return f"POSITION({args[0].strip()} IN {args[1].strip()})"

    sql = re.sub(
        r'\bCHARINDEX\s*\((.+?)\)',
        _charindex_to_position,
        sql,
        flags=re.IGNORECASE,
    )
    return sql


def _replace_getdate(sql: str) -> str:
    """Replace GETDATE() with CURRENT_TIMESTAMP."""
    sql = re.sub(r'\bGETDATE\s*\(\s*\)', 'CURRENT_TIMESTAMP', sql, flags=re.IGNORECASE)
    return sql


def _remove_analyze(sql: str) -> str:
    """
    Remove standalone ANALYZE statements.

    Redshift uses ANALYZE to update table statistics after bulk operations.
    PostgreSQL has ANALYZE too, but in a test context it's unnecessary overhead.
    """
    sql = re.sub(
        r'^\s*ANALYZE\b[^;]*;',
        '',
        sql,
        flags=re.IGNORECASE | re.MULTILINE,
    )
    return sql


def _remove_copy_commands(sql: str) -> str:
    """
    Remove Redshift COPY commands (S3 loading).

    COPY ... FROM 's3://...' iam_role '...' cannot be executed against PostgreSQL.
    These commands are infrastructure-specific and not part of SQL logic testing.
    """
    sql = re.sub(
        r'^\s*COPY\b[^;]*;',
        '',
        sql,
        flags=re.IGNORECASE | re.MULTILINE,
    )
    return sql


def _remove_unload_commands(sql: str) -> str:
    """
    Remove Redshift UNLOAD commands (S3 export).

    UNLOAD ('SELECT ...') TO 's3://...' cannot be executed against PostgreSQL.
    """
    sql = re.sub(
        r'^\s*UNLOAD\s*\(.*?\)\s*TO\b[^;]*;',
        '',
        sql,
        flags=re.IGNORECASE | re.DOTALL | re.MULTILINE,
    )
    # Handle unload without parenthesized query
    sql = re.sub(
        r'^\s*UNLOAD\b[^;]*;',
        '',
        sql,
        flags=re.IGNORECASE | re.MULTILINE,
    )
    return sql


def _split_sql_args(s: str) -> list[str]:
    """
    Split a comma-separated SQL argument string, respecting nested parentheses
    and quoted strings.
    """
    args = []
    depth = 0
    current = []
    in_single_quote = False
    in_double_quote = False

    for char in s:
        if char == "'" and not in_double_quote:
            in_single_quote = not in_single_quote
            current.append(char)
        elif char == '"' and not in_single_quote:
            in_double_quote = not in_double_quote
            current.append(char)
        elif char == '(' and not in_single_quote and not in_double_quote:
            depth += 1
            current.append(char)
        elif char == ')' and not in_single_quote and not in_double_quote:
            depth -= 1
            current.append(char)
        elif char == ',' and depth == 0 and not in_single_quote and not in_double_quote:
            args.append(''.join(current))
            current = []
        else:
            current.append(char)

    if current:
        args.append(''.join(current))

    return args

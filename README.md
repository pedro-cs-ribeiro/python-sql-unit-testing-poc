# RSQL Unit Testing POC

A proof-of-concept for unit testing Redshift SQL (`.rsql`) scripts using **pytest** and **Testcontainers**, targeting Python-based data pipelines that execute raw SQL files against Amazon Redshift.

## Problem Statement

Many data engineering pipelines execute raw `.rsql` files against Amazon Redshift using CLI tools (such as `rsql`) or JDBC connections. These pipelines typically have:

- Hundreds of `.rsql` files containing complex SQL logic (joins, aggregations, reconciliation, ETL transformations)
- No unit tests validating the SQL logic
- No way to test locally without access to a Redshift cluster
- Orchestration via Apache Airflow (or similar) with Python executor scripts

This POC demonstrates how to test these `.rsql` files locally using a PostgreSQL container as a Redshift substitute, with a compatibility layer that handles Redshift-specific syntax differences.

## Architecture

### How Production Pipelines Execute SQL

```
Airflow DAG
  -> BashOperator
    -> Python execute_sql_scripts.py
      -> Reads config list (e.g., script_list.txt) for .rsql file names
      -> For each .rsql file:
          -> Optional template substitution (@PREV_DATE@, $S3PATH, etc.)
          -> subprocess.Popen("rsql -D dsninstanceprofile -f <file> -a")
          -> rsql CLI connects to Redshift via DSN profile
          -> Executes the SQL
          -> Checks stdout for errors via regex
```

### How This POC Tests SQL

```
pytest
  -> Testcontainers starts a PostgreSQL 16 container
  -> For each test:
      -> Schema is created (from schema.sql)
      -> Seed data is loaded (from seed_data.sql)
      -> rsql_executor.py:
          -> Reads the .rsql file (same as production)
          -> Applies template substitution (same as production)
          -> Applies Redshift-to-PostgreSQL compatibility transforms (NEW)
          -> Executes via psycopg2 (replaces rsql CLI)
      -> Test assertions validate database state
```

The key insight is that `rsql_executor.py` mirrors the production `execute_sql_scripts.py` flow, but replaces the `rsql` CLI (which requires a Redshift cluster) with `psycopg2` (which connects to a local PostgreSQL container). A compatibility layer bridges the syntax differences.

## Project Structure

```
rsql-unit-testing-poc/
├── pyproject.toml                      # Project metadata and pytest config
├── requirements.txt                    # Python dependencies
├── conftest.py                         # Root conftest (adds project to sys.path)
├── redshift_compat/
│   ├── __init__.py                     # Redshift-to-PostgreSQL SQL transformer
│   └── compat.py                       # Re-exports for convenience
├── src/
│   ├── __init__.py
│   └── rsql_executor.py               # Executes .rsql files against PostgreSQL
├── sql/
│   ├── schema.sql                      # Test database DDL (derived from prod DDL)
│   ├── seed_data.sql                   # Test seed data
│   └── sample_queries/                 # Sample .rsql files (mirror prod patterns)
│       ├── ctas_recon.rsql             # CREATE TABLE AS SELECT (reconciliation)
│       ├── truncate_insert.rsql        # TRUNCATE + INSERT with transformations
│       ├── delete_existing.rsql        # DELETE ... WHERE EXISTS
│       ├── stored_procedure.rsql       # CREATE OR REPLACE PROCEDURE
│       ├── call_procedures.rsql        # CALL procedure(args)
│       ├── insert_mapping.rsql         # INSERT ... WHERE NOT EXISTS (idempotent)
│       ├── template_date_query.rsql    # Template substitution (@PREV_DATE@)
│       └── daily_stats_ddl.rsql        # Supporting DDL for template tests
└── tests/
    ├── conftest.py                     # Test fixtures (Testcontainers, DB setup)
    ├── test_ctas_queries.py            # Tests for CTAS reconciliation pattern
    ├── test_insert_queries.py          # Tests for TRUNCATE + INSERT pattern
    ├── test_delete_queries.py          # Tests for DELETE pattern
    ├── test_stored_procedures.py       # Tests for stored procedure pattern
    ├── test_insert_mapping.py          # Tests for idempotent insert pattern
    ├── test_template_substitution.py   # Tests for template variable replacement
    └── test_redshift_compat.py         # Tests for the compatibility layer itself
```

## SQL Patterns Covered

These patterns were identified from real production pipelines and are represented in the sample `.rsql` files:

| Pattern | File | Description |
|---|---|---|
| **CTAS** | `ctas_recon.rsql` | `DROP TABLE IF EXISTS` + `CREATE TABLE ... AS SELECT` with FULL OUTER JOIN reconciliation logic. Uses DISTKEY, SORTKEY, NVL, SYSDATE. |
| **TRUNCATE + INSERT** | `truncate_insert.rsql` | Clear target table and reload from source with transformations. Uses DECODE, NVL, NULLIF, TRIM. |
| **DELETE** | `delete_existing.rsql` | `DELETE ... WHERE EXISTS` to remove stale records based on source data. |
| **Stored Procedures** | `stored_procedure.rsql` | `CREATE OR REPLACE PROCEDURE` with PL/pgSQL and dynamic SQL (`EXECUTE`). |
| **Procedure Calls** | `call_procedures.rsql` | Multiple `CALL procedure(args)` statements in sequence. |
| **Idempotent Insert** | `insert_mapping.rsql` | `INSERT ... WHERE NOT EXISTS` with surrogate key generation via `MAX() + ROW_NUMBER()`. |
| **Template Substitution** | `template_date_query.rsql` | SQL with `@PREV_DATE@` placeholders replaced at runtime. |

## Redshift Compatibility Layer

Amazon Redshift is based on PostgreSQL but has significant syntax differences. The compatibility layer (`redshift_compat/__init__.py`) handles these transformations:

| Redshift Syntax | PostgreSQL Equivalent | Transformation |
|---|---|---|
| `DISTKEY(col)` | *(removed)* | Table distribution — not applicable to PostgreSQL |
| `SORTKEY(col)` | *(removed)* | Table sort order — not applicable to PostgreSQL |
| `COMPOUND SORTKEY(...)` | *(removed)* | Compound sort keys |
| `INTERLEAVED SORTKEY(...)` | *(removed)* | Interleaved sort keys |
| `DISTSTYLE KEY\|ALL\|EVEN` | *(removed)* | Distribution style |
| `ENCODE LZO\|ZSTD\|...` | *(removed)* | Column compression encoding |
| `DECODE(expr, v1, r1, ..., default)` | `CASE WHEN expr=v1 THEN r1 ... ELSE default END` | Conditional expression |
| `NVL(a, b)` | `COALESCE(a, b)` | NULL handling |
| `SYSDATE` | `CURRENT_TIMESTAMP` | Current timestamp |
| `GETDATE()` | `CURRENT_TIMESTAMP` | Current timestamp |
| `CHARINDEX(sub, str)` | `POSITION(sub IN str)` | String search |
| `ANALYZE table` | *(removed)* | Statistics update — unnecessary in tests |
| `COPY ... FROM 's3://...'` | *(removed)* | S3 data loading — infrastructure-specific |
| `UNLOAD (...) TO 's3://...'` | *(removed)* | S3 data export — infrastructure-specific |

## Prerequisites

- **Python 3.9+**
- **Docker** (required by Testcontainers to run the PostgreSQL container)
- **pip** (for installing dependencies)

## Quick Start

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Run all tests

```bash
pytest
```

### 3. Run tests with coverage

```bash
pytest --cov=src --cov=redshift_compat --cov-report=term-missing
```

### 4. Run a specific test file

```bash
pytest tests/test_ctas_queries.py -v
```

## How to Integrate Into Your Pipeline Repository

### Step 1: Add test dependencies

Add the following to your pipeline project (via `requirements.txt` or `pyproject.toml`):

```
pytest>=8.0.0
testcontainers[postgres]>=4.4.0
psycopg2-binary>=2.9.9
```

### Step 2: Copy the core modules

Copy these into your project:

- `redshift_compat/__init__.py` — the Redshift-to-PostgreSQL compatibility layer
- `src/rsql_executor.py` — the test-oriented SQL executor

### Step 3: Create your schema and seed data

Derive your test schema from your production DDL files:

1. Take your production DDL (e.g., `Entities_DDL.sql`)
2. Run it through the compatibility layer to strip Redshift-specific syntax
3. Save as `tests/sql/schema.sql`
4. Create seed data that exercises your SQL logic in `tests/sql/seed_data.sql`

### Step 4: Write tests for your .rsql files

Follow the patterns in the `tests/` directory. For each `.rsql` file you want to test:

1. Set up the required database state (schema + seed data via fixtures)
2. Execute the `.rsql` file using `execute_rsql_file()`
3. Assert the expected database state after execution

```python
from src.rsql_executor import execute_rsql_file

def test_my_rsql_script(db_conn):
    """Test that my_script.rsql produces expected results."""
    execute_rsql_file(db_conn, "sql/my_script.rsql")

    with db_conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM target_schema.target_table")
        count = cur.fetchone()[0]

    assert count == 42
```

### Step 5: Add to CI/CD pipeline

Add a test stage to your pipeline configuration. Example for Azure DevOps:

```yaml
- stage: Test
  jobs:
    - job: SQLUnitTests
      pool:
        vmImage: 'ubuntu-latest'
      steps:
        - task: UsePythonVersion@0
          inputs:
            versionSpec: '3.11'
        - script: |
            pip install -r requirements.txt
            pytest --junitxml=test-results.xml --cov=src --cov-report=xml
          displayName: 'Run SQL unit tests'
        - task: PublishTestResults@2
          inputs:
            testResultsFormat: 'JUnit'
            testResultsFiles: 'test-results.xml'
```

## Test Fixtures

The `tests/conftest.py` provides two main fixtures:

| Fixture | Scope | Description |
|---|---|---|
| `db_conn` | per-test | Connection with schema + seed data loaded. Use for most tests. |
| `db_conn_empty` | per-test | Connection with schema only (no seed data). Use when you need full control over test data. |
| `sample_query_path` | per-test | Helper to resolve paths to sample `.rsql` files. |

The PostgreSQL container is started **once per test session** (not per test) for performance. Each test gets a fresh database state via `DROP SCHEMA CASCADE` + recreation.

## Limitations

1. **COPY/UNLOAD commands are stripped** — These are Redshift-specific S3 operations that cannot be tested against PostgreSQL. If your SQL logic depends on data loaded via COPY, you need to seed that data via INSERT statements in your test setup.

2. **Some Redshift functions may not have PostgreSQL equivalents** — The compatibility layer handles the most common cases. If you encounter unsupported functions, extend `redshift_compat/__init__.py` with additional transformations.

3. **Redshift-specific data types** — Some Redshift types (e.g., `SUPER`, `HLLSKETCH`, `GEOMETRY`) don't exist in PostgreSQL. The current compatibility layer does not handle type conversions.

4. **Query performance characteristics differ** — PostgreSQL won't reflect Redshift's distribution and sort key optimisations. Tests validate correctness, not performance.

5. **External dependencies** — SQL that references external resources (S3, Vault, SFG, etc.) must be mocked or excluded from unit tests.

## Extending the Compatibility Layer

To add support for additional Redshift-specific syntax:

1. Add a new transformation function in `redshift_compat/__init__.py`
2. Call it from `convert_redshift_to_postgres()`
3. Add tests in `tests/test_redshift_compat.py`

Example:

```python
def _replace_regexp_substr(sql: str) -> str:
    """Replace Redshift REGEXP_SUBSTR with PostgreSQL equivalent."""
    # Redshift: REGEXP_SUBSTR(string, pattern)
    # PostgreSQL: SUBSTRING(string FROM pattern)
    sql = re.sub(
        r'\bREGEXP_SUBSTR\s*\((.+?),\s*(.+?)\)',
        r'SUBSTRING(\1 FROM \2)',
        sql,
        flags=re.IGNORECASE,
    )
    return sql
```

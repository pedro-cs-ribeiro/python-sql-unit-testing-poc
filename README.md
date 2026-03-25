# RSQL Unit Testing POC

A proof-of-concept for unit testing Redshift SQL (`.rsql`) scripts using **pytest** and **Testcontainers**, targeting Python-based data pipelines that execute raw SQL files against Amazon Redshift.

## Problem Statement

Many data engineering pipelines execute raw `.rsql` files against Amazon Redshift using CLI tools (such as `rsql`) or JDBC connections. These pipelines typically have:

- Hundreds of `.rsql` files containing complex SQL logic (joins, aggregations, reconciliation, ETL transformations)
- No unit tests validating the SQL logic
- No way to test locally without access to a Redshift cluster
- Orchestration via Apache Airflow (or similar) with Python executor scripts

This POC demonstrates how to test these `.rsql` files locally using a PostgreSQL container as a Redshift substitute, with [sqlglot](https://github.com/tobymao/sqlglot) handling the dialect translation.

## What This Tests

This POC tests **SQL logic correctness**: do your JOINs, aggregations, CASE expressions, and data transformations produce the right rows? It does this by transpiling Redshift SQL to PostgreSQL and running it against a local container.

For most teams, logic correctness is where the bugs are. If your SQL produces wrong results, the issue is almost always in the logic (a bad JOIN condition, a missing NULL check, an off-by-one in a date filter) rather than a Redshift-vs-PostgreSQL behavioural difference. This POC catches those bugs.

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
          -> Transpiles Redshift SQL to PostgreSQL via sqlglot
          -> Executes via psycopg2 (replaces rsql CLI)
      -> Test assertions validate database state
```

The key insight is that `rsql_executor.py` mirrors the production `execute_sql_scripts.py` flow, but replaces the `rsql` CLI (which requires a Redshift cluster) with `psycopg2` (which connects to a local PostgreSQL container). sqlglot bridges the dialect differences.

## Project Structure

```
rsql-unit-testing-poc/
├── pyproject.toml                      # Project metadata and pytest config
├── requirements.txt                    # Python dependencies
├── conftest.py                         # Root conftest (adds project to sys.path)
├── redshift_compat/
│   └── __init__.py                     # Redshift-to-PostgreSQL transpiler (sqlglot)
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

The compatibility layer (`redshift_compat/__init__.py`) uses [sqlglot](https://github.com/tobymao/sqlglot) to parse Redshift SQL into an AST and generate PostgreSQL-compatible SQL. This is a proper SQL transpiler that correctly handles nested expressions, quoting, and dialect differences. Infrastructure commands that aren't relevant to logic testing (ANALYZE, COPY, UNLOAD) are stripped via a small regex pre-processing step.

## Testing Against Real Redshift

For highest fidelity testing, you can run tests directly against an Amazon Redshift Serverless workgroup. This eliminates the need for any compatibility layer — your `.rsql` files execute against the real engine.

### How to set it up

1. Create a Redshift Serverless workgroup with auto-pause enabled (no cost when idle, ~$0.375/RPU-hour when active)
2. Replace the Testcontainers fixture with a connection to your Redshift endpoint
3. Set `skip_compatibility=True` in `execute_rsql_file()` calls — no transpilation needed
4. Store credentials via AWS Secrets Manager or environment variables in CI

```python
@pytest.fixture(scope="session")
def db_conn():
    """Connect to Redshift Serverless for integration tests."""
    conn = psycopg2.connect(
        host=os.environ["REDSHIFT_HOST"],
        port=5439,
        dbname=os.environ["REDSHIFT_DB"],
        user=os.environ["REDSHIFT_USER"],
        password=os.environ["REDSHIFT_PASSWORD"],
    )
    yield conn
    conn.close()
```

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

## Test Fixtures

The `tests/conftest.py` provides two main fixtures:

| Fixture | Scope | Description |
|---|---|---|
| `db_conn` | per-test | Connection with schema + seed data loaded. Use for most tests. |
| `db_conn_empty` | per-test | Connection with schema only (no seed data). Use when you need full control over test data. |
| `sample_query_path` | per-test | Helper to resolve paths to sample `.rsql` files. |

The PostgreSQL container is started **once per test session** (not per test) for performance. Each test gets a fresh database state via `DROP SCHEMA CASCADE` + recreation.


"""
Test fixtures for the RSQL unit testing POC.

Provides a PostgreSQL Testcontainer that mirrors the Redshift database used
in production. The container is started once per test session, and each test
gets a fresh database state via schema recreation and seed data loading.
"""

import os

import psycopg2
import pytest
from testcontainers.postgres import PostgresContainer

from src.rsql_executor import execute_rsql_file


# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))
SQL_DIR = os.path.join(PROJECT_ROOT, "sql")
SAMPLE_QUERIES_DIR = os.path.join(SQL_DIR, "sample_queries")
SCHEMA_FILE = os.path.join(SQL_DIR, "schema.sql")
SEED_FILE = os.path.join(SQL_DIR, "seed_data.sql")


# ---------------------------------------------------------------------------
# Session-scoped Testcontainer
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def postgres_container():
    """
    Start a PostgreSQL 16 container for the entire test session.

    Uses the same PostgreSQL version that most closely matches Redshift's
    PostgreSQL 8.0.2 base (Redshift has evolved significantly, but modern
    PostgreSQL provides the best syntax compatibility).
    """
    with PostgresContainer(
        image="postgres:16-alpine",
        dbname="test_db",
        username="test_user",
        password="test_pass",
    ) as container:
        yield container


@pytest.fixture(scope="session")
def db_connection_params(postgres_container):
    """Extract connection parameters from the running container."""
    return {
        "host": postgres_container.get_container_host_ip(),
        "port": postgres_container.get_exposed_port(5432),
        "dbname": "test_db",
        "user": "test_user",
        "password": "test_pass",
    }


# ---------------------------------------------------------------------------
# Per-test database connection with fresh state
# ---------------------------------------------------------------------------


@pytest.fixture()
def db_conn(db_connection_params):
    """
    Provide a database connection with a clean schema for each test.

    Drops and recreates all schemas, then loads the schema DDL and seed data.
    This ensures each test starts from a known state, similar to how
    @DataJpaTest + schema.sql/data.sql works in the Spring Boot POC.
    """
    conn = psycopg2.connect(**db_connection_params)
    conn.autocommit = True

    try:
        _reset_database(conn)
        _load_schema(conn)
        _load_seed_data(conn)
        yield conn
    finally:
        conn.close()


@pytest.fixture()
def db_conn_empty(db_connection_params):
    """
    Provide a database connection with schemas created but no seed data.

    Useful for tests that need to control exactly what data is present.
    """
    conn = psycopg2.connect(**db_connection_params)
    conn.autocommit = True

    try:
        _reset_database(conn)
        _load_schema(conn)
        yield conn
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Helper: path to sample query files
# ---------------------------------------------------------------------------


@pytest.fixture()
def sample_query_path():
    """Return a function that resolves sample query file paths."""

    def _get_path(filename: str) -> str:
        return os.path.join(SAMPLE_QUERIES_DIR, filename)

    return _get_path


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _reset_database(conn):
    """Drop and recreate all test schemas."""
    with conn.cursor() as cur:
        cur.execute("DROP SCHEMA IF EXISTS source_data CASCADE")
        cur.execute("DROP SCHEMA IF EXISTS target_data CASCADE")
        cur.execute("DROP SCHEMA IF EXISTS recon_data CASCADE")


def _load_schema(conn):
    """Load the schema DDL file."""
    execute_rsql_file(conn, SCHEMA_FILE, skip_compatibility=True)


def _load_seed_data(conn):
    """Load the seed data file."""
    execute_rsql_file(conn, SEED_FILE, skip_compatibility=True)

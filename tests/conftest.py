"""
Test fixtures for the RSQL unit testing POC.

Supports two database backends (mirroring the Java POC's H2 vs Testcontainers pattern):

  pytest                          # Default: PGlite (no Docker required)
  pytest --db-backend=pglite      # Explicit: PGlite (no Docker required)
  pytest --db-backend=testcontainers  # Full fidelity: real PostgreSQL via Docker/Podman

Both backends provide the same fixtures (db_conn, db_conn_empty, sample_query_path)
so all tests run identically regardless of which backend is selected.

NOTE: PGlite only supports a single connection at a time, so the pglite backend
uses one session-scoped connection that is reset between tests rather than
opening/closing connections per test.
"""

import os
from pathlib import Path

import psycopg2
import pytest

from src.rsql_executor import execute_rsql_file


# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))
SQL_DIR = os.path.join(PROJECT_ROOT, "sql")
SAMPLE_QUERIES_DIR = os.path.join(SQL_DIR, "sample_queries")
SCHEMA_FILE = os.path.join(SQL_DIR, "schema.sql")
SEED_FILE = os.path.join(SQL_DIR, "seed_data.sql")

# Pre-installed PGlite Node.js dependencies (see .pglite/package.json).
# Run `npm install` in .pglite/ once after cloning; py-pglite will reuse
# the node_modules from there instead of trying to call npm itself
# (which fails on Windows because npm is npm.cmd).
PGLITE_WORK_DIR = Path(PROJECT_ROOT) / ".pglite"


# ---------------------------------------------------------------------------
# CLI option: --db-backend
# ---------------------------------------------------------------------------


def pytest_addoption(parser):
    parser.addoption(
        "--db-backend",
        action="store",
        default="pglite",
        choices=["pglite", "testcontainers"],
        help="Database backend for tests: 'pglite' (no Docker) or 'testcontainers' (needs Docker/Podman)",
    )


# ---------------------------------------------------------------------------
# Session-scoped database backend + persistent connection
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def _db_session(request):
    """
    Provide a running database backend and a single persistent connection.

    For pglite: one connection is kept open for the entire session because
    PGlite-socket only supports a single connection per server lifetime.

    For testcontainers: a connection is opened once and reused for efficiency,
    though the backend does support multiple connections.

    Yields (conn, backend_context) where backend_context is the context manager
    that must stay open for the database to remain available.
    """
    backend = request.config.getoption("--db-backend")

    if backend == "pglite":
        yield from _pglite_session()
    else:
        yield from _testcontainers_session()


def _pglite_session():
    """Start PGlite and yield a single persistent connection."""
    import socket
    import time

    from py_pglite import PGliteConfig, PGliteManager

    # Find a free port since PGlite doesn't support port 0 auto-assign
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        free_port = s.getsockname()[1]

    config = PGliteConfig(
        use_tcp=True,
        tcp_host="127.0.0.1",
        tcp_port=free_port,
        node_options="--experimental-require-module",
        work_dir=PGLITE_WORK_DIR,
        auto_install_deps=False,
    )

    with PGliteManager(config=config) as manager:
        # Give the server a moment to fully initialise after the TCP port opens
        time.sleep(2)

        conn = psycopg2.connect(
            host=manager.config.tcp_host,
            port=manager.config.tcp_port,
            dbname="postgres",
            user="postgres",
            password="postgres",
            sslmode="disable",
        )
        conn.autocommit = True

        try:
            yield conn
        finally:
            conn.close()


def _testcontainers_session():
    """Start a PostgreSQL container and yield a persistent connection."""
    from testcontainers.postgres import PostgresContainer

    with PostgresContainer(
        image="binaries.avivagroup.com/docker-virtual/postgres:16.8-alpine",
        dbname="test_db",
        username="test_user",
        password="test_pass",
    ) as container:
        conn = psycopg2.connect(
            host=container.get_container_host_ip(),
            port=container.get_exposed_port(5432),
            dbname="test_db",
            user="test_user",
            password="test_pass",
        )
        conn.autocommit = True

        try:
            yield conn
        finally:
            conn.close()


# ---------------------------------------------------------------------------
# Per-test database connection with fresh state
# ---------------------------------------------------------------------------


@pytest.fixture()
def db_conn(_db_session):
    """
    Provide a database connection with a clean schema for each test.

    Drops and recreates all schemas, then loads the schema DDL and seed data.
    This ensures each test starts from a known state, similar to how
    @DataJpaTest + schema.sql/data.sql works in the Spring Boot POC.

    NOTE: With PGlite, this reuses the single session connection (not a new one).
    """
    conn = _db_session
    _reset_database(conn)
    _load_schema(conn)
    _load_seed_data(conn)
    yield conn


@pytest.fixture()
def db_conn_empty(_db_session):
    """
    Provide a database connection with schemas created but no seed data.

    Useful for tests that need to control exactly what data is present.
    """
    conn = _db_session
    _reset_database(conn)
    _load_schema(conn)
    yield conn


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

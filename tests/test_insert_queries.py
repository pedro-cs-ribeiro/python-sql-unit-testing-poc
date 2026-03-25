"""
Tests for TRUNCATE + INSERT .rsql scripts.

These tests verify that the truncate-and-reload pattern correctly
transforms data from source tables into target tables, applying
field-level transformations (DECODE, NVL, TRIM, etc.).
"""

from src.rsql_executor import execute_rsql_file


class TestTruncateInsertQuery:
    """
    Test the TRUNCATE + INSERT pattern.

    The truncate_insert.rsql script clears the target individual table
    and reloads it from source with transformations:
    - Gender coded as M/F/U via DECODE
    - NULLs replaced with defaults via NVL
    - Strings trimmed and empty values converted to NULL
    """

    def test_should_load_all_source_records(
        self, db_conn, sample_query_path
    ):
        """All source records should be loaded into the target table."""
        execute_rsql_file(db_conn, sample_query_path("truncate_insert.rsql"))

        with db_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM target_data.individual")
            count = cur.fetchone()[0]

        assert count == 5

    def test_should_decode_gender_values(
        self, db_conn, sample_query_path
    ):
        """
        Gender values should be transformed via DECODE:
        Male -> M, Female -> F, anything else -> U.
        """
        execute_rsql_file(db_conn, sample_query_path("truncate_insert.rsql"))

        with db_conn.cursor() as cur:
            cur.execute("""
                SELECT customer_id, gender
                FROM target_data.individual
                ORDER BY customer_id
            """)
            rows = cur.fetchall()

        gender_map = {row[0]: row[1] for row in rows}
        assert gender_map["CUST001"] == "M"   # Male -> M
        assert gender_map["CUST002"] == "F"   # Female -> F
        assert gender_map["CUST003"] == "M"   # Male -> M

    def test_should_replace_null_marital_status_with_unknown(
        self, db_conn_empty, sample_query_path
    ):
        """
        NVL(marital_status, 'Unknown') should replace NULL values
        with 'Unknown'.
        """
        # Insert a record with NULL marital status
        with db_conn_empty.cursor() as cur:
            cur.execute("""
                INSERT INTO source_data.individual
                    (uri, customer_id, given_name_one, family_name,
                     date_of_birth, gender, marital_status)
                VALUES
                    ('uri/test/001', 'TEST001', 'Test', 'User',
                     '2000-01-01', 'Male', NULL)
            """)

        execute_rsql_file(
            db_conn_empty, sample_query_path("truncate_insert.rsql")
        )

        with db_conn_empty.cursor() as cur:
            cur.execute("""
                SELECT marital_status
                FROM target_data.individual
                WHERE customer_id = 'TEST001'
            """)
            result = cur.fetchone()[0]

        assert result == "Unknown"

    def test_should_trim_and_nullify_empty_names(
        self, db_conn_empty, sample_query_path
    ):
        """
        NULLIF(TRIM(...), '') should convert whitespace-only names to NULL.
        """
        with db_conn_empty.cursor() as cur:
            cur.execute("""
                INSERT INTO source_data.individual
                    (uri, customer_id, given_name_one, family_name,
                     date_of_birth, gender)
                VALUES
                    ('uri/test/002', 'TEST002', '   ', 'Valid',
                     '2000-01-01', 'Male')
            """)

        execute_rsql_file(
            db_conn_empty, sample_query_path("truncate_insert.rsql")
        )

        with db_conn_empty.cursor() as cur:
            cur.execute("""
                SELECT given_name_one, family_name
                FROM target_data.individual
                WHERE customer_id = 'TEST002'
            """)
            row = cur.fetchone()

        assert row[0] is None   # Whitespace-only name -> NULL
        assert row[1] == "Valid"  # Normal name preserved

    def test_should_clear_existing_data_before_insert(
        self, db_conn, sample_query_path
    ):
        """
        TRUNCATE should clear existing target data before inserting,
        ensuring idempotent behaviour.
        """
        # Pre-populate target with some data
        with db_conn.cursor() as cur:
            cur.execute("""
                INSERT INTO target_data.individual
                    (uri, customer_id, given_name_one, family_name)
                VALUES ('uri/old/001', 'OLD001', 'Old', 'Record')
            """)

        execute_rsql_file(db_conn, sample_query_path("truncate_insert.rsql"))

        with db_conn.cursor() as cur:
            # Old record should be gone
            cur.execute("""
                SELECT COUNT(*) FROM target_data.individual
                WHERE customer_id = 'OLD001'
            """)
            old_count = cur.fetchone()[0]

            # Only source records should exist
            cur.execute("SELECT COUNT(*) FROM target_data.individual")
            total_count = cur.fetchone()[0]

        assert old_count == 0
        assert total_count == 5

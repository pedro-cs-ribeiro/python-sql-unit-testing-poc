"""
Tests for CTAS (CREATE TABLE AS SELECT) .rsql scripts.

These tests verify that reconciliation queries correctly identify mismatches
between source and target data, creating result tables with the expected
rows and match status flags.
"""

from src.rsql_executor import execute_rsql_file


class TestCTASReconQuery:
    """
    Test the CTAS reconciliation pattern.

    The ctas_recon.rsql script performs a FULL OUTER JOIN between source
    and target individual tables, comparing fields and flagging mismatches.
    Only mismatched rows are kept in the result table.
    """

    def test_should_detect_mismatches_when_target_is_empty(
        self, db_conn, sample_query_path
    ):
        """
        When target_data.individual is empty, all source records should appear
        as mismatches (they exist in source but not target).
        """
        execute_rsql_file(db_conn, sample_query_path("ctas_recon.rsql"))

        with db_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM recon_data.customer_recon")
            count = cur.fetchone()[0]

        # All 5 source individuals should be flagged as mismatches
        # since target table is empty
        assert count == 5

    def test_should_detect_no_mismatches_when_data_matches(
        self, db_conn, sample_query_path
    ):
        """
        When target data matches source data exactly, the recon table
        should be empty (no mismatches).
        """
        # Pre-load target with identical data from source
        with db_conn.cursor() as cur:
            cur.execute("""
                INSERT INTO target_data.individual
                    (uri, customer_id, given_name_one, family_name,
                     date_of_birth, gender, marital_status, citizenship,
                     deceased_date, last_updated_date, last_updated_user)
                SELECT uri, customer_id, given_name_one, family_name,
                       date_of_birth, gender, marital_status, citizenship,
                       deceased_date, last_updated_date, last_updated_user
                FROM source_data.individual
            """)

        execute_rsql_file(db_conn, sample_query_path("ctas_recon.rsql"))

        with db_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM recon_data.customer_recon")
            count = cur.fetchone()[0]

        assert count == 0

    def test_should_detect_specific_field_mismatches(
        self, db_conn, sample_query_path
    ):
        """
        When target data has specific differences, the recon table should
        flag exactly those mismatches.
        """
        # Insert target data with one record having a different gender
        with db_conn.cursor() as cur:
            cur.execute("""
                INSERT INTO target_data.individual
                    (uri, customer_id, given_name_one, family_name,
                     date_of_birth, gender)
                SELECT uri, customer_id, given_name_one, family_name,
                       date_of_birth, gender
                FROM source_data.individual
            """)
            # Modify one record to create a mismatch
            cur.execute("""
                UPDATE target_data.individual
                SET gender = 'Other'
                WHERE customer_id = 'CUST001'
            """)

        execute_rsql_file(db_conn, sample_query_path("ctas_recon.rsql"))

        with db_conn.cursor() as cur:
            cur.execute("""
                SELECT customer_id, gender_match_status, match_status
                FROM recon_data.customer_recon
                WHERE customer_id = 'CUST001'
            """)
            row = cur.fetchone()

        assert row is not None
        assert row[1] == 1  # gender_match_status = 1 (mismatch)
        assert row[2] == 1  # match_status = 1 (total mismatches)

    def test_recon_table_is_recreated_on_each_run(
        self, db_conn, sample_query_path
    ):
        """
        The CTAS script should DROP the existing recon table and recreate it,
        ensuring idempotent execution.
        """
        # Run once with empty target (5 mismatches)
        execute_rsql_file(db_conn, sample_query_path("ctas_recon.rsql"))

        with db_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM recon_data.customer_recon")
            first_count = cur.fetchone()[0]

        # Load matching data into target
        with db_conn.cursor() as cur:
            cur.execute("""
                INSERT INTO target_data.individual
                    (uri, customer_id, given_name_one, family_name,
                     date_of_birth, gender)
                SELECT uri, customer_id, given_name_one, family_name,
                       date_of_birth, gender
                FROM source_data.individual
            """)

        # Run again (0 mismatches now)
        execute_rsql_file(db_conn, sample_query_path("ctas_recon.rsql"))

        with db_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM recon_data.customer_recon")
            second_count = cur.fetchone()[0]

        assert first_count == 5
        assert second_count == 0

"""
Tests for stored procedure .rsql scripts.

These tests verify that PL/pgSQL procedures are created and executed
correctly, producing expected reconciliation results.
"""

from src.rsql_executor import execute_rsql_file


class TestStoredProcedures:
    """
    Test the stored procedure creation and CALL patterns.

    The stored_procedure.rsql script creates a procedure that dynamically
    builds and executes SQL to reconcile reference data between two source
    systems. The call_procedures.rsql script invokes it for multiple
    lookup types.
    """

    def test_should_create_procedure_successfully(
        self, db_conn, sample_query_path
    ):
        """The procedure should be created without errors."""
        execute_rsql_file(
            db_conn, sample_query_path("stored_procedure.rsql")
        )

        with db_conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*)
                FROM information_schema.routines
                WHERE routine_schema = 'recon_data'
                  AND routine_name = 'reconcile_reference_data'
            """)
            count = cur.fetchone()[0]

        assert count == 1

    def test_should_detect_missing_reference_data(
        self, db_conn, sample_query_path
    ):
        """
        When reference data exists in one system but not the other,
        the recon should flag it as MISSING_IN_A or MISSING_IN_B.
        """
        # Create the procedure
        execute_rsql_file(
            db_conn, sample_query_path("stored_procedure.rsql")
        )

        # Call it for MARITAL_STATUS (MDM has MAR/SNG/DIV, RELTIO has MAR/SNG/SEP)
        # SEP is inactive in RELTIO so it won't appear
        execute_rsql_file(
            db_conn, sample_query_path("call_procedures.rsql")
        )

        with db_conn.cursor() as cur:
            cur.execute("""
                SELECT lookup_code, recon_status
                FROM recon_data.recon_marital_status
                ORDER BY lookup_code
            """)
            rows = cur.fetchall()

        recon_map = {row[0]: row[1] for row in rows}

        # DIV exists in MDM but not in active RELTIO data -> MISSING_IN_B
        assert recon_map.get("DIV") == "MISSING_IN_B"

    def test_should_detect_matching_reference_data(
        self, db_conn, sample_query_path
    ):
        """
        When reference data matches between systems, it should not
        appear in the recon output (only mismatches are kept).
        """
        execute_rsql_file(
            db_conn, sample_query_path("stored_procedure.rsql")
        )
        execute_rsql_file(
            db_conn, sample_query_path("call_procedures.rsql")
        )

        with db_conn.cursor() as cur:
            # GENDER: M/F exist in both MDM and RELTIO with same values
            cur.execute("""
                SELECT COUNT(*)
                FROM recon_data.recon_gender
            """)
            count = cur.fetchone()[0]

        # Both M and F match, so recon table should be empty
        assert count == 0

    def test_should_create_recon_tables_per_lookup_type(
        self, db_conn, sample_query_path
    ):
        """
        Each CALL should create a separate recon table named after
        the lookup type.
        """
        execute_rsql_file(
            db_conn, sample_query_path("stored_procedure.rsql")
        )
        execute_rsql_file(
            db_conn, sample_query_path("call_procedures.rsql")
        )

        with db_conn.cursor() as cur:
            cur.execute("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'recon_data'
                  AND table_name LIKE 'recon_%'
                ORDER BY table_name
            """)
            tables = [row[0] for row in cur.fetchall()]

        assert "recon_gender" in tables
        assert "recon_marital_status" in tables

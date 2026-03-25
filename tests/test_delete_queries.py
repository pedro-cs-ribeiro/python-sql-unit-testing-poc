"""
Tests for DELETE .rsql scripts.

These tests verify that delete scripts correctly remove records from
target tables based on existence checks against source tables.
"""

from src.rsql_executor import execute_rsql_file


class TestDeleteQuery:
    """
    Test the DELETE ... WHERE EXISTS pattern.

    The delete_existing.rsql script removes records from target_data.individual
    where a matching customer_id exists in source_data.individual and the
    source record's scm_createdtime is before CURRENT_DATE.
    """

    def test_should_delete_matching_records(
        self, db_conn, sample_query_path
    ):
        """
        Records in target that match source (by customer_id) should be deleted
        when the source scm_createdtime is before today.
        """
        # Pre-populate target with records that match source
        with db_conn.cursor() as cur:
            cur.execute("""
                INSERT INTO target_data.individual
                    (uri, customer_id, given_name_one, family_name)
                VALUES
                    ('uri/tgt/001', 'CUST001', 'John', 'Smith'),
                    ('uri/tgt/002', 'CUST002', 'Jane', 'Doe')
            """)

        execute_rsql_file(db_conn, sample_query_path("delete_existing.rsql"))

        with db_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM target_data.individual")
            count = cur.fetchone()[0]

        # Both should be deleted (source scm_createdtime defaults to
        # CURRENT_TIMESTAMP which is before CURRENT_DATE's end)
        assert count == 0

    def test_should_not_delete_records_without_source_match(
        self, db_conn, sample_query_path
    ):
        """
        Records in target that have no matching customer_id in source
        should not be deleted.
        """
        with db_conn.cursor() as cur:
            cur.execute("""
                INSERT INTO target_data.individual
                    (uri, customer_id, given_name_one, family_name)
                VALUES
                    ('uri/tgt/099', 'NOMATCH', 'No', 'Match')
            """)

        execute_rsql_file(db_conn, sample_query_path("delete_existing.rsql"))

        with db_conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) FROM target_data.individual
                WHERE customer_id = 'NOMATCH'
            """)
            count = cur.fetchone()[0]

        assert count == 1

    def test_should_handle_empty_target_table(
        self, db_conn, sample_query_path
    ):
        """
        Running delete against an empty target table should not raise errors.
        """
        execute_rsql_file(db_conn, sample_query_path("delete_existing.rsql"))

        with db_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM target_data.individual")
            count = cur.fetchone()[0]

        assert count == 0

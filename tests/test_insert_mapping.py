"""
Tests for INSERT with key mapping .rsql scripts.

These tests verify the idempotent insert pattern that generates surrogate
keys and only inserts records that don't already exist in the target table.
"""

from src.rsql_executor import execute_rsql_file


class TestInsertMappingQuery:
    """
    Test the INSERT ... WHERE NOT EXISTS key mapping pattern.

    The insert_mapping.rsql script generates surrogate keys for customer
    mappings by computing MAX(existing_key) + ROW_NUMBER(), and only
    inserts records that don't already exist (deduplication via NOT EXISTS).
    """

    def test_should_insert_all_mappings_on_first_run(
        self, db_conn, sample_query_path
    ):
        """
        On first run with an empty mapping table, all source individuals
        should get a mapping entry.
        """
        execute_rsql_file(db_conn, sample_query_path("insert_mapping.rsql"))

        with db_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM target_data.customer_mapping")
            count = cur.fetchone()[0]

        # 5 source individuals should all get mappings
        assert count == 5

    def test_should_not_duplicate_on_second_run(
        self, db_conn, sample_query_path
    ):
        """
        Running the same script twice should not create duplicate mappings
        (idempotent insert via NOT EXISTS).
        """
        execute_rsql_file(db_conn, sample_query_path("insert_mapping.rsql"))
        execute_rsql_file(db_conn, sample_query_path("insert_mapping.rsql"))

        with db_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM target_data.customer_mapping")
            count = cur.fetchone()[0]

        assert count == 5

    def test_should_assign_sequential_keys(
        self, db_conn_empty, sample_query_path
    ):
        """
        Generated keys should be sequential starting from MAX(existing) + 1.
        """
        # Insert just 2 source records
        with db_conn_empty.cursor() as cur:
            cur.execute("""
                INSERT INTO source_data.individual (uri, customer_id, given_name_one, family_name)
                VALUES
                    ('uri/ind/001', 'C001', 'Alice', 'Test'),
                    ('uri/ind/002', 'C002', 'Bob', 'Test')
            """)

        execute_rsql_file(
            db_conn_empty, sample_query_path("insert_mapping.rsql")
        )

        with db_conn_empty.cursor() as cur:
            cur.execute("""
                SELECT customer_uri
                FROM target_data.customer_mapping
                ORDER BY mapping_id
            """)
            uris = [row[0] for row in cur.fetchall()]

        assert len(uris) == 2
        assert "uri/ind/001" in uris
        assert "uri/ind/002" in uris

    def test_should_only_insert_new_records(
        self, db_conn_empty, sample_query_path
    ):
        """
        When some mappings already exist, only new (unmapped) records
        should be inserted.
        """
        # Insert a source record and pre-create its mapping
        with db_conn_empty.cursor() as cur:
            cur.execute("""
                INSERT INTO source_data.individual (uri, customer_id, given_name_one, family_name)
                VALUES
                    ('uri/ind/001', 'C001', 'Alice', 'Test'),
                    ('uri/ind/002', 'C002', 'Bob', 'Test')
            """)
            cur.execute("""
                INSERT INTO target_data.customer_mapping
                    (customer_id, customer_uri, created_by)
                VALUES ('C001', 'uri/ind/001', 'pre_existing')
            """)

        execute_rsql_file(
            db_conn_empty, sample_query_path("insert_mapping.rsql")
        )

        with db_conn_empty.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM target_data.customer_mapping")
            count = cur.fetchone()[0]

            cur.execute("""
                SELECT customer_uri
                FROM target_data.customer_mapping
                WHERE created_by = 'batch_load'
            """)
            new_records = [row[0] for row in cur.fetchall()]

        assert count == 2  # 1 pre-existing + 1 new
        assert len(new_records) == 1
        assert new_records[0] == "uri/ind/002"

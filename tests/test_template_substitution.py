"""
Tests for template substitution logic.

These tests verify that the pipeline's template variable replacement
(@VAR@ and $VAR patterns) works correctly, and that the resulting
SQL executes as expected against the database.
"""

from src.rsql_executor import execute_rsql_file
from redshift_compat import apply_template_substitution


class TestTemplateSubstitution:
    """
    Test the template substitution mechanism used in production pipelines.

    Production pipelines replace placeholders like @PREV_DATE@ and $S3PATH
    before executing .rsql files. This tests both the substitution logic
    and the execution of the resulting SQL.
    """

    def test_should_replace_at_style_placeholders(self):
        """@VARIABLE@ style placeholders should be replaced."""
        sql = "SELECT * FROM table WHERE date = '@PREV_DATE@'"
        result = apply_template_substitution(
            sql, {"PREV_DATE": "2024-01-15"}
        )
        assert result == "SELECT * FROM table WHERE date = '2024-01-15'"

    def test_should_replace_dollar_style_placeholders(self):
        """$VARIABLE style placeholders should be replaced."""
        sql = "UNLOAD ('SELECT 1') TO '$S3PATH/output/' iam_role 'arn:aws:iam::$ACCNUMBER:role/test'"
        result = apply_template_substitution(
            sql, {"S3PATH": "s3://my-bucket", "ACCNUMBER": "123456789"}
        )
        assert "s3://my-bucket/output/" in result
        assert "123456789" in result

    def test_should_handle_multiple_occurrences(self):
        """All occurrences of the same placeholder should be replaced."""
        sql = """
            INSERT INTO t1 SELECT '@PREV_DATE@'::DATE;
            INSERT INTO t2 SELECT '@PREV_DATE@'::DATE;
        """
        result = apply_template_substitution(
            sql, {"PREV_DATE": "2024-01-15"}
        )
        assert result.count("2024-01-15") == 2
        assert "@PREV_DATE@" not in result

    def test_should_leave_unknown_placeholders_unchanged(self):
        """Placeholders not in the variables dict should remain as-is."""
        sql = "SELECT '@UNKNOWN@' AS col"
        result = apply_template_substitution(sql, {"PREV_DATE": "2024-01-15"})
        assert "@UNKNOWN@" in result

    def test_should_execute_date_template_query(
        self, db_conn, sample_query_path
    ):
        """
        The template_date_query.rsql should execute correctly after
        @PREV_DATE@ substitution and produce aggregated counts.
        """
        # Create the daily_stats table
        execute_rsql_file(
            db_conn,
            sample_query_path("daily_stats_ddl.rsql"),
            skip_compatibility=True,
        )

        # Execute with a date that includes all seed data
        execute_rsql_file(
            db_conn,
            sample_query_path("template_date_query.rsql"),
            template_vars={"PREV_DATE": "2099-12-31"},
        )

        with db_conn.cursor() as cur:
            cur.execute("""
                SELECT entity_type, record_count
                FROM recon_data.daily_stats
                ORDER BY entity_type
            """)
            rows = cur.fetchall()

        stats = {row[0]: row[1] for row in rows}

        assert stats["contract"] == 7   # 7 contracts in seed data
        assert stats["individual"] == 5  # 5 individuals in seed data

    def test_should_filter_by_template_date(
        self, db_conn, sample_query_path
    ):
        """
        Records with scm_createdtime after @PREV_DATE@ should be excluded.
        """
        # Create the daily_stats table
        execute_rsql_file(
            db_conn,
            sample_query_path("daily_stats_ddl.rsql"),
            skip_compatibility=True,
        )

        # Use a date far in the past so nothing qualifies
        execute_rsql_file(
            db_conn,
            sample_query_path("template_date_query.rsql"),
            template_vars={"PREV_DATE": "1900-01-01"},
        )

        with db_conn.cursor() as cur:
            cur.execute("""
                SELECT entity_type, record_count
                FROM recon_data.daily_stats
                ORDER BY entity_type
            """)
            rows = cur.fetchall()

        stats = {row[0]: row[1] for row in rows}

        assert stats["contract"] == 0
        assert stats["individual"] == 0

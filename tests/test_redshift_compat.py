"""
Tests for the Redshift compatibility layer.

These tests verify that Redshift-specific SQL syntax is correctly
transformed into PostgreSQL-compatible SQL, ensuring .rsql files
can be executed against a standard PostgreSQL test database.
"""

from redshift_compat import convert_redshift_to_postgres


class TestDistKeyRemoval:
    """Test removal of DISTKEY clauses."""

    def test_should_remove_distkey_in_create_table(self):
        sql = "CREATE TABLE t (id INT PRIMARY KEY DISTKEY, name VARCHAR(50))"
        result = convert_redshift_to_postgres(sql)
        assert "DISTKEY" not in result
        assert "PRIMARY KEY" in result

    def test_should_remove_distkey_with_column_reference(self):
        sql = "CREATE TABLE t (id INT) DISTKEY(id)"
        result = convert_redshift_to_postgres(sql)
        assert "DISTKEY" not in result


class TestSortKeyRemoval:
    """Test removal of SORTKEY clauses."""

    def test_should_remove_sortkey(self):
        sql = "CREATE TABLE t (id INT) SORTKEY(id)"
        result = convert_redshift_to_postgres(sql)
        assert "SORTKEY" not in result

    def test_should_remove_compound_sortkey(self):
        sql = "CREATE TABLE t (id INT) COMPOUND SORTKEY(id, name)"
        result = convert_redshift_to_postgres(sql)
        assert "SORTKEY" not in result
        assert "COMPOUND" not in result

    def test_should_remove_interleaved_sortkey(self):
        sql = "CREATE TABLE t (id INT) INTERLEAVED SORTKEY(id, name)"
        result = convert_redshift_to_postgres(sql)
        assert "SORTKEY" not in result
        assert "INTERLEAVED" not in result


class TestDistStyleRemoval:
    """Test removal of DISTSTYLE clauses."""

    def test_should_remove_diststyle_key(self):
        sql = "CREATE TABLE t (id INT) DISTSTYLE KEY"
        result = convert_redshift_to_postgres(sql)
        assert "DISTSTYLE" not in result

    def test_should_remove_diststyle_all(self):
        sql = "CREATE TABLE t (id INT) DISTSTYLE ALL"
        result = convert_redshift_to_postgres(sql)
        assert "DISTSTYLE" not in result

    def test_should_remove_diststyle_even(self):
        sql = "CREATE TABLE t (id INT) DISTSTYLE EVEN"
        result = convert_redshift_to_postgres(sql)
        assert "DISTSTYLE" not in result


class TestEncodeRemoval:
    """Test removal of ENCODE compression clauses."""

    def test_should_remove_encode_lzo(self):
        sql = "CREATE TABLE t (name VARCHAR(100) ENCODE LZO)"
        result = convert_redshift_to_postgres(sql)
        assert "ENCODE" not in result
        assert "LZO" not in result
        assert "VARCHAR(100)" in result

    def test_should_remove_encode_zstd(self):
        sql = "CREATE TABLE t (name VARCHAR(100) ENCODE ZSTD)"
        result = convert_redshift_to_postgres(sql)
        assert "ENCODE" not in result

    def test_should_remove_encode_raw(self):
        sql = "CREATE TABLE t (id INT ENCODE RAW)"
        result = convert_redshift_to_postgres(sql)
        assert "ENCODE" not in result


class TestNVLReplacement:
    """Test NVL -> COALESCE replacement."""

    def test_should_replace_nvl_with_coalesce(self):
        sql = "SELECT NVL(name, 'default') FROM t"
        result = convert_redshift_to_postgres(sql)
        assert "COALESCE(name, 'default')" in result
        assert "NVL" not in result

    def test_should_handle_nested_nvl(self):
        sql = "SELECT NVL(NVL(a, b), c) FROM t"
        result = convert_redshift_to_postgres(sql)
        assert "COALESCE(COALESCE(a, b), c)" in result


class TestSysdateReplacement:
    """Test SYSDATE -> CURRENT_TIMESTAMP replacement."""

    def test_should_replace_sysdate(self):
        sql = "SELECT SYSDATE AS now"
        result = convert_redshift_to_postgres(sql)
        assert "CURRENT_TIMESTAMP" in result
        assert "SYSDATE" not in result


class TestGetdateReplacement:
    """Test GETDATE() -> CURRENT_TIMESTAMP replacement."""

    def test_should_replace_getdate(self):
        sql = "SELECT GETDATE() AS now"
        result = convert_redshift_to_postgres(sql)
        assert "CURRENT_TIMESTAMP" in result
        assert "GETDATE" not in result


class TestCharindexReplacement:
    """Test CHARINDEX -> POSITION replacement."""

    def test_should_replace_charindex(self):
        sql = "SELECT CHARINDEX('abc', column1) FROM t"
        result = convert_redshift_to_postgres(sql)
        assert "POSITION('abc' IN column1)" in result
        assert "CHARINDEX" not in result


class TestDecodeReplacement:
    """Test DECODE -> CASE WHEN replacement."""

    def test_should_replace_simple_decode(self):
        sql = "SELECT DECODE(gender, 'M', 'Male', 'F', 'Female', 'Unknown') AS gender_desc FROM t"
        result = convert_redshift_to_postgres(sql)
        assert "CASE" in result
        assert "WHEN gender = 'M' THEN 'Male'" in result
        assert "WHEN gender = 'F' THEN 'Female'" in result
        assert "ELSE 'Unknown'" in result
        assert "END" in result


class TestAnalyzeRemoval:
    """Test removal of ANALYZE statements."""

    def test_should_remove_analyze(self):
        sql = "INSERT INTO t SELECT 1;\nANALYZE t;\n"
        result = convert_redshift_to_postgres(sql)
        assert "ANALYZE" not in result
        assert "INSERT INTO t SELECT 1" in result


class TestCopyRemoval:
    """Test removal of COPY commands."""

    def test_should_remove_copy_from_s3(self):
        sql = "COPY t FROM 's3://bucket/path' iam_role 'arn:aws:iam::123:role/test' FORMAT CSV;"
        result = convert_redshift_to_postgres(sql)
        assert "COPY" not in result.strip()


class TestUnloadRemoval:
    """Test removal of UNLOAD commands."""

    def test_should_remove_unload(self):
        sql = "UNLOAD ('SELECT * FROM t') TO 's3://bucket/output/' iam_role 'arn:aws:iam::123:role/test';"
        result = convert_redshift_to_postgres(sql)
        assert "UNLOAD" not in result.strip()


class TestCombinedTransformations:
    """Test multiple transformations applied together."""

    def test_should_handle_complex_redshift_sql(self):
        sql = """
            CREATE TABLE result
            DISTKEY(id)
            SORTKEY(id, name)
            AS
            SELECT
                id,
                NVL(name, 'unknown') AS name,
                DECODE(status, 'A', 'Active', 'I', 'Inactive', 'Unknown') AS status_desc,
                SYSDATE AS created_date
            FROM source;
            ANALYZE result;
        """
        result = convert_redshift_to_postgres(sql)

        assert "DISTKEY" not in result
        assert "SORTKEY" not in result
        assert "COALESCE(name, 'unknown')" in result
        assert "CASE" in result
        assert "CURRENT_TIMESTAMP" in result
        assert "ANALYZE" not in result
        # Core SQL structure preserved
        assert "CREATE TABLE result" in result
        assert "SELECT" in result
        assert "FROM source" in result

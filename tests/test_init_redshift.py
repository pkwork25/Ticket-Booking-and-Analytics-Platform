from pathlib import Path

from init_redshift import SQL_FILES, split_sql


def test_split_sql_handles_comments_and_quotes():
    sql = "-- comment; here\nCREATE TABLE a (x int);\nSELECT 'a;b' AS v;\n"
    assert split_sql(sql) == ["CREATE TABLE a (x int)", "SELECT 'a;b' AS v"]


def test_ddl_files_split_into_expected_statements():
    stmts = [s for f in SQL_FILES for s in split_sql(Path(f).read_text())]
    assert sum(s.startswith("CREATE OR REPLACE VIEW") for s in stmts) == 5
    assert sum(s.startswith("CREATE TABLE") for s in stmts) == 8

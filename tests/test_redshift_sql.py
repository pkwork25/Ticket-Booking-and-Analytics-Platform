import re
from pathlib import Path

from ticket_etl import redshift_sql as R
from ticket_etl import schema as S

DDL = (Path(__file__).resolve().parents[1] / "redshift" / "01_tables.sql").read_text()


def ddl_columns(table):
    m = re.search(rf"CREATE TABLE IF NOT EXISTS tp\.{table} \((.*?)\)\s*\nDISTKEY", DDL, re.S)
    assert m, table
    cols = []
    for line in m.group(1).splitlines():
        line = line.strip()
        if line and not line.startswith("PRIMARY KEY"):
            cols.append(line.split()[0])
    return cols


def test_python_column_lists_match_ddl():
    assert ddl_columns("booking_events") == S.BOOKING_EVENTS_COLUMNS
    assert ddl_columns("payment_events") == S.PAYMENT_EVENTS_COLUMNS
    assert ddl_columns("bookings_current") == S.BOOKINGS_CURRENT_COLUMNS
    assert ddl_columns("payments_current") == S.PAYMENTS_CURRENT_COLUMNS


def test_postactions_are_single_transactions_without_truncate():
    for sql in (
        R.append_dedup_postactions("booking_events", S.BOOKING_EVENTS_COLUMNS),
        R.upsert_latest_postactions("bookings_current", "booking_id", S.BOOKINGS_CURRENT_COLUMNS),
    ):
        assert sql.startswith("BEGIN;") and sql.endswith("END;")
        assert "TRUNCATE" not in sql  # TRUNCATE commits implicitly in Redshift


def test_upsert_is_version_guarded():
    sql = R.upsert_latest_postactions("payments_current", "payment_id", S.PAYMENTS_CURRENT_COLUMNS)
    assert "s.last_event_time >= tp.payments_current.last_event_time" in sql
    assert "WHERE t.payment_id IS NULL" in sql


def test_preactions_create_and_truncate_stage():
    sql = R.preactions("payment_events")
    assert "CREATE TABLE IF NOT EXISTS tp_stage.payment_events (LIKE tp.payment_events)" in sql
    assert sql.rstrip(";").endswith("TRUNCATE tp_stage.payment_events")

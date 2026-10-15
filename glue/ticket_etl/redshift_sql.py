"""SQL the Glue job runs around each Redshift COPY.

Pattern: Glue COPYs the micro-batch into a staging table (``preactions`` create/truncate it),
then ``postactions`` apply it to the target inside one transaction:

* event tables  -> insert-only anti-join on (event_id, event_time)  => idempotent on replays
* current state -> version-guarded delete + insert on the business key => late or replayed
                   events can never overwrite a newer state

No TRUNCATE inside BEGIN/END: in Redshift TRUNCATE commits implicitly, which would break the
transaction. The stage table is truncated by the next batch's preactions instead.
"""

from __future__ import annotations

TARGET_SCHEMA = "tp"
STAGE_SCHEMA = "tp_stage"


def _stage(table: str) -> str:
    return f"{STAGE_SCHEMA}.{table}"


def _target(table: str) -> str:
    return f"{TARGET_SCHEMA}.{table}"


def preactions(table: str) -> str:
    return ";".join(
        [
            f"CREATE SCHEMA IF NOT EXISTS {STAGE_SCHEMA}",
            f"CREATE TABLE IF NOT EXISTS {_stage(table)} (LIKE {_target(table)})",
            f"TRUNCATE {_stage(table)}",
        ]
    ) + ";"


def append_dedup_postactions(table: str, columns: list[str]) -> str:
    """Insert events that are not already in the target (exactly-once effect)."""
    cols = ", ".join(columns)
    s_cols = ", ".join(f"s.{c}" for c in columns)
    return (
        "BEGIN;"
        f"INSERT INTO {_target(table)} ({cols}) "
        f"SELECT {s_cols} FROM {_stage(table)} s "
        f"LEFT JOIN {_target(table)} t ON t.event_id = s.event_id AND t.event_time = s.event_time "
        "WHERE t.event_id IS NULL;"
        "END;"
    )


def upsert_latest_postactions(table: str, key: str, columns: list[str]) -> str:
    """Upsert current state, guarded by last_event_time (newer-or-equal wins)."""
    cols = ", ".join(columns)
    s_cols = ", ".join(f"s.{c}" for c in columns)
    tgt = _target(table)
    stg = _stage(table)
    return (
        "BEGIN;"
        f"DELETE FROM {tgt} USING {stg} s "
        f"WHERE {tgt}.{key} = s.{key} AND s.last_event_time >= {tgt}.last_event_time;"
        f"INSERT INTO {tgt} ({cols}) "
        f"SELECT {s_cols} FROM {stg} s LEFT JOIN {tgt} t ON t.{key} = s.{key} "
        f"WHERE t.{key} IS NULL;"
        "END;"
    )

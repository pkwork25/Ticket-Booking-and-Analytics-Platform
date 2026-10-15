"""Spark transformations for one micro-batch (pure DataFrame -> DataFrame functions).

Kept free of awsglue imports so they run under plain PySpark in unit tests / CI.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from . import schema as S

# Tie-breaker when two events for the same key share an event_time: the later lifecycle step wins.
_BOOKING_RANK = {"booking_created": 1, "booking_expired": 2, "booking_confirmed": 3, "booking_cancelled": 4}
_PAYMENT_RANK = {"payment_initiated": 1, "payment_failed": 2, "payment_succeeded": 3, "refund_processed": 4}


def _rank_expr(mapping: dict[str, int]) -> Column:
    expr = F.lit(0)
    for etype, rank in mapping.items():
        expr = F.when(F.col("event_type") == etype, F.lit(rank)).otherwise(expr)
    return expr


def normalize(df: DataFrame) -> DataFrame:
    """Typed timestamps, trimmed / upper-cased enums, ingest time."""
    return (
        df.withColumn("event_time_raw", F.col("event_time"))
        .withColumn("event_time", F.to_timestamp("event_time"))
        .withColumn("show_time", F.to_timestamp("show_time"))
        .withColumn("ingest_time", F.current_timestamp())
        .withColumn("event_type", F.lower(F.trim("event_type")))
        .withColumn("payment_method", F.upper(F.trim("payment_method")))
        .withColumn("gateway", F.lower(F.trim("gateway")))
        .withColumn("currency", F.coalesce(F.upper("currency"), F.lit("INR")))
    )


def with_validation(df: DataFrame) -> DataFrame:
    """Add ``dq_errors`` (array<string>) mirroring ``schema.validate_event``."""
    is_booking = F.col("event_type").isin(*S.BOOKING_EVENT_TYPES)
    is_payment = F.col("event_type").isin(*S.PAYMENT_EVENT_TYPES)

    def blank(c: str) -> Column:
        return F.col(c).isNull() | (F.trim(F.col(c)) == "")

    checks = [
        ("missing_event_id", blank("event_id")),
        ("unknown_event_type", ~F.col("event_type").isin(*S.EVENT_TYPES) | F.col("event_type").isNull()),
        ("bad_event_time", F.col("event_time").isNull()),
        ("missing_booking_id", blank("booking_id")),
        ("bad_booking_status", is_booking & ~F.coalesce(F.col("booking_status").isin(*S.BOOKING_STATUSES), F.lit(False))),
        ("bad_seats", is_booking & ~F.coalesce(F.col("seats").between(1, 20), F.lit(False))),
        ("bad_ticket_amount", is_booking & ~F.coalesce(F.col("ticket_amount") >= 0, F.lit(False))),
        ("missing_payment_id", is_payment & blank("payment_id")),
        ("bad_payment_status", is_payment & ~F.coalesce(F.col("payment_status").isin(*S.PAYMENT_STATUSES), F.lit(False))),
        ("bad_payment_method", is_payment & ~F.coalesce(F.col("payment_method").isin(*S.PAYMENT_METHODS), F.lit(False))),
        ("bad_amount", is_payment & ~F.coalesce(F.col("amount") > 0, F.lit(False))),
    ]  # fmt: skip
    errors = F.array(*[F.when(cond, F.lit(name)) for name, cond in checks])
    return df.withColumn("dq_errors", F.filter(errors, lambda x: x.isNotNull()))


def split_valid(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    valid = df.filter(F.size("dq_errors") == 0).drop("dq_errors")
    rejects = df.filter(F.size("dq_errors") > 0)
    return valid, rejects


def dedupe_events(df: DataFrame) -> DataFrame:
    """At-least-once producers + Kinesis retries => duplicates. Keep one row per event_id."""
    return df.dropDuplicates(["event_id"])


def booking_events(df: DataFrame) -> DataFrame:
    return df.filter(F.col("event_type").isin(*S.BOOKING_EVENT_TYPES)).select(*S.BOOKING_EVENTS_COLUMNS)


def payment_events(df: DataFrame) -> DataFrame:
    return df.filter(F.col("event_type").isin(*S.PAYMENT_EVENT_TYPES)).select(*S.PAYMENT_EVENTS_COLUMNS)


def _latest(df: DataFrame, key: str, rank: dict[str, int]) -> DataFrame:
    w = Window.partitionBy(key).orderBy(
        F.col("event_time").desc(), _rank_expr(rank).desc(), F.col("event_id").desc()
    )
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def bookings_current(booking_df: DataFrame) -> DataFrame:
    latest = _latest(booking_df, "booking_id", _BOOKING_RANK)
    return latest.select(
        "booking_id", "user_id", "event_name", "event_category", "venue_city", "show_time",
        "seats", "ticket_amount", "booking_status",
        F.col("event_id").alias("last_event_id"),
        F.col("event_type").alias("last_event_type"),
        F.col("event_time").alias("last_event_time"),
        F.col("ingest_time").alias("updated_at"),
    )  # fmt: skip


def payments_current(payment_df: DataFrame) -> DataFrame:
    latest = _latest(payment_df, "payment_id", _PAYMENT_RANK)
    return latest.select(
        "payment_id", "booking_id", "user_id", "payment_method", "gateway", "payment_status",
        "amount", "currency", "failure_code", "refund_amount",
        F.col("event_id").alias("last_event_id"),
        F.col("event_type").alias("last_event_type"),
        F.col("event_time").alias("last_event_time"),
        F.col("ingest_time").alias("updated_at"),
    )  # fmt: skip


def payment_outcome_stats(payment_df: DataFrame, dimension: str) -> DataFrame:
    """Attempts / failures per dimension over terminal outcomes in this batch."""
    terminal = payment_df.filter(F.col("event_type").isin("payment_succeeded", "payment_failed"))
    failed = F.col("event_type") == "payment_failed"
    agg = terminal.groupBy(dimension).agg(
        F.count("*").alias("attempts"),
        F.sum(F.when(failed, 1).otherwise(0)).alias("failures"),
        F.sum(F.when(failed, F.col("amount")).otherwise(0.0)).alias("failed_amount"),
    )
    top_code = (
        terminal.filter(failed)
        .groupBy(dimension, "failure_code")
        .count()
        .withColumn(
            "_rn",
            F.row_number().over(Window.partitionBy(dimension).orderBy(F.col("count").desc(), "failure_code")),
        )
        .filter("_rn = 1")
        .select(dimension, F.col("failure_code").alias("top_failure_code"))
    )
    return agg.join(top_code, dimension, "left")


def failed_payments(payment_df: DataFrame) -> DataFrame:
    return payment_df.filter(F.col("event_type") == "payment_failed").select(
        "payment_id", "booking_id", "gateway", "payment_method", "amount", "failure_code"
    )

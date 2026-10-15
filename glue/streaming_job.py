"""TicketPulse - AWS Glue Streaming ETL job (Spark Structured Streaming).

Kinesis (ticket_events) --micro-batch every WINDOW_SECONDS-->
  1. archive raw events to S3 (JSON.gz, partitioned by event_type / dt / hour)
  2. normalise + validate; rejects -> s3://<raw>/quarantine/ with their rule violations
  3. dedupe by event_id
  4. Redshift Serverless: append booking/payment events (idempotent) and upsert current state
     (version-guarded) via staging tables + COPY
  5. evaluate real-time alert rules and push alerts to the SQS FIFO buffer
  6. publish custom CloudWatch metrics

Checkpoints in S3 make the job resume exactly where it stopped; every sink is idempotent, so a
replayed micro-batch has no visible effect (raw archive is at-least-once by design).
"""

import sys
import time
from datetime import datetime, timezone

import boto3
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

from ticket_etl import alerts as A
from ticket_etl import redshift_sql as R
from ticket_etl import schema as S
from ticket_etl import transforms as T

ARG_NAMES = [
    "JOB_NAME",
    "SOURCE_DATABASE",
    "SOURCE_TABLE",
    "RAW_BUCKET",
    "CHECKPOINT_PATH",
    "REDSHIFT_TMP_DIR",
    "REDSHIFT_CONNECTION",
    "REDSHIFT_DB",
    "REDSHIFT_IAM_ROLE",
    "ALERTS_QUEUE_URL",
    "WINDOW_SECONDS",
    "FAILURE_RATE_THRESHOLD",
    "MIN_PAYMENTS_FOR_ALERT",
    "HIGH_VALUE_AMOUNT",
    "STARTING_POSITION",
]
args = getResolvedOptions(sys.argv, ARG_NAMES)

sc = SparkContext.getOrCreate()
glue = GlueContext(sc)
spark = glue.spark_session
spark.conf.set("spark.sql.session.timeZone", "UTC")
job = Job(glue)
job.init(args["JOB_NAME"], args)
log = glue.get_logger()

WINDOW_SECONDS = int(args["WINDOW_SECONDS"])
THRESHOLDS = A.Thresholds(
    failure_rate=float(args["FAILURE_RATE_THRESHOLD"]),
    min_payments=int(args["MIN_PAYMENTS_FOR_ALERT"]),
    high_value_amount=float(args["HIGH_VALUE_AMOUNT"]),
)
RAW_PREFIX = f"s3://{args['RAW_BUCKET']}/raw/ticket_events/"
QUARANTINE_PREFIX = f"s3://{args['RAW_BUCKET']}/quarantine/ticket_events/"

sqs = boto3.client("sqs")
cloudwatch = boto3.client("cloudwatch")


# ----------------------------------------------------------------------------- sinks
def archive_raw(df, prefix: str) -> None:
    safe_type = F.when(F.lower(F.col("event_type")).isin(*S.EVENT_TYPES), F.lower(F.col("event_type"))).otherwise(
        F.lit("unknown")
    )
    (
        df.withColumn("event_type_part", safe_type)
        .withColumn("dt", F.date_format(F.current_timestamp(), "yyyy-MM-dd"))
        .withColumn("hour", F.date_format(F.current_timestamp(), "HH"))
        .write.mode("append")
        .partitionBy("event_type_part", "dt", "hour")
        .option("compression", "gzip")
        .json(prefix)
    )


def write_redshift(df, table: str, postactions: str) -> None:
    dyf = DynamicFrame.fromDF(df, glue, f"dyf_{table}")
    glue.write_dynamic_frame.from_jdbc_conf(
        frame=dyf,
        catalog_connection=args["REDSHIFT_CONNECTION"],
        connection_options={
            "database": args["REDSHIFT_DB"],
            "dbtable": f"{R.STAGE_SCHEMA}.{table}",
            "preactions": R.preactions(table),
            "postactions": postactions,
            "aws_iam_role": args["REDSHIFT_IAM_ROLE"],
        },
        redshift_tmp_dir=args["REDSHIFT_TMP_DIR"],
        transformation_ctx=f"write_{table}",
    )


def send_alerts(alerts: list) -> int:
    sent = 0
    for chunk in A.to_sqs_entries(alerts):
        pending = chunk
        for attempt in range(3):
            resp = sqs.send_message_batch(QueueUrl=args["ALERTS_QUEUE_URL"], Entries=pending)
            sent += len(resp.get("Successful", []))
            failed_ids = {f["Id"] for f in resp.get("Failed", [])}
            pending = [e for e in pending if e["Id"] in failed_ids]
            if not pending:
                break
            time.sleep(2**attempt)
        if pending:
            # Failing the batch makes Glue replay it; FIFO dedup ids stop duplicates of the rest.
            raise RuntimeError(f"Could not enqueue {len(pending)} alerts: {pending}")
    return sent


def put_metrics(**values) -> None:
    try:
        cloudwatch.put_metric_data(
            Namespace="TicketPulse",
            MetricData=[
                {"MetricName": name, "Value": float(v), "Unit": "Seconds" if name.endswith("Seconds") else "Count"}
                for name, v in values.items()
            ],
        )
    except Exception as exc:  # metrics must never fail the pipeline
        log.warn(f"put_metric_data failed: {exc}")


# ----------------------------------------------------------------------------- per micro-batch
def process_batch(batch_df, batch_id: int) -> None:
    started = time.time()
    if batch_df.rdd.isEmpty():
        return

    batch_df.persist()
    archive_raw(batch_df, RAW_PREFIX)

    checked = T.with_validation(T.normalize(batch_df)).persist()
    valid, rejects = T.split_valid(checked)

    reject_count = rejects.count()
    if reject_count:
        (
            rejects.withColumn("batch_id", F.lit(batch_id))
            .withColumn("dt", F.date_format(F.current_timestamp(), "yyyy-MM-dd"))
            .write.mode("append")
            .partitionBy("dt")
            .json(QUARANTINE_PREFIX)
        )

    valid_count = valid.count()
    events = T.dedupe_events(valid).persist()
    event_count = events.count()

    bookings = T.booking_events(events).persist()
    payments = T.payment_events(events).persist()

    if bookings.take(1):
        write_redshift(bookings, "booking_events", R.append_dedup_postactions("booking_events", S.BOOKING_EVENTS_COLUMNS))
        write_redshift(
            T.bookings_current(bookings),
            "bookings_current",
            R.upsert_latest_postactions("bookings_current", "booking_id", S.BOOKINGS_CURRENT_COLUMNS),
        )
    if payments.take(1):
        write_redshift(payments, "payment_events", R.append_dedup_postactions("payment_events", S.PAYMENT_EVENTS_COLUMNS))
        write_redshift(
            T.payments_current(payments),
            "payments_current",
            R.upsert_latest_postactions("payments_current", "payment_id", S.PAYMENTS_CURRENT_COLUMNS),
        )

    # ---- real-time alerting (aggregates are tiny: one row per gateway / method)
    alerts_sent = 0
    if payments.take(1):
        gw = [r.asDict() for r in T.payment_outcome_stats(payments, "gateway").collect()]
        pm = [r.asDict() for r in T.payment_outcome_stats(payments, "payment_method").collect()]
        failed = [r.asDict() for r in T.failed_payments(payments).filter(F.col("amount") >= THRESHOLDS.high_value_amount).collect()]
        max_ts = events.agg(F.max("event_time")).first()[0] or datetime.now(timezone.utc)
        alerts = A.detect_alerts(gw, pm, failed, max_ts, WINDOW_SECONDS, THRESHOLDS)
        if alerts:
            alerts_sent = send_alerts(alerts)
            log.info(f"batch {batch_id}: raised {alerts_sent} alerts: {[a.rule for a in alerts]}")

    max_event_time = events.agg(F.max("event_time")).first()[0]
    lag = (datetime.now(timezone.utc) - max_event_time.replace(tzinfo=timezone.utc)).total_seconds() if max_event_time else 0
    put_metrics(
        ProcessedEvents=event_count,
        RejectedEvents=reject_count,
        DuplicateEvents=valid_count - event_count,
        AlertsRaised=alerts_sent,
        EndToEndLagSeconds=max(lag, 0),
        BatchDurationSeconds=time.time() - started,
    )
    log.info(
        f"batch {batch_id}: processed={event_count} rejected={reject_count} "
        f"duplicates={valid_count - event_count} alerts={alerts_sent}"
    )

    for df in (payments, bookings, events, checked, batch_df):
        df.unpersist()


# ----------------------------------------------------------------------------- stream
source = glue.create_data_frame.from_catalog(
    database=args["SOURCE_DATABASE"],
    table_name=args["SOURCE_TABLE"],
    transformation_ctx="kinesis_source",
    additional_options={"startingPosition": args["STARTING_POSITION"], "inferSchema": "false"},
)

glue.forEachBatch(
    frame=source,
    batch_function=process_batch,
    options={"windowSize": f"{WINDOW_SECONDS} seconds", "checkpointLocation": args["CHECKPOINT_PATH"]},
)
job.commit()

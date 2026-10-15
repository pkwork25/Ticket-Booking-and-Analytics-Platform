-- TicketPulse | Redshift Serverless schema
-- Run once (scripts/init_redshift.sh runs it through the Redshift Data API).
-- Column order must match glue/ticket_etl/schema.py (*_COLUMNS).

CREATE SCHEMA IF NOT EXISTS tp;
CREATE SCHEMA IF NOT EXISTS tp_stage;

-- Immutable event log: every booking lifecycle event (append-only, idempotent on event_id).
CREATE TABLE IF NOT EXISTS tp.booking_events (
    event_id        VARCHAR(64)   NOT NULL,
    event_type      VARCHAR(32)   NOT NULL,
    event_time      TIMESTAMP     NOT NULL,
    ingest_time     TIMESTAMP     NOT NULL,
    source          VARCHAR(32),
    booking_id      VARCHAR(64)   NOT NULL,
    user_id         VARCHAR(64),
    event_name      VARCHAR(256),
    event_category  VARCHAR(32),
    venue_city      VARCHAR(64),
    show_time       TIMESTAMP,
    seats           SMALLINT,
    ticket_amount   DECIMAL(12,2),
    booking_status  VARCHAR(24),
    PRIMARY KEY (event_id)
)
DISTKEY (booking_id)
SORTKEY (event_time);

CREATE TABLE IF NOT EXISTS tp.payment_events (
    event_id        VARCHAR(64)   NOT NULL,
    event_type      VARCHAR(32)   NOT NULL,
    event_time      TIMESTAMP     NOT NULL,
    ingest_time     TIMESTAMP     NOT NULL,
    source          VARCHAR(32),
    payment_id      VARCHAR(64)   NOT NULL,
    booking_id      VARCHAR(64)   NOT NULL,
    user_id         VARCHAR(64),
    payment_method  VARCHAR(16),
    gateway         VARCHAR(32),
    payment_status  VARCHAR(16),
    amount          DECIMAL(12,2),
    currency        CHAR(3),
    failure_code    VARCHAR(48),
    refund_amount   DECIMAL(12,2),
    PRIMARY KEY (event_id)
)
DISTKEY (booking_id)
SORTKEY (event_time);

-- Current state (one row per key), upserted every micro-batch with a last_event_time guard.
CREATE TABLE IF NOT EXISTS tp.bookings_current (
    booking_id       VARCHAR(64)  NOT NULL,
    user_id          VARCHAR(64),
    event_name       VARCHAR(256),
    event_category   VARCHAR(32),
    venue_city       VARCHAR(64),
    show_time        TIMESTAMP,
    seats            SMALLINT,
    ticket_amount    DECIMAL(12,2),
    booking_status   VARCHAR(24),
    last_event_id    VARCHAR(64),
    last_event_type  VARCHAR(32),
    last_event_time  TIMESTAMP    NOT NULL,
    updated_at       TIMESTAMP,
    PRIMARY KEY (booking_id)
)
DISTKEY (booking_id)
SORTKEY (last_event_time);

CREATE TABLE IF NOT EXISTS tp.payments_current (
    payment_id       VARCHAR(64)  NOT NULL,
    booking_id       VARCHAR(64)  NOT NULL,
    user_id          VARCHAR(64),
    payment_method   VARCHAR(16),
    gateway          VARCHAR(32),
    payment_status   VARCHAR(16),
    amount           DECIMAL(12,2),
    currency         CHAR(3),
    failure_code     VARCHAR(48),
    refund_amount    DECIMAL(12,2),
    last_event_id    VARCHAR(64),
    last_event_type  VARCHAR(32),
    last_event_time  TIMESTAMP    NOT NULL,
    updated_at       TIMESTAMP,
    PRIMARY KEY (payment_id)
)
DISTKEY (booking_id)
SORTKEY (last_event_time);

-- Staging tables (truncated by the Glue job's preactions before every COPY).
CREATE TABLE IF NOT EXISTS tp_stage.booking_events   (LIKE tp.booking_events);
CREATE TABLE IF NOT EXISTS tp_stage.payment_events   (LIKE tp.payment_events);
CREATE TABLE IF NOT EXISTS tp_stage.bookings_current (LIKE tp.bookings_current);
CREATE TABLE IF NOT EXISTS tp_stage.payments_current (LIKE tp.payments_current);

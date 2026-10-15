-- TicketPulse | analytics views (late-binding so they survive table rebuilds)

-- Gateway x method health over the last 15 minutes (terminal outcomes only).
CREATE OR REPLACE VIEW tp.v_payment_health_15m AS
SELECT
    gateway,
    payment_method,
    COUNT(*)                                                         AS attempts,
    SUM(CASE WHEN event_type = 'payment_succeeded' THEN 1 ELSE 0 END) AS succeeded,
    SUM(CASE WHEN event_type = 'payment_failed'    THEN 1 ELSE 0 END) AS failed,
    ROUND(100.0 * SUM(CASE WHEN event_type = 'payment_succeeded' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2)
                                                                     AS success_rate_pct,
    SUM(CASE WHEN event_type = 'payment_succeeded' THEN amount ELSE 0 END) AS captured_inr
FROM tp.payment_events
WHERE event_type IN ('payment_succeeded', 'payment_failed')
  AND event_time >= DATEADD(minute, -15, GETDATE())
GROUP BY 1, 2
WITH NO SCHEMA BINDING;

-- Hourly booking funnel: created -> payment attempted -> confirmed / expired.
CREATE OR REPLACE VIEW tp.v_booking_funnel_hourly AS
WITH b AS (
    SELECT DATE_TRUNC('hour', MIN(CASE WHEN event_type = 'booking_created' THEN event_time END)) AS hour,
           booking_id,
           MAX(CASE WHEN event_type = 'booking_confirmed' THEN 1 ELSE 0 END) AS confirmed,
           MAX(CASE WHEN event_type = 'booking_expired'   THEN 1 ELSE 0 END) AS expired,
           MAX(CASE WHEN event_type = 'booking_cancelled' THEN 1 ELSE 0 END) AS cancelled
    FROM tp.booking_events
    GROUP BY booking_id
),
p AS (SELECT DISTINCT booking_id FROM tp.payment_events WHERE event_type = 'payment_initiated')
SELECT b.hour,
       COUNT(*)                                        AS bookings_created,
       COUNT(p.booking_id)                             AS payment_attempted,
       SUM(b.confirmed)                                AS confirmed,
       SUM(b.expired)                                  AS expired,
       SUM(b.cancelled)                                AS cancelled,
       ROUND(100.0 * SUM(b.confirmed) / NULLIF(COUNT(*), 0), 2) AS conversion_pct
FROM b LEFT JOIN p ON p.booking_id = b.booking_id
WHERE b.hour IS NOT NULL
GROUP BY b.hour
WITH NO SCHEMA BINDING;

-- Daily GMV by city / category, net of refunds.
CREATE OR REPLACE VIEW tp.v_revenue_daily AS
WITH sales AS (
    SELECT TRUNC(event_time) AS day, venue_city, event_category,
           COUNT(*) AS confirmed_bookings, SUM(seats) AS tickets, SUM(ticket_amount) AS gross_inr
    FROM tp.booking_events
    WHERE event_type = 'booking_confirmed'
    GROUP BY 1, 2, 3
),
refunds AS (
    SELECT TRUNC(r.event_time) AS day, b.venue_city, b.event_category, SUM(r.refund_amount) AS refunded_inr
    FROM tp.payment_events r
    JOIN tp.bookings_current b ON b.booking_id = r.booking_id
    WHERE r.event_type = 'refund_processed'
    GROUP BY 1, 2, 3
)
SELECT s.day, s.venue_city, s.event_category, s.confirmed_bookings, s.tickets, s.gross_inr,
       COALESCE(r.refunded_inr, 0)                  AS refunded_inr,
       s.gross_inr - COALESCE(r.refunded_inr, 0)    AS net_inr
FROM sales s
LEFT JOIN refunds r ON r.day = s.day AND r.venue_city = s.venue_city AND r.event_category = s.event_category
WITH NO SCHEMA BINDING;

-- Top failure reasons in the last 24 hours.
CREATE OR REPLACE VIEW tp.v_failure_codes_24h AS
SELECT gateway, payment_method, failure_code, COUNT(*) AS failures, SUM(amount) AS failed_inr
FROM tp.payment_events
WHERE event_type = 'payment_failed'
  AND event_time >= DATEADD(hour, -24, GETDATE())
GROUP BY 1, 2, 3
WITH NO SCHEMA BINDING;

-- Is the pipeline keeping up?
CREATE OR REPLACE VIEW tp.v_pipeline_freshness AS
SELECT 'payment_events' AS table_name, MAX(event_time) AS max_event_time, MAX(ingest_time) AS max_ingest_time,
       DATEDIFF(second, MAX(event_time), GETDATE()) AS seconds_since_last_event,
       AVG(DATEDIFF(second, event_time, ingest_time)) AS avg_ingest_lag_seconds_1h
FROM tp.payment_events WHERE event_time >= DATEADD(hour, -1, GETDATE())
UNION ALL
SELECT 'booking_events', MAX(event_time), MAX(ingest_time),
       DATEDIFF(second, MAX(event_time), GETDATE()),
       AVG(DATEDIFF(second, event_time, ingest_time))
FROM tp.booking_events WHERE event_time >= DATEADD(hour, -1, GETDATE())
WITH NO SCHEMA BINDING;

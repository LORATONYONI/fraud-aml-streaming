-- Transaction count and value per event-time hour and type over the last 30 days.
-- The event_date filter prunes partitions, so Athena only reads (and bills) the days you ask for.
WITH transactions AS (
    SELECT DISTINCT txn_id, event_ts, txn_type, amount
    FROM "${database}"."transactions"
    WHERE event_date >= date_format(current_date - INTERVAL '30' DAY, '%Y-%m-%d')
)
SELECT
    date_trunc('hour', from_iso8601_timestamp(event_ts)) AS event_hour,
    txn_type,
    count(*) AS transactions,
    sum(CAST(amount AS DECIMAL(18, 2))) AS total_amount
FROM transactions
GROUP BY 1, 2
ORDER BY 1, 2;

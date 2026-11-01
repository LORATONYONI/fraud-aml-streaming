-- The investigation queue: accounts ranked by their most serious alert, then by number of alerts.
WITH alerts AS (
    SELECT DISTINCT
        alert_key,
        account_id,
        rule_id,
        severity,
        CASE severity WHEN 'CRITICAL' THEN 3 WHEN 'HIGH' THEN 2 WHEN 'MEDIUM' THEN 1 ELSE 0 END AS severity_rank,
        from_iso8601_timestamp(event_ts) AS event_time,
        CAST(amount AS DECIMAL(18, 2)) AS amount
    FROM "${database}"."alerts"
)
SELECT
    account_id,
    max_by(severity, severity_rank) AS worst_severity,
    count(*) AS alerts,
    array_join(array_agg(DISTINCT rule_id), ', ') AS rules,
    sum(amount) AS flagged_amount,
    min(event_time) AS first_alert,
    max(event_time) AS last_alert
FROM alerts
GROUP BY account_id
ORDER BY max(severity_rank) DESC, alerts DESC, flagged_amount DESC
LIMIT 50;

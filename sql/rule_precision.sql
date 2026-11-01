-- Precision per detection rule: of the transactions each rule flagged, how many were suspicious?
-- Bronze is at-least-once, so de-duplicate on the natural keys first.
WITH transactions AS (
    SELECT DISTINCT txn_id, label_suspicious
    FROM "${database}"."transactions"
),
alerts AS (
    SELECT DISTINCT alert_key, rule_id, txn_id
    FROM "${database}"."alerts"
)
SELECT
    a.rule_id,
    count(*) AS alerts,
    count_if(t.label_suspicious) AS true_positives,
    round(100.0 * count_if(t.label_suspicious) / count(*), 1) AS precision_pct
FROM alerts AS a
JOIN transactions AS t ON t.txn_id = a.txn_id
GROUP BY a.rule_id
ORDER BY alerts DESC;

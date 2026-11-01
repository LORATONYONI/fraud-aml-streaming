-- Recall per scenario type: a case counts as detected if any of its transactions raised an alert.
-- (PaySim fraud rows are one-transaction cases.)
WITH suspicious AS (
    SELECT DISTINCT txn_id, label_scenario, label_scenario_id
    FROM "${database}"."transactions"
    WHERE label_suspicious
),
flagged AS (
    SELECT DISTINCT txn_id
    FROM "${database}"."alerts"
),
cases AS (
    SELECT
        s.label_scenario,
        s.label_scenario_id,
        max(CASE WHEN f.txn_id IS NOT NULL THEN 1 ELSE 0 END) AS detected
    FROM suspicious AS s
    LEFT JOIN flagged AS f ON f.txn_id = s.txn_id
    GROUP BY 1, 2
)
SELECT
    label_scenario,
    count(*) AS cases,
    sum(detected) AS detected,
    round(100.0 * sum(detected) / count(*), 1) AS recall_pct
FROM cases
GROUP BY label_scenario
ORDER BY label_scenario;

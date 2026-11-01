"""Lambda: alerts stream -> SNS email for alerts at or above a minimum severity.

Reading the alerts table's stream (change data capture) instead of publishing from the detector
means an email is sent only for an alert that was actually stored, and a failed publish is retried
by the stream. Alerts in one batch are grouped into a single email to stay within the SNS free tier
(1,000 emails a month).
"""

from __future__ import annotations

import json
import logging
import os
from collections.abc import Sequence
from typing import Any

from fraud_aml.ddb_stream import inserted_images
from fraud_aml.models import SEVERITY_ORDER, Alert, format_ts

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

MAX_LINES = 25


def select_alerts(event: dict[str, Any], min_severity: str) -> list[Alert]:
    floor = SEVERITY_ORDER[min_severity]
    alerts = [Alert.from_record(image) for _, image in inserted_images(event)]
    return [a for a in alerts if SEVERITY_ORDER.get(a.severity, 0) >= floor]


def build_message(alerts: Sequence[Alert], min_severity: str) -> tuple[str, str]:
    subject = f"[fraud-aml] {len(alerts)} {min_severity}+ alert(s)"
    lines = [f"{len(alerts)} alert(s) at or above {min_severity} severity:", ""]
    for a in sorted(alerts, key=lambda a: (-SEVERITY_ORDER.get(a.severity, 0), a.event_ts))[:MAX_LINES]:
        lines.append(
            f"- [{a.severity}] {a.rule_id} | account {a.account_id} | {a.txn_type} {a.amount} "
            f"| {format_ts(a.event_ts)}\n  {a.reason}"
        )
    if len(alerts) > MAX_LINES:
        lines.append(f"... and {len(alerts) - MAX_LINES} more. Query the alerts table in Athena.")
    return subject[:100], "\n".join(lines)


def handler(event: dict[str, Any], context: Any = None, sns_client: Any = None) -> dict:
    min_severity = os.environ.get("MIN_SEVERITY", "CRITICAL")
    alerts = select_alerts(event, min_severity)
    if not alerts:
        return {"notified": 0}
    if sns_client is None:
        import boto3

        sns_client = boto3.client("sns")
    subject, message = build_message(alerts, min_severity)
    sns_client.publish(TopicArn=os.environ["ALERT_TOPIC_ARN"], Subject=subject, Message=message)
    logger.info("notification sent %s", json.dumps({"alerts": len(alerts)}))
    return {"notified": len(alerts)}

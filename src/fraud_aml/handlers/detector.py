"""Lambda: transactions stream -> detection rules -> alerts table.

Triggered by the DynamoDB Stream on the transactions table (INSERT events only, filtered by the
event source mapping). Notifications are *not* sent from here: the notifier Lambda reads the alerts
table's own stream, so an alert is only announced once it is safely stored.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import timedelta
from decimal import InvalidOperation
from typing import Any

from fraud_aml.config import RuleConfig
from fraud_aml.ddb_stream import inserted_images
from fraud_aml.engine import DetectionEngine
from fraud_aml.models import Transaction

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

_engine: DetectionEngine | None = None


def _build_engine() -> DetectionEngine:
    import boto3  # available in the Lambda runtime; imported lazily so tests don't need it

    from fraud_aml.stores import DynamoDBAlertSink, DynamoDBStateStore

    dynamodb = boto3.resource("dynamodb")
    return DetectionEngine(
        RuleConfig.from_json(os.environ.get("RULE_OVERRIDES")),
        DynamoDBStateStore(dynamodb, os.environ["STATE_TABLE"]),
        DynamoDBAlertSink(dynamodb, os.environ["ALERTS_TABLE"]),
        state_ttl=timedelta(days=int(os.environ.get("STATE_TTL_DAYS", "30"))),
        alert_ttl=timedelta(days=int(os.environ.get("ALERT_TTL_DAYS", "30"))),
    )


def parse_transactions(event: dict[str, Any]) -> tuple[list[Transaction], int]:
    """Return the parsable transactions and how many records were rejected as malformed.

    A malformed record is logged and skipped instead of failing the batch, because a poison record
    would otherwise block its shard until it expires.
    """
    txns, rejected = [], 0
    for record, image in inserted_images(event):
        try:
            txns.append(Transaction.from_record(image))
        except (KeyError, ValueError, TypeError, InvalidOperation) as exc:
            rejected += 1
            logger.error(
                "rejected malformed transaction record",
                extra={"error": repr(exc), "sequence": record.get("dynamodb", {}).get("SequenceNumber")},
            )
    return txns, rejected


def handler(event: dict[str, Any], context: Any = None, engine: DetectionEngine | None = None) -> dict:
    global _engine
    if engine is None:
        if _engine is None:
            _engine = _build_engine()
        engine = _engine

    txns, rejected = parse_transactions(event)
    result = engine.process(txns)
    summary = {**result.summary(), "rejected": rejected}
    logger.info("detector batch complete %s", json.dumps(summary, sort_keys=True))
    return summary

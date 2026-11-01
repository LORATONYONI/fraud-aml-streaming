"""Lambda: transactions + alerts streams -> S3 bronze layer (gzipped NDJSON).

Partitioned by *event* date so Athena partition projection can prune by date without crawlers.
Delivery is at least once (a retried or split batch can write a record twice); the silver layer
de-duplicates on ``txn_id`` / ``alert_key``.
"""

from __future__ import annotations

import gzip
import io
import json
import logging
import os
from collections.abc import Mapping
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Protocol

from fraud_aml.ddb_stream import inserted_images, table_name_from_arn
from fraud_aml.models import format_ts

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

# Numeric attributes written to S3 as JSON integers. Every other number (money) is written as a
# fixed-point string so no consumer can silently turn it into a float.
INTEGER_FIELDS = frozenset({"step", "expires_at"})


class ObjectWriter(Protocol):
    def put(self, key: str, body: bytes) -> None: ...


class S3ObjectWriter:
    def __init__(self, bucket: str, s3_client: Any = None) -> None:
        if s3_client is None:
            import boto3

            s3_client = boto3.client("s3")
        self.bucket = bucket
        self.s3 = s3_client

    def put(self, key: str, body: bytes) -> None:
        self.s3.put_object(Bucket=self.bucket, Key=key, Body=body, ContentType="application/gzip")


def to_json_value(name: str, value: Any) -> Any:
    if isinstance(value, Decimal):
        return int(value) if name in INTEGER_FIELDS else format(value, "f")
    if isinstance(value, set):
        return sorted(to_json_value(name, v) for v in value)
    if isinstance(value, list):
        return [to_json_value(name, v) for v in value]
    if isinstance(value, dict):
        return {k: to_json_value(k, v) for k, v in value.items()}
    if isinstance(value, bytes):
        return value.hex()
    return value


def _event_date(image: Mapping[str, Any], fallback: str) -> str:
    ts = image.get("event_ts")
    if isinstance(ts, str) and len(ts) >= 10 and ts[4] == "-" and ts[7] == "-":
        return ts[:10]
    return fallback


def archive(
    event: Mapping[str, Any],
    writer: ObjectWriter,
    entity_by_table: Mapping[str, str],
    now: datetime | None = None,
) -> dict[str, Any]:
    now = now or datetime.now(timezone.utc)
    archived_at = format_ts(now)
    groups: dict[tuple[str, str], list[tuple[str, dict[str, Any]]]] = {}
    skipped = 0

    for record, image in inserted_images(event):
        table = table_name_from_arn(record.get("eventSourceARN", ""))
        entity = entity_by_table.get(table)
        if entity is None:
            skipped += 1
            logger.warning("record from unmapped table %s skipped", table)
            continue
        sequence = str(record.get("dynamodb", {}).get("SequenceNumber", "0"))
        row = {k: to_json_value(k, v) for k, v in image.items()}
        row.update(
            ingest_stream_sequence=sequence,
            ingest_archived_at=archived_at,
            ingest_source_table=table,
        )
        groups.setdefault((entity, _event_date(image, archived_at[:10])), []).append((sequence, row))

    written = []
    for (entity, event_date), rows in sorted(groups.items()):
        first_sequence = min(rows, key=lambda r: (len(r[0]), r[0]))[0]
        # Deterministic key: an identical retry overwrites the same object instead of duplicating it.
        key = f"bronze/{entity}/event_date={event_date}/{entity}-{first_sequence}.ndjson.gz"
        buffer = io.BytesIO()
        with gzip.GzipFile(fileobj=buffer, mode="wb", mtime=0) as gz:
            for _, row in rows:
                gz.write(json.dumps(row, separators=(",", ":"), sort_keys=True).encode() + b"\n")
        writer.put(key, buffer.getvalue())
        written.append({"key": key, "rows": len(rows)})

    return {"objects": written, "rows": sum(o["rows"] for o in written), "skipped": skipped}


def handler(event: dict[str, Any], context: Any = None, writer: ObjectWriter | None = None) -> dict:
    writer = writer or S3ObjectWriter(os.environ["LAKE_BUCKET"])
    entity_by_table = json.loads(os.environ["ENTITY_BY_TABLE"])
    summary = archive(event, writer, entity_by_table)
    logger.info("archiver batch complete %s", json.dumps(summary))
    return summary

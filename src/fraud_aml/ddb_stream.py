"""Helpers for DynamoDB Streams records delivered to Lambda.

Stream images use DynamoDB's typed JSON (``{"S": "abc"}``, ``{"N": "12.5"}``). This small decoder
avoids pulling boto3 into the core library and keeps numbers as exact Decimals.
"""

from __future__ import annotations

import base64
from collections.abc import Iterator, Mapping
from decimal import Decimal
from typing import Any


def deserialize(value: Mapping[str, Any]) -> Any:
    if len(value) != 1:
        raise ValueError(f"expected a single type tag, got {list(value)}")
    ((tag, raw),) = value.items()
    if tag == "S":
        return raw
    if tag == "N":
        return Decimal(raw)
    if tag == "BOOL":
        return bool(raw)
    if tag == "NULL":
        return None
    if tag == "M":
        return {k: deserialize(v) for k, v in raw.items()}
    if tag == "L":
        return [deserialize(v) for v in raw]
    if tag == "SS":
        return set(raw)
    if tag == "NS":
        return {Decimal(v) for v in raw}
    if tag == "B":
        return base64.b64decode(raw)
    raise ValueError(f"unsupported DynamoDB type tag {tag!r}")


def deserialize_image(image: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    return {key: deserialize(value) for key, value in image.items()}


def table_name_from_arn(event_source_arn: str) -> str:
    """``arn:aws:dynamodb:<region>:<acct>:table/<name>/stream/<label>`` -> ``<name>``."""
    try:
        return event_source_arn.split(":table/", 1)[1].split("/", 1)[0]
    except IndexError as exc:
        raise ValueError(f"not a DynamoDB stream ARN: {event_source_arn!r}") from exc


def inserted_images(event: Mapping[str, Any]) -> Iterator[tuple[Mapping[str, Any], dict[str, Any]]]:
    """Yield ``(raw_record, new_image)`` for every INSERT in a Lambda stream event."""
    for record in event.get("Records", []):
        if record.get("eventName") != "INSERT":
            continue
        image = record.get("dynamodb", {}).get("NewImage")
        if image:
            yield record, deserialize_image(image)

"""Create the Kafka topics if they don't exist yet: ``python -m fraud_aml.kafka.topics``."""

from __future__ import annotations

import argparse
import sys
from typing import Any

from fraud_aml.kafka.common import DEFAULT_BOOTSTRAP, TOPICS


def ensure_topics(bootstrap: str, admin: Any = None, timeout_s: float = 30) -> list[str]:
    """Create any missing topic and return the names created. Safe to run repeatedly."""
    if admin is None:
        from confluent_kafka.admin import AdminClient

        admin = AdminClient({"bootstrap.servers": bootstrap})
    from confluent_kafka.admin import NewTopic

    existing = set(admin.list_topics(timeout=timeout_s).topics)
    missing = [
        NewTopic(spec.name, num_partitions=spec.partitions, replication_factor=1, config=spec.config)
        for spec in TOPICS
        if spec.name not in existing
    ]
    if not missing:
        return []
    created = []
    for name, future in admin.create_topics(missing, operation_timeout=timeout_s).items():
        try:
            future.result()
            created.append(name)
        except Exception as exc:  # a parallel run may have created it first
            if "TOPIC_ALREADY_EXISTS" not in str(exc):
                raise
    return created


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m fraud_aml.kafka.topics", description=__doc__)
    parser.add_argument("--bootstrap", default=DEFAULT_BOOTSTRAP)
    args = parser.parse_args(argv)
    created = ensure_topics(args.bootstrap)
    names = ", ".join(f"{t.name} ({t.partitions} partitions)" for t in TOPICS)
    print(f"Topics ready on {args.bootstrap}: {names}" + (f" — created {', '.join(created)}" if created else ""))
    return 0


if __name__ == "__main__":
    sys.exit(main())

"""Merge baseline traffic with injected scenarios into one event-time-ordered stream."""

from __future__ import annotations

import heapq
import itertools
import random
from collections.abc import Iterable, Iterator, Sequence

from fraud_aml.producer.baseline import BaseEvent, Emitted
from fraud_aml.producer.scenarios import Scenario, ScheduledEvent
from fraud_aml.producer.world import World


def build_stream(
    base: Iterable[BaseEvent],
    world: World,
    scenarios: Sequence[Scenario],
    rng: random.Random,
    *,
    limit: int | None,
    scenario_every: int,
) -> Iterator[Emitted]:
    """Yield ``(transaction, ground_truth)`` pairs in event-time order.

    ``limit`` counts baseline transactions only. A scenario starts every ``scenario_every`` baseline
    transactions (cycling through ``scenarios``); scenario transactions are added on top, and any
    still pending when the baseline ends are flushed at the end so no scenario is cut short.
    """
    pending: list[tuple[object, int, ScheduledEvent]] = []
    order = itertools.count()
    cycle = itertools.cycle(scenarios) if scenarios and scenario_every > 0 else None
    offset = scenario_every // 2

    def due(until: object | None) -> Iterator[Emitted]:
        while pending and (until is None or pending[0][0] <= until):
            event = heapq.heappop(pending)[2]
            txn = event.build(event.ts)
            if txn is not None:
                yield txn, event.truth

    for i, (ts, make) in enumerate(itertools.islice(base, limit)):
        if cycle is not None and i % scenario_every == offset:
            for event in next(cycle).schedule(world, ts, rng):
                heapq.heappush(pending, (event.ts, next(order), event))
        yield from due(ts)
        yield make()
    yield from due(None)

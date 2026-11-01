"""Measure detection quality against the simulation's ground truth.

* Precision per rule: of the transactions a rule flagged, how many were really suspicious?
* Recall per scenario: of the injected scenarios (and PaySim fraud cases), how many were caught by
  at least one alert on at least one of their transactions? Scenario-level recall matches how
  investigators work: one good alert opens the whole case.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field

from fraud_aml.models import Alert, GroundTruth


@dataclass
class RuleStats:
    alerts: int = 0
    true_positives: int = 0

    @property
    def precision(self) -> float:
        return self.true_positives / self.alerts if self.alerts else 0.0


@dataclass
class ScenarioStats:
    cases: int = 0
    detected: int = 0

    @property
    def recall(self) -> float:
        return self.detected / self.cases if self.cases else 0.0


@dataclass
class Report:
    transactions: int
    suspicious_transactions: int
    flagged_transactions: int
    flagged_suspicious: int
    rules: dict[str, RuleStats] = field(default_factory=dict)
    scenarios: dict[str, ScenarioStats] = field(default_factory=dict)

    @property
    def precision(self) -> float:
        return self.flagged_suspicious / self.flagged_transactions if self.flagged_transactions else 0.0

    @property
    def recall(self) -> float:
        return self.flagged_suspicious / self.suspicious_transactions if self.suspicious_transactions else 0.0


def evaluate(truth_by_txn: Mapping[str, GroundTruth], alerts: Iterable[Alert]) -> Report:
    alerts = list(alerts)
    rules: dict[str, RuleStats] = defaultdict(RuleStats)
    flagged = {a.txn_id for a in alerts}
    for alert in alerts:
        stats = rules[alert.rule_id]
        stats.alerts += 1
        truth = truth_by_txn.get(alert.txn_id)
        if truth is not None and truth.suspicious:
            stats.true_positives += 1

    cases: dict[str, set[str]] = defaultdict(set)
    detected: dict[str, set[str]] = defaultdict(set)
    for txn_id, truth in truth_by_txn.items():
        if truth.suspicious and truth.scenario and truth.scenario_id:
            cases[truth.scenario].add(truth.scenario_id)
            if txn_id in flagged:
                detected[truth.scenario].add(truth.scenario_id)

    suspicious = {t for t, g in truth_by_txn.items() if g.suspicious}
    return Report(
        transactions=len(truth_by_txn),
        suspicious_transactions=len(suspicious),
        flagged_transactions=len(flagged),
        flagged_suspicious=len(flagged & suspicious),
        rules=dict(sorted(rules.items())),
        scenarios={
            name: ScenarioStats(cases=len(ids), detected=len(detected[name])) for name, ids in sorted(cases.items())
        },
    )


def format_report(report: Report) -> str:
    lines = [
        "",
        "Detection quality (against simulation ground truth)",
        "=" * 60,
        f"Transactions processed:      {report.transactions:>8,}",
        f"Suspicious (ground truth):   {report.suspicious_transactions:>8,}",
        f"Flagged by any rule:         {report.flagged_transactions:>8,}",
        f"Overall precision:           {report.precision:>8.1%}",
        f"Transaction-level recall:    {report.recall:>8.1%}",
        "",
        f"{'Rule':<22}{'Alerts':>8}{'True pos.':>11}{'Precision':>11}",
        "-" * 52,
    ]
    for rule_id, stats in report.rules.items():
        lines.append(f"{rule_id:<22}{stats.alerts:>8,}{stats.true_positives:>11,}{stats.precision:>11.1%}")
    lines += [
        "",
        f"{'Scenario':<22}{'Cases':>8}{'Detected':>11}{'Recall':>11}",
        "-" * 52,
    ]
    for name, stats in report.scenarios.items():
        lines.append(f"{name:<22}{stats.cases:>8,}{stats.detected:>11,}{stats.recall:>11.1%}")
    return "\n".join(lines)

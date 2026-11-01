from decimal import Decimal

from fraud_aml.config import RuleConfig
from fraud_aml.rules import (
    account_drained,
    amount_spike,
    high_value_transfer,
    mule_pass_through,
    structuring,
    velocity_burst,
)
from fraud_aml.state import EVENT_CASH_IN, EVENT_OUT, EVENT_TRANSFER_IN, AccountEvent, AccountState
from helpers import txn

CFG = RuleConfig()


def state_with(*events: tuple[float, str, str]) -> AccountState:
    """Events given as (minutes_from_T0, kind, amount)."""
    state = AccountState("C1000000001")
    base = txn().epoch
    for i, (minutes, kind, amount) in enumerate(events):
        state.record(AccountEvent(base + int(minutes * 60), kind, Decimal(amount), f"sid{i}"), CFG)
    return state


class TestAccountDrained:
    def test_fires_when_account_is_emptied(self):
        t = txn("TRANSFER", "5000", balance_before="5000", balance_after="0")
        assert account_drained(t, AccountState("C1"), CFG) is not None

    def test_ignores_partial_withdrawal(self):
        t = txn("CASH_OUT", "4000", balance_before="5000")
        assert account_drained(t, AccountState("C1"), CFG) is None

    def test_ignores_small_balances_and_payments(self):
        small = txn("CASH_OUT", "500", balance_before="500", balance_after="0")
        payment = txn("PAYMENT", "5000", balance_before="5000", balance_after="0")
        assert account_drained(small, AccountState("C1"), CFG) is None
        assert account_drained(payment, AccountState("C1"), CFG) is None


def test_high_value_transfer_threshold():
    assert high_value_transfer(txn("TRANSFER", "200000", dest="C2"), AccountState("C1"), CFG) is not None
    assert high_value_transfer(txn("TRANSFER", "199999.99", dest="C2"), AccountState("C1"), CFG) is None


class TestVelocity:
    def test_sixth_outgoing_in_an_hour_fires(self):
        state = state_with(*[(i * 5, EVENT_OUT, "50") for i in range(5)])
        finding = velocity_burst(txn("PAYMENT", "50", minutes=30), state, CFG)
        assert finding is not None
        assert finding[1]["count"] == 6

    def test_events_outside_window_do_not_count(self):
        state = state_with(*[(i * 5, EVENT_OUT, "50") for i in range(5)])
        assert velocity_burst(txn("PAYMENT", "50", minutes=61), state, CFG) is None

    def test_deposits_do_not_count(self):
        state = state_with(*[(i, EVENT_CASH_IN, "50") for i in range(10)])
        assert velocity_burst(txn("PAYMENT", "50", minutes=11), state, CFG) is None


class TestStructuring:
    def test_third_near_threshold_deposit_fires(self):
        state = state_with((0, EVENT_CASH_IN, "9500"), (60, EVENT_CASH_IN, "9800"))
        finding = structuring(txn("CASH_IN", "9900", minutes=120), state, CFG)
        assert finding is not None
        assert finding[1]["total"] == Decimal("29200.00")

    def test_deposits_at_or_above_threshold_are_not_structuring(self):
        state = state_with((0, EVENT_CASH_IN, "9500"), (60, EVENT_CASH_IN, "9800"))
        assert structuring(txn("CASH_IN", "10000", minutes=120), state, CFG) is None

    def test_small_deposits_do_not_count(self):
        state = state_with((0, EVENT_CASH_IN, "500"), (60, EVENT_CASH_IN, "9800"))
        assert structuring(txn("CASH_IN", "9900", minutes=120), state, CFG) is None


class TestMulePassThrough:
    def test_cash_out_of_received_transfers_fires(self):
        state = state_with((0, EVENT_TRANSFER_IN, "4000"), (30, EVENT_TRANSFER_IN, "3000"))
        finding = mule_pass_through(txn("CASH_OUT", "6500", minutes=90), state, CFG)
        assert finding is not None
        assert finding[1]["inbound_transfers"] == 2

    def test_spending_before_the_money_arrived_is_not_pass_through(self):
        state = state_with((0, EVENT_OUT, "5000"), (60, EVENT_TRANSFER_IN, "7000"))
        assert mule_pass_through(txn("CASH_OUT", "1000", minutes=90), state, CFG) is None

    def test_small_inflows_are_ignored(self):
        state = state_with((0, EVENT_TRANSFER_IN, "1000"))
        assert mule_pass_through(txn("CASH_OUT", "1000", minutes=10), state, CFG) is None


class TestAmountSpike:
    def test_fires_far_above_personal_baseline(self):
        state = state_with(*[(i, EVENT_OUT, "100") for i in range(5)])
        assert amount_spike(txn("TRANSFER", "1500", dest="C2", minutes=10), state, CFG) is not None

    def test_needs_enough_history(self):
        state = state_with(*[(i, EVENT_OUT, "100") for i in range(4)])
        assert amount_spike(txn("TRANSFER", "1500", dest="C2", minutes=10), state, CFG) is None

    def test_normal_amounts_do_not_fire(self):
        state = state_with(*[(i, EVENT_OUT, "100") for i in range(5)])
        assert amount_spike(txn("TRANSFER", "900", dest="C2", minutes=10), state, CFG) is None

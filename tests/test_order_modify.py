#===============================================================================
from unittest import TestCase

from src.models import Order, OrderCreate, Reject

from tests.helpers import make_account, make_symbol, make_tick, time_after

################################################################################
#===============================================================================
class TestOrderModify(TestCase):
    #===============================================================================
    def setUp(self):
        self.symbol = make_symbol()
        self.account = make_account(self.symbol)
        self.quote = make_tick(self.symbol)
        request = OrderCreate(account = self.account, symbol = self.symbol,
            size = 1, price_sl = 98.0, price_tp = 102.0)
        self.order = Order.from_request(request, self.quote)
        self.assertIsInstance(self.order, Order)

    #===============================================================================
    def test_order_modify_changes_stop_loss(self):
        """[C] A placed order has an existing stop-loss value.
        [S] Generate a modification that changes the stop-loss.
        [D] The modification is accepted and updates the order value.
        [N] The take-profit remains unchanged.
        """
        modify = self.order.modify(self.quote, price_sl = 97.0)
        if isinstance(modify, Reject):
            self.fail(f"Unexpected modification rejection: {modify.reason}: {modify.message}")

        self.order.on_modify(modify)

        self.assertEqual(self.order.price_sl, 97.0)
        self.assertEqual(self.order.price_tp, 102.0)

    #===============================================================================
    def test_order_modify_removes_stop_loss(self):
        """[C] SL, TP, and expiration are removable optional parameters.
        [S] Generate a modification that explicitly sets SL to None.
        [D] The modification clears SL while preserving TP.
        [N] Explicit None differs from omitting the field.
        """
        modify = self.order.modify(self.quote, price_sl = None)
        if isinstance(modify, Reject):
            self.fail(f"Unexpected modification rejection: {modify.reason}: {modify.message}")

        self.order.on_modify(modify)

        self.assertIsNone(self.order.price_sl)
        self.assertEqual(self.order.price_tp, 102.0)

    #===============================================================================
    def test_order_modify_removes_take_profit_and_expiration(self):
        """[C] An order has optional TP and expiration values.
        [S] Change TP, then explicitly remove TP and expiration.
        [D] Each requested value is applied without changing unrelated fields.
        [N] Optional removal is supported by passing None explicitly.
        """
        self.order.expiration = time_after(10)
        modify = self.order.modify(self.quote, price_tp = 103.0)
        self.order.on_modify(modify)
        self.assertEqual(self.order.price_tp, 103.0)

        modify = self.order.modify(self.quote, price_tp = None, expiration = None)
        self.order.on_modify(modify)

        self.assertIsNone(self.order.price_tp)
        self.assertIsNone(self.order.expiration)

    #===============================================================================
    def test_order_modify_rejects_without_effective_change(self):
        """[C] A placed order receives a modification equal to its current SL.
        [S] Generate the modification without changing any effective parameter.
        [D] The request returns a NO_MODIFY rejection.
        [N] A no-op must not mutate the order.
        """
        result = self.order.modify(self.quote, price_sl = self.order.price_sl)

        self.assertIsInstance(result, Reject)
        self.assertEqual(result.reason, Reject.Reason.NO_MODIFY.name)
        self.assertEqual(self.order.price_sl, 98.0)

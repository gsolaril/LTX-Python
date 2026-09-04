#===============================================================================
from unittest import TestCase

from src.models import Order, OrderCreate, Reject, Trade

from tests.helpers import make_account, make_symbol, make_tick, time_after

################################################################################
#===============================================================================
class TestOrder(TestCase):
    #===============================================================================
    def setUp(self):
        self.symbol = make_symbol()
        self.account = make_account(self.symbol)
        self.quote = make_tick(self.symbol)

    #===============================================================================
    def test_market_request_converts_to_placed_order(self):
        """[C] A valid market request is evaluated against the latest quote.
        [S] Convert a positive-size request into an Order.
        [D] The result is a placed buy order retaining request identity and values.
        [N] The explicit entry price remains absent until execution.
        """
        request = OrderCreate(account = self.account, symbol = self.symbol, size = 1)

        result = Order.from_request(request, self.quote)

        self.assertIsInstance(result, Order)
        self.assertEqual(result.status, Order.Status.PLACED)
        self.assertEqual(result.UID, request.UID)
        self.assertEqual(result.EID, request.UID)
        self.assertEqual(result.size, request.size)
        self.assertIsNone(result.price)
        self.assertEqual(result.time, self.quote.time_event)

    #===============================================================================
    def test_market_order_fills_at_quote_side(self):
        """[C] A placed market order has no explicit entry price.
        [S] Convert and fill a buy order using a quote with distinct ask and bid.
        [D] The resulting trade uses the ask price for the buy execution.
        [N] The quote-side convention is asserted through the produced Trade.
        """
        request = OrderCreate(account = self.account, symbol = self.symbol, size = 1)
        order = Order.from_request(request, self.quote)
        order.status = Order.Status.FILLED

        trade = Trade.from_order(order, self.quote)

        self.assertEqual(trade.price, self.quote.mkt_price(False))
        self.assertEqual(trade.price_trade, self.quote.mkt_price(False))
        self.assertEqual(trade.UID, order.UID)

    #===============================================================================
    def test_limit_order_at_current_price_is_fillable(self):
        """[C] A limit entry reaches the current executable market price exactly.
        [S] Create an order at the current ask and check it against that quote.
        [D] Equality with the limit is treated as a fill boundary.
        [N] This records the inclusive boundary agreed for limit prices.
        """
        entry_price = self.quote.pa + self.symbol.min_price_diff
        request = OrderCreate(account = self.account, symbol = self.symbol,
            size = 1, price = entry_price)
        order = Order.from_request(request, self.quote)
        if isinstance(order, Reject):
            self.fail(f"Unexpected order rejection: {order.reason}: {order.message}")

        fill_quote = make_tick(self.symbol, ask = entry_price,
            bid = entry_price - 1.0)
        self.assertTrue(order.check_filled(fill_quote))

    #===============================================================================
    def test_pending_order_stays_placed_until_price_crosses(self):
        """[C] A buy limit order is below the current ask.
        [S] Check it first at the untouched quote, then at its exact limit price.
        [D] It remains placed initially and becomes fillable at equality.
        [N] A pending order fills once its executable boundary is reached.
        """
        entry_price = self.quote.pa - 2.0
        request = OrderCreate(account = self.account, symbol = self.symbol,
            size = 1, price = entry_price)
        order = Order.from_request(request, self.quote)
        fill_quote = make_tick(self.symbol, time = time_after(1),
            ask = entry_price, bid = entry_price - 1.0)

        self.assertFalse(order.check_filled(self.quote))
        self.assertEqual(order.status, Order.Status.PLACED)
        self.assertTrue(order.check_filled(fill_quote))
        order.status = Order.Status.FILLED
        self.assertFalse(order.check_filled(fill_quote))

    #===============================================================================
    def test_filled_order_is_not_filled_again(self):
        """[C] An order has already transitioned to FILLED.
        [S] Check the same order against another quote.
        [D] A filled order cannot be filled a second time.
        [N] The status guard takes precedence over quote movement.
        """
        request = OrderCreate(account = self.account, symbol = self.symbol, size = 1)
        order = Order.from_request(request, self.quote)
        order.status = Order.Status.FILLED

        self.assertFalse(order.check_filled(make_tick(self.symbol,
            time = time_after(1), ask = 101.0, bid = 100.0)))

    #===============================================================================
    def test_dumped_order_is_not_fillable(self):
        """[C] A pending order has been cancelled and marked DUMPED.
        [S] Check the dumped order against a quote that reaches its price.
        [D] A cancelled order cannot re-enter execution.
        [N] Terminal order states are not fillable.
        """
        request = OrderCreate(account = self.account, symbol = self.symbol,
            size = 1, price = self.quote.pa - 2.0)
        order = Order.from_request(request, self.quote)
        order.on_delete(order.delete(make_tick(self.symbol, time = time_after(1))))
        fill_quote = make_tick(self.symbol, time = time_after(2),
            ask = order.price, bid = order.price - 1.0)

        self.assertFalse(order.check_filled(fill_quote))

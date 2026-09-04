#===============================================================================
from unittest import TestCase

from src.models import OrderCreate, Reject

from tests.helpers import make_account, make_symbol, make_tick, time_after

################################################################################
#===============================================================================
class TestOrderCreate(TestCase):
    #===============================================================================
    def setUp(self):
        self.symbol = make_symbol()
        self.account = make_account(self.symbol)
        self.quote = make_tick(self.symbol)

    #===============================================================================
    def test_market_order_derives_side_and_type(self):
        """[C] A request has no explicit entry or stop prices.
        [S] Create a positive and a negative market-size request.
        [D] Both requests are market orders with buy/sell sides derived from size.
        [N] Zero price is treated as an unspecified market entry.
        """
        buy = OrderCreate(account = self.account, symbol = self.symbol, size = 1,
            price = None)
        sell = OrderCreate(account = self.account, symbol = self.symbol, size = -1,
            price = 0)

        self.assertEqual(buy.type, OrderCreate.Type.MARKET)
        self.assertEqual(buy.side, OrderCreate.Side.BUY)
        self.assertEqual(sell.type, OrderCreate.Type.MARKET)
        self.assertEqual(sell.side, OrderCreate.Side.SELL)

    #===============================================================================
    def test_limit_order_keeps_price_and_side(self):
        """[C] A request specifies a valid non-zero limit price.
        [S] Create buy and sell limit requests around the current quote.
        [D] Each request preserves its price and derives its expected side/type.
        [N] Entry validation is tested separately from construction.
        """
        buy = OrderCreate(account = self.account, symbol = self.symbol, size = 1,
            price = 98.0)
        sell = OrderCreate(account = self.account, symbol = self.symbol, size = -1,
            price = 101.0)

        self.assertEqual(buy.price, 98.0)
        self.assertEqual(buy.type, OrderCreate.Type.LIMIT)
        self.assertEqual(buy.side, OrderCreate.Side.BUY)
        self.assertEqual(sell.price, 101.0)
        self.assertEqual(sell.type, OrderCreate.Type.LIMIT)
        self.assertEqual(sell.side, OrderCreate.Side.SELL)

    #===============================================================================
    def test_stop_prices_are_preserved(self):
        """[C] A market request includes valid stop-loss and take-profit values.
        [S] Create a buy request with both protective prices.
        [D] The request retains both values and accepts its quote validation.
        [N] The stop values are placed on the valid side of the entry price.
        """
        request = OrderCreate(account = self.account, symbol = self.symbol, size = 1,
            price_sl = 98.0, price_tp = 102.0)

        self.assertEqual(request.price_sl, 98.0)
        self.assertEqual(request.price_tp, 102.0)
        self.assertIsNone(request.reject(self.quote))

    #===============================================================================
    def test_small_size_is_rejected(self):
        """[C] The symbol defines a minimum order size of 0.001.
        [S] Create a request smaller than that minimum.
        [D] Validation returns a WRONG_SIZE rejection with the request UID.
        [N] The request remains inspectable for diagnostic output.
        """
        request = OrderCreate(account = self.account, symbol = self.symbol,
            size = 0.0001)

        result = request.reject(self.quote)

        self.assertIsInstance(result, Reject)
        self.assertEqual(result.reason, Reject.Reason.WRONG_SIZE.name)
        self.assertEqual(result.UID, request.UID)

    #===============================================================================
    def test_exact_minimum_size_is_accepted(self):
        """[C] The symbol minimum order size is an inclusive boundary.
        [S] Create a request whose absolute size equals that minimum.
        [D] Size validation accepts the request.
        [N] Equality with the minimum is valid in this trading context.
        """
        request = OrderCreate(account = self.account, symbol = self.symbol,
            size = self.symbol.min_order_size)

        self.assertIsNone(request.reject(self.quote))

    #===============================================================================
    def test_invalid_stop_placement_is_rejected(self):
        """[C] A buy market order has an SL above and TP below its current price.
        [S] Validate the request against the latest quote.
        [D] Validation returns a WRONG_SL rejection before placement.
        [N] Protective levels must be on the correct side of the entry.
        """
        request = OrderCreate(account = self.account, symbol = self.symbol,
            size = 1, price_sl = 101.0, price_tp = 99.0)

        result = request.reject(self.quote)

        self.assertIsInstance(result, Reject)
        self.assertEqual(result.reason, Reject.Reason.WRONG_SL.name)

    #===============================================================================
    def test_expired_request_is_rejected(self):
        """[C] An order expiration precedes the quote event time.
        [S] Create a market request that expired one microsecond before the quote.
        [D] Validation returns an EXPIRED rejection.
        [N] The timestamp is deterministic and distinct from account creation time.
        """
        request = OrderCreate(account = self.account, symbol = self.symbol, size = 1,
            expiration = time_after(-1))

        result = request.reject(self.quote)

        self.assertIsInstance(result, Reject)
        self.assertEqual(result.reason, Reject.Reason.EXPIRED.name)

#===============================================================================
from unittest import TestCase

from src.models import Order, OrderCreate, Trade

from tests.helpers import make_account, make_candle, make_symbol, make_tick, time_after

################################################################################
#===============================================================================
class TestTrade(TestCase):
    #===============================================================================
    def setUp(self):
        self.symbol = make_symbol()
        self.account = make_account(self.symbol)
        self.quote = make_tick(self.symbol)

    #===============================================================================
    def _trade(self, size: float = 1, price_sl: float = None,
            price_tp: float = None):
        request = OrderCreate(account = self.account, symbol = self.symbol,
            size = size, price_sl = price_sl, price_tp = price_tp)
        order = Order.from_request(request, self.quote)
        self.assertIsInstance(order, Order)
        order.status = Order.Status.FILLED
        return Trade.from_order(order, self.quote)

    #===============================================================================
    def test_market_trade_has_quote_side_entry_price(self):
        """[C] A market order executes immediately against a crossed quote.
        [S] Create a buy trade and a sell trade from market orders.
        [D] The buy enters at ask and the sell enters at bid.
        [N] Distinct quote prices make an accidental side inversion observable.
        """
        buy = self._trade(size = 1)
        sell = self._trade(size = -1)

        self.assertEqual(buy.price_trade, self.quote.pa)
        self.assertEqual(sell.price_trade, self.quote.pb)

    #===============================================================================
    def test_pnl_has_expected_sign_and_amount(self):
        """[C] A buy trade entered at ask and later closes at a higher bid.
        [S] Update the trade with a later quote and calculate its PNL.
        [D] The profit equals the close-entry price difference times signed size.
        [N] A one-unit trade makes the expected amount directly observable.
        """
        trade = self._trade(size = 1)
        trade.price = 105.0

        self.assertEqual(trade.pnl, 500.0)

    #===============================================================================
    def test_sell_trade_loss_has_negative_pnl(self):
        """[C] A sell trade entered at bid is later closed at a higher ask.
        [S] Set the close price above the entry price.
        [D] The signed PNL is negative for the losing sell trade.
        [N] Contract size remains included in the calculation.
        """
        trade = self._trade(size = -1)
        trade.price = 105.0

        self.assertEqual(trade.pnl, -600.0)

    #===============================================================================
    def test_buy_trade_closes_at_bid(self):
        """[C] A buy trade must be closed by selling into the bid.
        [S] Check closure with a later tick whose ask and bid differ.
        [D] The trade's closing price is the later bid, not the ask.
        [N] No SL or TP is configured, so the trade remains open.
        """
        trade = self._trade(size = 1)
        closing_quote = make_tick(self.symbol, time = time_after(1),
            ask = 105.0, bid = 104.0)

        self.assertFalse(trade.check_closed(closing_quote))
        self.assertEqual(trade.price, closing_quote.pb)
        self.assertEqual(trade.status, Trade.Status.OPENED)

    #===============================================================================
    def test_sell_trade_closes_at_ask(self):
        """[C] A sell trade must be closed by buying back at the ask.
        [S] Check closure with a later tick whose ask and bid differ.
        [D] The trade's closing price is the later ask, not the bid.
        [N] No SL or TP is configured, so the trade remains open.
        """
        trade = self._trade(size = -1)
        closing_quote = make_tick(self.symbol, time = time_after(1),
            ask = 105.0, bid = 104.0)

        self.assertFalse(trade.check_closed(closing_quote))
        self.assertEqual(trade.price, closing_quote.pa)
        self.assertEqual(trade.status, Trade.Status.OPENED)

    #===============================================================================
    def test_buy_trade_closes_at_candle_bid_range(self):
        """[C] A buy trade is evaluated against a candle with an ask/bid range.
        [S] Process a later candle without configured stops.
        [D] The close price uses the candle's bid-side low for a buy trade.
        [N] Candle range data models the worst executable close within its interval.
        """
        trade = self._trade(size = 1)
        closing_quote = make_candle(self.symbol, time = time_after(60_000_000),
            low_bid = 97.0, close_bid = 99.0, high_ask = 103.0,
            close_ask = 102.0)

        self.assertFalse(trade.check_closed(closing_quote))
        self.assertEqual(trade.price, closing_quote.lb)

    #===============================================================================
    def test_buy_trade_with_sl_only_closes_at_sl(self):
        """[C] A buy trade has an SL but no TP.
        [S] Provide a quote whose bid touches the SL boundary.
        [D] The trade closes with SL status at the configured SL price.
        [N] A missing TP must not disable SL evaluation.
        """
        trade = self._trade(size = 1, price_sl = 99.0)
        closing_quote = make_tick(self.symbol, time = time_after(1),
            ask = 100.0, bid = 99.0)

        self.assertTrue(trade.check_closed(closing_quote))
        self.assertEqual(trade.status, Trade.Status.SL)
        self.assertEqual(trade.price, 99.0)

    #===============================================================================
    def test_buy_trade_with_tp_only_closes_at_tp(self):
        """[C] A buy trade has a TP but no SL.
        [S] Provide a quote whose bid touches the TP boundary.
        [D] The trade closes with TP status at the configured TP price.
        [N] A missing SL must not disable TP evaluation.
        """
        trade = self._trade(size = 1, price_tp = 102.0)
        closing_quote = make_tick(self.symbol, time = time_after(1),
            ask = 103.0, bid = 102.0)

        self.assertTrue(trade.check_closed(closing_quote))
        self.assertEqual(trade.status, Trade.Status.TP)
        self.assertEqual(trade.price, 102.0)

    #===============================================================================
    def test_sell_trade_with_sl_only_closes_at_sl(self):
        """[C] A sell position has an SL above its entry and no TP.
        [S] Provide a quote whose ask touches the SL boundary.
        [D] The position closes with SL status at the configured price.
        [N] A missing TP must not disable sell-side SL evaluation.
        """
        trade = self._trade(size = -1, price_sl = 101.0)
        closing_quote = make_tick(self.symbol, time = time_after(1),
            ask = 101.0, bid = 100.0)

        self.assertTrue(trade.check_closed(closing_quote))
        self.assertEqual(trade.status, Trade.Status.SL)
        self.assertEqual(trade.price, 101.0)

    #===============================================================================
    def test_sell_trade_with_tp_only_closes_at_tp(self):
        """[C] A sell position has a TP below its entry and no SL.
        [S] Provide a quote whose ask touches the TP boundary.
        [D] The position closes with TP status at the configured price.
        [N] A missing SL must not disable sell-side TP evaluation.
        """
        trade = self._trade(size = -1, price_tp = 98.0)
        closing_quote = make_tick(self.symbol, time = time_after(1),
            ask = 98.0, bid = 97.0)

        self.assertTrue(trade.check_closed(closing_quote))
        self.assertEqual(trade.status, Trade.Status.TP)
        self.assertEqual(trade.price, 98.0)

    #===============================================================================
    def test_sell_stop_loss_takes_precedence_when_both_boundaries_are_touched(self):
        """[C] A sell position has both SL and TP configured.
        [S] Evaluate an ask that crosses both protective boundaries.
        [D] The first deterministic check closes the position as SL.
        [N] SL evaluation precedes TP evaluation for both position directions.
        """
        trade = self._trade(size = -1, price_sl = 101.0, price_tp = 98.0)
        closing_quote = make_tick(self.symbol, time = time_after(1),
            ask = 102.0, bid = 97.0)

        self.assertTrue(trade.check_closed(closing_quote))
        self.assertEqual(trade.status, Trade.Status.SL)

    #===============================================================================
    def test_stop_loss_takes_precedence_when_both_boundaries_are_touched(self):
        """[C] A buy trade has both SL and TP configured.
        [S] Evaluate a quote whose ranged bid is beyond both configured levels.
        [D] The first deterministic check closes the trade as SL.
        [N] SL is evaluated before TP when both conditions are true.
        """
        trade = self._trade(size = 1, price_sl = 98.0, price_tp = 102.0)
        closing_quote = make_tick(self.symbol, time = time_after(1),
            ask = 103.0, bid = 97.0)

        self.assertTrue(trade.check_closed(closing_quote))
        self.assertEqual(trade.status, Trade.Status.SL)

    #===============================================================================
    def test_closed_trade_ignores_modification(self):
        """[C] A position has already been closed by its SL.
        [S] Attempt to modify its SL and TP afterward.
        [D] The closed position retains its protective values.
        [N] Closed positions cannot re-enter the active lifecycle.
        """
        trade = self._trade(size = 1, price_sl = 98.0, price_tp = 102.0)
        closing_quote = make_tick(self.symbol, time = time_after(1),
            ask = 99.0, bid = 98.0)
        trade.check_closed(closing_quote)

        trade.on_modify(trade.modify(closing_quote, price_sl = 97.0))

        self.assertEqual(trade.status, Trade.Status.SL)
        self.assertEqual(trade.price_sl, 98.0)
        self.assertEqual(trade.price_tp, 102.0)

    #===============================================================================
    def test_trade_modification_removes_stops(self):
        """[C] An active position has both optional protective values.
        [S] Generate a modification explicitly removing SL and TP.
        [D] Both values are cleared while the position remains open.
        [N] Explicit None values represent removal.
        """
        trade = self._trade(size = 1, price_sl = 98.0, price_tp = 102.0)
        modify = trade.modify(self.quote, price_sl = None, price_tp = None)

        trade.on_modify(modify)

        self.assertIsNone(trade.price_sl)
        self.assertIsNone(trade.price_tp)

    #===============================================================================
    def test_hedging_increases_position(self):
        """[C] A buy trade is followed by a same-side buy hedge.
        [S] Apply a one-unit hedge to an active two-unit trade.
        [D] The active position grows to three units and keeps its identity.
        [N] The original trade remains the position-defining trade.
        """
        trade = self._trade(size = 2)
        hedge = self._trade(size = 1)
        UID, EID = trade.UID, trade.EID

        trade.check_hedged(hedge)

        self.assertEqual(trade.size, 3)
        self.assertEqual(trade.UID, UID)
        self.assertEqual(trade.EID, EID)

    #===============================================================================
    def test_hedging_decreases_position_without_closing(self):
        """[C] A buy trade is partially hedged by a sell trade.
        [S] Apply a one-unit sell hedge to an active two-unit trade.
        [D] The active position remains a one-unit buy with its original identity.
        [N] The opposing hedge is consumed as a hedge operation.
        """
        trade = self._trade(size = 2)
        hedge = self._trade(size = -1)
        UID, EID = trade.UID, trade.EID

        trade.check_hedged(hedge)

        self.assertEqual(trade.size, 1)
        self.assertEqual(trade.side, Trade.Side.BUY)
        self.assertEqual(trade.UID, UID)
        self.assertEqual(trade.EID, EID)

    #===============================================================================
    def test_hedging_closes_position_at_zero(self):
        """[C] A buy trade is hedged by an equal sell trade.
        [S] Apply a two-unit sell hedge to an active two-unit trade.
        [D] The active trade reaches the HEDGED state and keeps its identity.
        [N] Equality with the minimum remaining size closes the position.
        """
        trade = self._trade(size = 2)
        hedge = self._trade(size = -2)
        UID, EID = trade.UID, trade.EID

        trade.check_hedged(hedge)

        self.assertEqual(trade.status, Trade.Status.HEDGED)
        self.assertEqual(trade.UID, UID)
        self.assertEqual(trade.EID, EID)

    #===============================================================================
    def test_hedging_inverts_position_identity(self):
        """[C] A larger sell hedge opposes an active two-unit buy trade.
        [S] Apply a three-unit sell hedge to the active position.
        [D] The position becomes a one-unit sell using the hedge identity.
        [N] Inversion transfers UID and EID to the trade defining the new side.
        """
        trade = self._trade(size = 2)
        hedge = self._trade(size = -3)
        hedge_UID, hedge_EID = hedge.UID, hedge.EID

        trade.check_hedged(hedge)

        self.assertEqual(trade.size, -1)
        self.assertEqual(trade.side, Trade.Side.SELL)
        self.assertEqual(trade.UID, hedge_UID)
        self.assertEqual(trade.EID, hedge_EID)

#===============================================================================
from unittest import TestCase
from pandas import Timedelta

from src.models import Account, Order, OrderCreate, Reject, Rules, Trade

from tests.helpers import make_account, make_symbol, make_tick, symbol_key, time_after

################################################################################
#===============================================================================
class TestAccount(TestCase):
    #===============================================================================
    def setUp(self):
        self.symbol = make_symbol()
        self.account = make_account(self.symbol)
        self.quote = make_tick(self.symbol)
        self.account.time = self.quote.time_event
        self.rules = Rules()

    #===============================================================================
    def _market_fill(self, account: Account, symbol, size: float, quote):
        """Submit and fill one market order after the account's current event."""
        request = OrderCreate(account = account, symbol = symbol, size = size)
        request.time = account.time + Timedelta(microseconds = 1)
        order = account.on_order_create(request, self.rules, quote)
        self.assertIsInstance(order, Order,
            f"Unexpected order result: {getattr(order, 'reason', order)}")
        trade = account.on_order_filled(order, self.rules, quote)
        if (trade is not None): self.assertIsInstance(trade, Trade,
            f"Unexpected fill result: {getattr(trade, 'reason', trade)}")
        return trade

    #===============================================================================
    def test_market_order_uses_quote_for_margin_and_entry(self):
        """[C] A fresh account receives an immediately executable buy order.
        [S] Create the order and process it at a quote with distinct ask/bid.
        [D] The order is accepted and its trade enters at the current ask.
        [N] Market margin must use the same effective quote-side price.
        """
        request = OrderCreate(account = self.account, symbol = self.symbol,
            size = 1)

        order = self.account.on_order_create(request, self.rules, self.quote)
        self.assertIsInstance(order, Order)
        filled = self.account.on_order_filled(order, self.rules, self.quote)

        self.assertIsInstance(filled, Trade)
        self.assertEqual(filled.price_trade, self.quote.pa)
        self.assertEqual(self.account.order_count, 0)
        self.assertEqual(self.account.trade_count, 1)
        self.assertIn("NETTING", self.account.trades_active[
            (self.symbol.venue, self.symbol.symbol)])

    #===============================================================================
    def test_margin_rejects_order_over_configured_limit(self):
        """[C] The account permits at most 1 percent margin usage.
        [S] Submit a market order whose quote-priced margin exceeds that limit.
        [D] The request is rejected with MAX_MARGIN and enters no order book.
        [N] The effective market price is the quote ask for this buy request.
        """
        rules = Rules(max_mPRC = 0.01)
        request = OrderCreate(account = self.account, symbol = self.symbol,
            size = 2)

        result = self.account.on_order_create(request, rules, self.quote)

        self.assertIsInstance(result, Reject)
        self.assertEqual(result.reason, Reject.Reason.MAX_MARGIN.name)
        self.assertEqual(self.account.order_count, 0)

    #===============================================================================
    def test_order_frequency_rejects_an_old_event(self):
        """[C] An order event occurs before the account's current timestamp.
        [S] Submit an order with a timestamp one microsecond before account time.
        [D] The stale request is rejected and does not enter account books.
        [N] This models an old event that never entered the live account.
        """
        rules = Rules(max_freq_us = 1000)
        request = OrderCreate(account = self.account, symbol = self.symbol,
            size = 1)
        request.time = time_after(-1)

        result = self.account.on_order_create(request, rules, self.quote)

        self.assertIsNone(result)
        self.assertEqual(self.account.order_count, 0)

    #===============================================================================
    def test_order_limit_rejects_at_exact_boundary(self):
        """[C] The account has already reached its maximum order count.
        [S] Submit a valid order with max_orders equal to the current count.
        [D] The new order is rejected with MAX_ORDERS and is not stored.
        [N] Equality with the configured order limit is an inclusive boundary.
        """
        rules = Rules(max_orders = 0)
        request = OrderCreate(account = self.account, symbol = self.symbol,
            size = 1)

        result = self.account.on_order_create(request, rules, self.quote)

        self.assertIsInstance(result, Reject)
        self.assertEqual(result.reason, Reject.Reason.MAX_ORDERS.name)
        self.assertEqual(self.account.order_count, 0)

    #===============================================================================
    def test_trade_closes_at_sl_and_updates_account_state(self):
        """[C] A fresh netting account opens one buy trade with an SL.
        [S] Fill it at ask, then process a later bid touching the SL.
        [D] The trade moves to closed books and account state realizes the loss.
        [N] The close price is the bid because a buy closes by selling.
        """
        request = OrderCreate(account = self.account, symbol = self.symbol,
            size = 1, price_sl = 98.0)
        order = self.account.on_order_create(request, self.rules, self.quote)
        trade = self.account.on_order_filled(order, self.rules, self.quote)
        self.assertIsInstance(trade, Trade)
        closing_quote = make_tick(self.symbol, time = time_after(1),
            ask = 99.0, bid = 98.0)

        result = self.account.on_trade_closed(trade, self.rules, closing_quote)

        symbol_key = (self.symbol.venue, self.symbol.symbol)
        self.assertIs(result, trade)
        self.assertEqual(trade.status, Trade.Status.CLOSED)
        self.assertIsNone(self.account.trades_active[symbol_key]["NETTING"])
        self.assertIn(trade.UID, self.account.trades_closed[symbol_key])
        self.assertEqual(self.account.trade_count, 0)
        self.assertEqual(self.account.balance, 9800.0)
        self.assertEqual(self.account.rPNL, -200.0)
        self.assertEqual(self.account.uPNL, 0.0)
        self.assertEqual(self.account.NAV, 0.0)
        self.assertEqual(self.account.GAV, 0.0)

    #===============================================================================
    def test_delete_trade_realizes_pnl_and_moves_trade_to_closed_book(self):
        """[C] A netting account contains one active market buy trade.
        [S] Delete the trade at a later quote with a higher bid.
        [D] The trade is closed, realized PNL is booked, and active books are cleared.
        [N] Trade must be checked before Order because Trade inherits from Order.
        """
        request = OrderCreate(account = self.account, symbol = self.symbol,
            size = 1)
        order = self.account.on_order_create(request, self.rules, self.quote)
        trade = self.account.on_order_filled(order, self.rules, self.quote)
        closing_quote = make_tick(self.symbol, time = time_after(1),
            ask = 105.0, bid = 104.0)
        delete = trade.delete(closing_quote)

        result = self.account.on_order_delete(delete, closing_quote)

        symbol_key = (self.symbol.venue, self.symbol.symbol)
        self.assertIs(result, trade)
        self.assertEqual(trade.status, Trade.Status.CLOSED)
        self.assertIsNone(self.account.trades_active[symbol_key]["NETTING"])
        self.assertIn(trade.UID, self.account.trades_closed[symbol_key])
        self.assertEqual(self.account.trade_count, 0)
        self.assertEqual(self.account.balance, 10400.0)
        self.assertEqual(self.account.rPNL, 400.0)

    #===============================================================================
    def test_delete_active_order_moves_it_to_closed_book(self):
        """[C] A netting account contains one pending limit order.
        [S] Delete the active order through the account lifecycle.
        [D] The order becomes DUMPED and moves to the closed order book.
        [N] No trade or balance state is created by deleting a pending order.
        """
        request = OrderCreate(account = self.account, symbol = self.symbol,
            size = 1, price = 98.0)
        order = self.account.on_order_create(request, self.rules, self.quote)
        delete = order.delete(make_tick(self.symbol, time = time_after(1)))

        result = self.account.on_order_delete(delete, self.quote)

        symbol_key = (self.symbol.venue, self.symbol.symbol)
        self.assertIs(result, order)
        self.assertEqual(order.status, Order.Status.DUMPED)
        self.assertNotIn(order.UID, self.account.orders_active[symbol_key])
        self.assertIn(order.UID, self.account.orders_closed[symbol_key])
        self.assertEqual(self.account.order_count, 0)
        self.assertEqual(self.account.balance, 10000.0)

    #===============================================================================
    def test_check_order_exists_handles_invalid_uid_with_empty_netting_book(self):
        """[C] A fresh netting account has no active trade.
        [S] Look up a modification carrying an unknown UID.
        [D] The lookup returns an UNKNOWN_UID rejection without raising.
        [N] Empty NETTING is a valid sentinel state, not a trade object.
        """
        from src.models import OrderModify
        request = OrderModify(account = self.account)
        request.UID = "UNKNOWN"

        result = self.account.check_order_exists(request, self.quote)

        self.assertIsInstance(result, Reject)
        self.assertEqual(result.reason, Reject.Reason.UNKNOWN_UID.name)

    #===============================================================================
    def test_trade_modify_updates_active_trade(self):
        """[C] A netting account contains one active trade with an SL.
        [S] Modify that trade through Account.on_order_modify.
        [D] The active trade receives the new SL value.
        [N] Account lookup must resolve the NETTING trade by UID.
        """
        request = OrderCreate(account = self.account, symbol = self.symbol,
            size = 1, price_sl = 98.0)
        order = self.account.on_order_create(request, self.rules, self.quote)
        trade = self.account.on_order_filled(order, self.rules, self.quote)
        modify = trade.modify(self.quote, price_sl = 97.0)

        result = self.account.on_order_modify(modify, self.quote)

        self.assertIs(result, trade)
        self.assertEqual(trade.price_sl, 97.0)

    #===============================================================================
    def test_trade_closes_at_tp_and_realizes_profit(self):
        """[C] A fresh netting account opens one buy trade with a TP.
        [S] Fill at ask, then process a later bid touching the TP.
        [D] The trade closes and realizes the contract-sized profit.
        [N] A buy closes at bid, which is the relevant TP trigger price.
        """
        request = OrderCreate(account = self.account, symbol = self.symbol,
            size = 1, price_tp = 102.0)
        order = self.account.on_order_create(request, self.rules, self.quote)
        trade = self.account.on_order_filled(order, self.rules, self.quote)
        closing_quote = make_tick(self.symbol, time = time_after(1),
            ask = 103.0, bid = 102.0)

        result = self.account.on_trade_closed(trade, self.rules, closing_quote)

        self.assertIs(result, trade)
        self.assertEqual(trade.status, Trade.Status.CLOSED)
        self.assertEqual(self.account.balance, 10200.0)
        self.assertEqual(self.account.rPNL, 200.0)
        self.assertEqual(self.account.trade_count, 0)

    #===============================================================================
    def test_mark_to_market_updates_unrealized_account_state(self):
        """[C] A netting account contains one active buy position without stops.
        [S] Mark the position against a later quote without closing it.
        [D] uPNL, NAV, GAV, equity, margin, and mPRC update while balance/rPNL do not.
        [N] The buy position is entered at ask and marked for closure at bid.
        """
        request = OrderCreate(account = self.account, symbol = self.symbol,
            size = 1)
        order = self.account.on_order_create(request, self.rules, self.quote)
        trade = self.account.on_order_filled(order, self.rules, self.quote)
        quote = make_tick(self.symbol, time = time_after(1),
            ask = 105.0, bid = 104.0)

        result = self.account.on_trade_closed(trade, self.rules, quote)

        self.assertIsNone(result)
        self.assertEqual(self.account.balance, 10000.0)
        self.assertEqual(self.account.rPNL, 0.0)
        self.assertEqual(self.account.uPNL, 400.0)
        self.assertEqual(self.account.NAV, 10400.0)
        self.assertEqual(self.account.GAV, 10400.0)
        self.assertEqual(self.account.equity, 10400.0)
        self.assertEqual(self.account.margin, 104.0)
        self.assertEqual(self.account.mPRC, 0.01)
        self.assertEqual(self.account.trade_count, 1)

    #===============================================================================
    def test_hedging_account_keeps_same_symbol_positions_independent(self):
        """[C] A hedging account can hold multiple positions for one symbol.
        [S] Open two same-symbol buy positions with different UIDs.
        [D] Both positions remain separate and the trade count is two.
        [N] Aggregate exposure is intentionally not maintained by Account.
        """
        account = make_account(self.symbol, account_id = "HEDGE",
            is_hedging = True)
        quote = make_tick(self.symbol)
        account.time = quote.time_event
        rules = Rules()
        requests = [OrderCreate(account = account, symbol = self.symbol,
            size = 1), OrderCreate(account = account, symbol = self.symbol,
            size = -1)]
        trades = []
        for request in requests:
            request.time = account.time + Timedelta(microseconds = 1)
            order = account.on_order_create(request, rules, quote)
            trades.append(account.on_order_filled(order, rules, quote))

        positions = account.trades_active[symbol_key(self.symbol)]
        self.assertEqual(len(positions), 2)
        self.assertEqual(account.trade_count, 2)
        self.assertNotEqual(trades[0].UID, trades[1].UID)
        self.assertNotEqual(trades[0].EID, trades[1].EID)
        self.assertIs(positions[trades[0].UID], trades[0])
        self.assertIs(positions[trades[1].UID], trades[1])

        trades[0].price = self.quote.pb - 1.0
        trades[1].price = self.quote.pa + 1.0
        self.assertEqual(trades[0].pnl, -200.0)
        self.assertEqual(trades[1].pnl, -200.0)
        self.assertEqual(trades[0].status, Trade.Status.OPENED)
        self.assertEqual(trades[1].status, Trade.Status.OPENED)

    #===============================================================================
    def test_hedging_account_modifies_and_closes_only_selected_position(self):
        """[C] A hedging account contains two independent same-symbol positions.
        [S] Modify and then delete only the first position.
        [D] The second position remains active and unchanged.
        [N] Position identity determines every lifecycle operation.
        """
        account = make_account(self.symbol, account_id = "HEDGE-OPS",
            is_hedging = True)
        quote = make_tick(self.symbol)
        account.time = quote.time_event
        rules = Rules()
        first_request = OrderCreate(account = account, symbol = self.symbol,
            size = 1, price_sl = 98.0)
        second_request = OrderCreate(account = account, symbol = self.symbol,
            size = -1, price_tp = 98.0)
        first_request.time = account.time + Timedelta(microseconds = 1)
        first = account.on_order_filled(
            account.on_order_create(first_request, rules, quote), rules, quote)
        second_request.time = account.time + Timedelta(microseconds = 1)
        second = account.on_order_filled(
            account.on_order_create(second_request, rules, quote), rules, quote)
        modify = first.modify(quote, price_sl = 97.0)

        account.on_order_modify(modify, quote)
        delete = first.delete(make_tick(self.symbol, time = time_after(1)))
        account.on_order_delete(delete, make_tick(self.symbol,
            time = time_after(1), ask = 101.0, bid = 100.0))

        positions = account.trades_active[symbol_key(self.symbol)]
        self.assertNotIn(first.UID, positions)
        self.assertIn(second.UID, positions)
        self.assertEqual(second.price_tp, 98.0)
        self.assertEqual(account.trade_count, 1)

    #===============================================================================
    def test_closed_objects_are_not_addressable_for_later_commands(self):
        """[C] An account has already closed one pending order and one position.
        [S] Reuse both UIDs in later modification lookups.
        [D] Closed objects are no longer addressable in active account books.
        [N] The lookup returns UNKNOWN_UID rather than mutating closed state.
        """
        order_request = OrderCreate(account = self.account, symbol = self.symbol,
            size = 1, price = 98.0)
        order = self.account.on_order_create(order_request, self.rules, self.quote)
        order_delete = order.delete(self.quote)
        self.account.on_order_delete(order_delete, self.quote)

        trade_request = OrderCreate(account = self.account, symbol = self.symbol,
            size = 1)
        trade_order = self.account.on_order_create(trade_request,
            self.rules, self.quote)
        trade = self.account.on_order_filled(trade_order, self.rules, self.quote)
        trade_delete = trade.delete(make_tick(self.symbol, time = time_after(1)))
        self.account.on_order_delete(trade_delete, make_tick(self.symbol,
            time = time_after(1), ask = 101.0, bid = 100.0))

        from src.models import OrderModify
        for UID in (order.UID, trade.UID):
            modify = OrderModify(account = self.account)
            modify.UID = UID
            result = self.account.check_order_exists(modify, self.quote)
            self.assertIsInstance(result, Reject)
            self.assertEqual(result.reason, Reject.Reason.UNKNOWN_UID.name)

    #===============================================================================
    def test_netting_and_books_are_isolated_per_symbol(self):
        """[C] A netting account trades two independent symbols.
        [S] Open two positions on symbol A and one position on symbol B.
        [D] Symbol A nets only within A, while symbol B retains its own position.
        [N] Account books must never cross symbol boundaries.
        """
        symbol_b = make_symbol(symbol = "OTHERSYMBOL")
        for books in (self.account.orders_active, self.account.trades_active,
                self.account.orders_closed, self.account.trades_closed):
            books[symbol_key(symbol_b)] = {}
        self.account.trades_active[symbol_key(symbol_b)]["NETTING"] = None
        self.account.time = self.quote.time_event
        request_a1 = OrderCreate(account = self.account, symbol = self.symbol,
            size = 1)
        request_a2 = OrderCreate(account = self.account, symbol = self.symbol,
            size = 1)
        request_b = OrderCreate(account = self.account, symbol = symbol_b,
            size = 1)
        quote_b = make_tick(symbol_b)

        for request, quote in ((request_a1, self.quote), (request_a2, self.quote),
                (request_b, quote_b)):
            request.time = self.account.time + Timedelta(microseconds = 1)
            order = self.account.on_order_create(request, self.rules, quote)
            self.assertIsInstance(order, Order,
                f"Unexpected order result: {getattr(order, 'reason', order)}")
            self.account.on_order_filled(order, self.rules, quote)

        position_a = self.account.trades_active[symbol_key(self.symbol)]["NETTING"]
        position_b = self.account.trades_active[symbol_key(symbol_b)]["NETTING"]
        self.assertEqual(position_a.size, 2)
        self.assertEqual(position_b.size, 1)
        self.assertEqual(self.account.trade_count, 2)
        self.assertEqual(self.account.GAV,
            abs(position_a.asset_value) + abs(position_b.asset_value))

    #===============================================================================
    def test_netting_buy_buy_sell_sell_closes_one_position(self):
        """[C] A netting account maintains one position per symbol.
        [S] Apply buy, buy, sell, and sell market orders sequentially.
        [D] The position sizes reduce to zero and the active position is closed.
        [N] The sequence tests netting rather than independent hedging positions.
        """
        first = self._market_fill(self.account, self.symbol, 1, self.quote)
        second = self._market_fill(self.account, self.symbol, 1, self.quote)
        position = self.account.trades_active[symbol_key(self.symbol)]["NETTING"]
        self.assertEqual(position.UID, first.UID)
        self.assertEqual(position.size, 2)
        self._market_fill(self.account, self.symbol, -1, self.quote)
        self._market_fill(self.account, self.symbol, -1, self.quote)

        key = symbol_key(self.symbol)
        self.assertIsNone(self.account.trades_active[key]["NETTING"])
        self.assertEqual(self.account.trade_count, 0)

    #===============================================================================
    def test_netting_sell_sell_buy_buy_closes_one_position(self):
        """[C] A netting account maintains one position per symbol.
        [S] Apply sell, sell, buy, and buy market orders sequentially.
        [D] The position sizes reduce to zero and the active position is closed.
        [N] The reverse sequence must obey the same netting rules.
        """
        first = self._market_fill(self.account, self.symbol, -1, self.quote)
        second = self._market_fill(self.account, self.symbol, -1, self.quote)
        position = self.account.trades_active[symbol_key(self.symbol)]["NETTING"]
        self.assertEqual(position.UID, first.UID)
        self.assertEqual(position.size, -2)
        self._market_fill(self.account, self.symbol, 1, self.quote)
        self._market_fill(self.account, self.symbol, 1, self.quote)

        key = symbol_key(self.symbol)
        self.assertIsNone(self.account.trades_active[key]["NETTING"])
        self.assertEqual(self.account.trade_count, 0)

    #===============================================================================
    def test_netting_full_hedge_can_open_new_position(self):
        """[C] A netting account has an active one-unit buy position.
        [S] Fully hedge it with a sell, then submit a new buy position.
        [D] The old position closes and the new position becomes active.
        [N] Full hedge must not permanently block future positions.
        """
        first = self._market_fill(self.account, self.symbol, 1, self.quote)
        self._market_fill(self.account, self.symbol, -1, self.quote)
        replacement = self._market_fill(self.account, self.symbol, 1, self.quote)

        active = self.account.trades_active[symbol_key(self.symbol)]["NETTING"]
        self.assertIs(active, replacement)
        self.assertEqual(active.size, 1)
        self.assertNotEqual(active.UID, first.UID)
        self.assertEqual(self.account.trade_count, 1)

    #===============================================================================
    def test_netting_inversion_can_be_closed_after_side_flip(self):
        """[C] A netting account contains a one-unit buy position.
        [S] Apply a larger sell order to invert the position, then delete it.
        [D] The inverted sell position remains addressable and closes normally.
        [N] UID/EID transfer during inversion must not break account lookup.
        """
        first = self._market_fill(self.account, self.symbol, 1, self.quote)
        inverted = self._market_fill(self.account, self.symbol, -2, self.quote)
        active = self.account.trades_active[symbol_key(self.symbol)]["NETTING"]
        close_quote = make_tick(self.symbol, time = time_after(1),
            ask = 98.0, bid = 97.0)
        delete = active.delete(close_quote)

        result = self.account.on_order_delete(delete, close_quote)

        self.assertIs(result, active)
        self.assertEqual(active.status, Trade.Status.CLOSED)
        self.assertEqual(active.side, Trade.Side.SELL)
        self.assertIsNone(self.account.trades_active[
            symbol_key(self.symbol)]["NETTING"])
        self.assertEqual(self.account.trade_count, 0)

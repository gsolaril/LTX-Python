#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
import time
from enum import IntEnum
from pandas import Timestamp, Timedelta
from dataclasses import dataclass, field
from typing import Any, List, Tuple, Dict, Set
from typing import Iterable, Callable, ClassVar
from src.models import Account, Rules, TimeFrame
from src.models import Quote, Candle, Tick
from src.models import OrderCreate, OrderReject
from src.models import OrderModify, OrderDelete
from src.models import Order, OrderDictBySym as OrderDict
from src.models import Trade, TradeDictBySym as TradeDict
from src.models import SymbolDict, QuoteDict
from src.models import StreamingAgent
from src.utils import ClickHouse, Redis, b64
from src.utils import Log

#███████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄
@dataclass#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class ExecReceiver(StreamingAgent):
    time: Timestamp = field(init = True)
    account: Account = field(init = True, default = None)
    symbols: SymbolDict = field(init = True)
    timeframes: Set[str] = field(init = True)
    rules: Rules = field(init = True)
    STREAM_PREFIX: ClassVar[str] = "BTX-"
    STREAM_MIDFIX: ClassVar[str] = "EXEC"
    DEFAULT_LEVERAGE: ClassVar[float] = 100
    DEFAULT_BALANCE: ClassVar[float] = 10000
    XGROUP: ClassVar[str] = Redis.Group.EXEC
    VERBOSE_MAX_ORDERS: ClassVar[str] = "Max orders ({0}) reached for symbol: \"{1}\""

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __post_init__(self):
        if (self.account is None): self.account = Account(id = b64(),
          venue = self.STREAM_PREFIX, balance = self.DEFAULT_BALANCE,
          leverage = self.DEFAULT_LEVERAGE, time = self.time)
        self.stream_prefix = self.STREAM_PREFIX + self.account.id
        self._timeframes = set()
        self._tick_driven = False
        for tf in self.timeframes:
            if (tf == "T1"): self._tick_driven = True
            else: self._timeframes.add(TimeFrame[tf])
        self._min_tf: TimeFrame = min(self._timeframes)
        self.callbacks: dict[Order.Action, Callable] = {
            Order.Action.CREATE: self.order_create,
            Order.Action.MODIFY: self.order_modify,
            Order.Action.DELETE: self.order_delete,
        }
        super().__post_init__()

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def main(self):
        self.quotes = QuoteDict()
        self.n_orders = self.n_trades = 0
        self.orders_active = OrderDict()
        self.trades_active = TradeDict()
        ex_key = (self.stream_prefix, self.STREAM_MIDFIX, self.account.id)
        listen_to = {Redis.join(*ex_key)}
        for symbol in self.symbols.values():
            head = [self.stream_prefix, "DATA", symbol.venue, symbol.symbol]
            for tf in self.timeframes: listen_to.add(Redis.join(*head, tf))

        await Redis.consume(self.process, src = self, xstreams = listen_to, n = 0)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def process(self, stream: str, message_id: str, payload: dict):
        btid, source, stream = stream.split(Redis.SEP, maxsplit = 3)
        if (btid != self.account.id): return Log.error(
            f"Wrong BTID: \"{btid} != {self.account.id}\"")
        responses = None
        if (source == "DATA"):
            responses = await self.on_quote(stream, message_id, payload)
        elif (source == "EXEC"):
            action_str = stream.split(Redis.SEP)[-1]
            action = Order.ACTION[action_str]
            callback = self.callbacks[action]
            responses = await callback(**payload)
        if responses: await self.send_responses(responses)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def on_quote(self, stream: str, message_id: str, payload: dict):
        venue_name, symbol_name, tf = stream.split(Redis.SEP)[2 :]
        symbol_key = (venue_name, symbol_name)
        try: payload["symbol"] = self.symbols[symbol_key]
        except Exception as EXC: return Log.exception(EXC)
        payload["time"] = Redis.id_to_timestamp(message_id)
        if (self._tick_driven and (tf == "T1")): obj = Tick(**payload)
        elif (tf == self._min_tf.name):
            obj = Candle(tf = TimeFrame[tf], **payload)
            if (obj.ob is None) or (obj.ob == obj.oa): obj.ob = obj.oa - self.rules.fixed_spread
            if (obj.hb is None) or (obj.hb == obj.ha): obj.hb = obj.ha + self.rules.fixed_spread
            if (obj.lb is None) or (obj.lb == obj.la): obj.lb = obj.la - self.rules.fixed_spread
            if (obj.cb is None) or (obj.cb == obj.ca): obj.cb = obj.ca + self.rules.fixed_spread
        else: return
        self.time = obj.time_event
        self.quotes[symbol_key] = obj
        responses = await self.clearing(*symbol_key)
        if responses: await self.send_responses(*responses)
         
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def clearing(self, venue: str, symbol: str):
        symbol_key = (venue, symbol)
        quote: Quote = self.quotes[symbol_key]
        stuff_rejected = dict[str, OrderReject]()

        orders_to_clear = dict[str, Order]()
        for UID in list(self.orders_active[symbol_key]):
            order: Order = self.orders_active[symbol_key][UID]
            result = self.account.check_order(order, self.rules, quote)
            if result is None: continue
            elif isinstance(result, OrderReject):
                stuff_rejected[result.UID] = result
            self.orders_active[symbol_key].pop(UID)
            self.account.orders_closed[UID] = order
            self.account.orders_active.pop(UID)
            orders_to_clear[UID] = order
            if self.account.is_hedging:
                trade: Trade = Trade(order, time_place = quote.time_event)
                self.account.trades_active[symbol_key][UID] = trade
            else:
                trade: Trade = self.trades_active[symbol_key]
                if (trade is not None): trade.on_fill(order)
                else: self.trades_active[symbol_key] = Trade(
                  order = order, time_place = quote.time_event)

        trades_to_clear = dict[str, Trade]()
        for UID in list(self.trades_active[symbol_key]):
            trade: Trade = self.trades_active[symbol_key][UID]
            result = self.account.check_trade(trade, self.rules, quote)
            if result is None: continue
            elif isinstance(result, OrderReject):
                stuff_rejected[result.UID] = result
            self.account.trades_closed[UID] = trade
            self.account.trades_active.pop(UID)
            trades_to_clear[UID] = trade
            if self.account.is_hedging:
                self.trades_active[symbol_key].pop(UID)
            else: self.trades_active[symbol_key] = dict()
        
        # TODO: Update GAV, NAV and PNL in the account!

        return [*orders_to_clear.values(),
                *trades_to_clear.values(),
                *stuff_rejected.values()]

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def order_create(self, **payload):
        venue_name = payload.pop("venue")
        symbol_name = payload.pop("symbol")
        symbol_key = (venue_name, symbol_name)
        try: symbol = self.symbols.get(symbol_key)
        except Exception as EXC: return Log.exception(EXC)
        quote = self.quotes[symbol_key]
        request = OrderCreate(account = self.account, symbol = symbol, **payload)
        response = self.account.on_order_create(request, self.rules, quote)
        return [response]

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def order_modify(self, **payload):
        UID = payload.get("UID", None)
        if (UID is None): return OrderReject.from_request(time = self.time,
            reason = OrderReject.Reason.NOT_FOUND, request = payload)
        order: Order = self.orders_active[UID]
        symbol_key = (order.symbol.venue, order.symbol.symbol)
        quote = self.quotes[symbol_key]
        request = OrderModify(account = self.account, UID = UID, **payload)
        response = self.account.on_order_modify(request, self.rules, quote)
        order.on_modify(response)
        return [response]
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def order_delete(self, **payload):
        UID = payload.get("UID", None)
        if (UID is None): return OrderReject.from_request(time = self.time,
            reason = OrderReject.Reason.NOT_FOUND, request = payload)
        order: Order = self.orders_active[UID]
        symbol_key = (order.symbol.venue, order.symbol.symbol)
        quote = self.quotes[symbol_key]
        request = OrderDelete(account = self.account, UID = UID, **payload)
        response = self.account.on_order_delete(request, self.rules, quote)
        return [response]

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    @Redis.stream#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def send_response(self, *responses):
        async for response in responses:
            yield response
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
from src.models import OrderModify, OrderDelete, Message
from src.models import Order, OrderDict as OrderDict
from src.models import Trade, TradeDict as TradeDict
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
    LOG_RESP: ClassVar[dict[type, Callable]] = {
        OrderCreate: Log.info, OrderModify: Log.info, OrderDelete: Log.info,
        OrderReject: Log.warning, Order: Log.success, Trade: Log.success, }
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
        self.quotes: QuoteDict = dict.fromkeys(self.symbols)
        ex_key = (self.stream_prefix, self.STREAM_MIDFIX, self.account.id)
        self.orders, self.trades = dict[str, Order](), dict[str, Trade]()
        for symbol_key, orders in self.account.orders_active.items():
            if (symbol_key in self.symbols): self.orders.update(orders)
            raise KeyError(f"\"{symbol_key}\" not found in symbols")
        for symbol_key, trades in self.account.trades_active.items():
            if (symbol_key in self.symbols): self.trades.update(trades)
            raise KeyError(f"\"{symbol_key}\" not found in symbols")
            self.trades.update(trades)
        for symbol_key in self.symbols:
            if (symbol_key not in self.orders):
                self.orders[symbol_key] = dict()
            if (symbol_key not in self.trades):
                self.trades[symbol_key] = dict()

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
        payload["time"] = Redis.id_to_timestamp(message_id)
        responses = None
        if (source == "DATA"):
            responses = await self.on_quote(stream, payload)
        elif (source == "EXEC"):
            action_str = stream.split(Redis.SEP)[-1]
            action = Order.ACTION[action_str]
            callback = self.callbacks[action]
            responses = await callback(**payload)
        if responses: await self.send_responses(responses)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def on_quote(self, stream: str, payload: dict):
        venue_name, symbol_name, tf = stream.split(Redis.SEP)[2 :]
        symbol_key = (venue_name, symbol_name)
        try: payload["symbol"] = self.symbols[symbol_key]
        except Exception as EXC: return Log.exception(EXC)
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
        await self.send_responses(
            *(await self.order_clearing(*symbol_key)),
            *(await self.trade_clearing(*symbol_key)))

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def order_clearing(self, venue: str, symbol: str):
        symbol_key = (venue, symbol)
        quote: Quote = self.quotes[symbol_key]
        stuff_rejected = dict[str, OrderReject]()
        orders_cleared = dict[str, Order]()
        for UID in sorted(self.account.orders_active[symbol_key]):
            order: Order = self.account.orders_active[symbol_key][UID]
            result = self.account.on_order_filled(order, self.rules, quote)
            if isinstance(result, OrderReject): stuff_rejected[UID] = result
            elif (result is None): continue
            orders_cleared[order.UID] = order
            self.orders.pop(order.UID)
        return [*stuff_rejected.values(),
                *orders_cleared.values()]

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def trade_clearing(self, venue: str, symbol: str):
        symbol_key = (venue, symbol)
        quote: Quote = self.quotes[symbol_key]
        stuff_rejected = dict[str, OrderReject]()
        trades_cleared = dict[str, Trade]()
        for UID in sorted(self.account.trades_active[symbol_key]):
            trade: Trade = self.account.trades_active[symbol_key][UID]
            result = self.account.on_trade_closed(trade, self.rules, quote)
            if isinstance(result, OrderReject): stuff_rejected[UID] = result
            elif (result is None): continue
            trades_cleared[trade.UID] = trade
            self.trades.pop(trade.UID)
        return [*stuff_rejected.values(),
                *trades_cleared.values()]

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def order_create(self, **payload):
        venue_name = payload.pop("venue", None)
        symbol_name = payload.pop("symbol", None)
        symbol_key = (venue_name, symbol_name)
        symbol_str = str.join(" ", symbol_key)
        symbol = self.symbols.get(symbol_key, None)
        quote = self.quotes.get(symbol_key, None)
        if (symbol is None) or (quote is None):
            reason = OrderReject.Reason.UNKNOWN_SYMBOL
            message = reason.value.format(subject = "OrderCreate", 
                symbol = symbol_str,summary = payload.get("UID", None))
            response = OrderReject(account = self.account, **payload,
                reason = reason, message = message, time = self.time)
        else:
            quote = self.quotes[symbol_key]
            request = Order.create(self.account, symbol = symbol, **payload)
            response = self.account.on_order_create(request, self.rules, quote)
        return [response]

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def check_order_exists(self,
          RequestType: type, **kwargs):
        if (UID := kwargs.pop("UID", None)):
            if (UID in self.orders): return self.orders[UID]
            if (UID in self.trades): return self.trades[UID]
        subject = RequestType.__name__
        reason = OrderReject.Reason.UNKNOWN_UID
        message: str = reason.value.format(subject = subject,
            summary = Message.VERBOSE_REPR.format(UID = UID))
        return OrderReject(account = self.account, UID = UID,
            reason = reason, message = message, time = self.time)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def order_modify(self, **payload):
        obj = await self.check_order_exists(OrderModify, **payload)
        if isinstance(obj, OrderReject): return [obj]
        quote: Quote = self.quotes[(obj.symbol.venue, obj.symbol.symbol)]
        request = OrderModify(account = self.account, **payload)
        response = self.account.on_order_modify(request, quote)
        return [response]
        
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def order_delete(self, **payload):
        obj = await self.check_order_exists(OrderDelete, **payload)
        if isinstance(obj, OrderReject): return [obj]
        quote: Quote = self.quotes[(obj.symbol.venue, obj.symbol.symbol)]
        request = OrderDelete(account = self.account, **payload)
        response = self.account.on_order_delete(request, quote)
        return [response]

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    @Redis.stream#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def send_responses(self, *responses):
        async for response in responses:
            log_resp = self.LOG_RESP.get(
                type(response), Log.debug)
            log_resp(response)
            yield response
        yield self.account


#███████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄
@dataclass#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class TestExecReceiver(ExecReceiver):
    pass
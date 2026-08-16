#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
import time
from enum import IntEnum
from pandas import Timestamp, Timedelta
from dataclasses import dataclass, field
from typing import List, Tuple, Dict, Set
from typing import Iterable, Callable, ClassVar
from src.models import Account, Rules, Order, Trade
from src.models import OrderCreate, OrderModify, OrderDelete
from src.models import TimeFrame, Candle, Tick, SymbolDict
from src.models import StreamingAgent
from src.utils import ClickHouse, Redis, b64
from src.utils import Log

#███████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
QuoteDict = dict[Tuple[str, str], dict[str, Tick | Candle]]
OrderDict = dict[Tuple[str, str], dict[str, Order]]
TradeDict = dict[Tuple[str, str], dict[str, Trade]]

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
        super().__post_init__()

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def main(self):
        self.quotes = QuoteDict()
        self.n_orders = self.n_trades = 0
        self.orders_active, self.orders_closed = OrderDict(), OrderDict()
        self.trades_active, self.trades_closed = TradeDict(), TradeDict()
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
        response = None
        if (source == "DATA"):
            response = await self.on_quote(stream, message_id, payload)
        elif (source == "EXEC"):
            action = stream.split(Redis.SEP)[-1]
            if (action == "create"): response = await self.order_create(payload)
            elif (action == "modify"): response = await self.order_modify(payload)
            elif (action == "delete"): response = await self.order_delete(payload)
        if response: await self.send_response(response)

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
        response = await self.clearing(*symbol_key)
        if response: await self.send_response(response)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def order_create(self, payload: dict):
        venue_name = payload.pop("venue")
        symbol_name = payload.pop("symbol")
        symbol_key = (venue_name, symbol_name)
        try: symbol = self.symbols.get(symbol_key)
        except Exception as EXC: return Log.exception(EXC)
        order = OrderCreate(symbol = symbol, account = self.account, **payload)
        
        response: Order = Order(order)
        self.account.
        self.active[symbol][order.UID] = response
        return response

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def order_modify(self, payload: dict): ...
    def order_delete(self, payload: dict): ...
         
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def clearing(self, venue: str, symbol: str):
        to_cancel = dict[str, Order]()
        to_execute = dict[str, Order]()
        symbol_key = (venue, symbol)
        for order in self.orders_active[symbol_key]:
            pass

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    @Redis.stream#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def send_response(self, response: Order):
        ... # Response should have the right "__dict__"
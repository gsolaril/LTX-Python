#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
import time
from enum import IntEnum
from pandas import Timestamp, Timedelta
from dataclasses import dataclass, field
from typing import List, Tuple, Dict, Set
from typing import Iterable, Callable, ClassVar
from src.models import Account, Order, Response, Position
from src.models import TimeFrame, Candle, Tick, SymbolDict
from src.models import StreamingAgent
from src.utils import ClickHouse, Redis, b64
from src.utils import Log

#███████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
QuoteDict = dict[Tuple[str, str], dict[str, Tick | Candle]]
OrderDict = dict[Tuple[str, str], dict[str, Response]]
PosDict = dict[Tuple[str, str], dict[str, Position]]
#▄▄▄▄▄▄▄▄▄
@dataclass
class Rules:
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    class AccountType(IntEnum): NETTING, HEDGING, DUAL = 0, 1, 2
    acc_type: AccountType = field(default = AccountType.NETTING)
    commission: float = field(default = 0.0)
    max_drawdown: float = field(default = 1.0)
    max_leverage: float = field(default = 100.0)
    max_sizer_up: float = field(default = None)
    max_sizer_dn: float = field(default = None)
    max_freq: Timedelta = field(default = None)
    slippage_mn: float = field(default = 0.0)
    slippage_sd: float = field(default = 1.0)
    fixed_spread: float = field(default = 2.0)
    max_orders: int = field(default = 100)

#▄▄▄▄▄▄▄▄▄▄▄
@dataclass#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class ExecReceiver(StreamingAgent):
    STREAM_PREFIX: ClassVar[str] = "BTX-"
    STREAM_MIDFIX: ClassVar[str] = "EXEC"
    DEFAULT_BALANCE: ClassVar[float] = 100000
    XGROUP: ClassVar[str] = Redis.Group.EXEC
    time: Timestamp = field(init = True)
    account: Account = field(init = True, default = None)
    symbols: SymbolDict = field(init = True)
    timeframes: Set[str] = field(init = True)
    rules: Rules = field(init = True)

    VERBOSE_MAX_ORDERS: ClassVar[str] = "Max orders ({0}) reached for symbol: \"{1}\""

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __post_init__(self):
        if (self.account is None): self.account = Account(id = b64(),
            venue = self.STREAM_PREFIX, leverage = self.rules.max_leverage,
            balance = self.DEFAULT_BALANCE, last_updated = self.time)
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
        self.orders_active, self.orders_closed = OrderDict(), OrderDict()
        self.positions_active, self.positions_closed = PosDict(), PosDict()
        listen_to = {Redis.join(self.stream_prefix, self.STREAM_MIDFIX, self.btid)}

        for symbol in self.symbols.values():
            head = [self.stream_prefix, "DATA", symbol.venue, symbol.symbol]
            for tf in self.timeframes: listen_to.add(Redis.join(*head, tf))
        
        await Redis.consume(self.process, src = self, xstreams = listen_to, n = 0)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def process(self, stream: str, message_id: str, payload: dict):
        btid, source, stream = stream.split(Redis.SEP, maxsplit = 3)
        if (btid != self.account.id): Log.error(
            f"Wrong BTID: \"{btid} != {self.account.id}\"")
        if (source == "DATA"): await self.process_data(payload, message_id, stream)
        elif (source == "EXEC"):
            action = stream.split(Redis.SEP)[-1]
            if (action == "create"): response = await self.order_create(payload)
            elif (action == "modify"): response = await self.order_modify(payload)
            elif (action == "delete"): response = await self.order_delete(payload)
            if response: await self.send_response(response)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def order_create(self, payload: dict):
        venue_name = payload.pop("venue")
        symbol_name = payload.pop("symbol")
        symbol_key = (venue_name, symbol_name)
        try: symbol = self.symbols.get(symbol_key)
        except Exception as EXC: return Log.exception(EXC)
        order = Order(symbol = symbol, account = self.account, **payload)
        if (len(self.active[symbol]) >= self.rules.max_orders):
            return Log.error(self.VERBOSE_MAX_ORDERS.format(
                self.rules.max_orders, symbol))
        response: Response = Response(order)
        self.active[symbol][order.UID] = response
        return response

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def order_modify(self, payload: dict): ...
    def order_delete(self, payload: dict): ...

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def process_data(self, payload: dict, message_id: str, stream: str):
        venue_name, symbol_name, tf = stream.split(Redis.SEP)[2 :]
        symbol_key = (venue_name, symbol_name)
        try: payload["symbol"] = self.symbols[symbol_key]
        except Exception as EXC: return Log.exception(EXC)
        payload["time"] = Redis.id_to_timestamp(message_id)
        if (self._tick_driven and (tf == "T1")): obj = Tick(**payload)
        elif (tf == self._min_tf.name):
            if payload["ob"] is None: payload["ob"] = payload["oa"] - self.rules.fixed_spread
            if payload["hb"] is None: payload["hb"] = payload["ha"] + self.rules.fixed_spread
            if payload["lb"] is None: payload["lb"] = payload["la"] - self.rules.fixed_spread
            if payload["cb"] is None: payload["cb"] = payload["ca"] + self.rules.fixed_spread
            obj = Candle(tf = TimeFrame[tf], **payload)
        else: return
        self.time = obj.time_event
        self.quotes[symbol_key] = obj
        response = await self.clearing(*symbol_key)
        if response: await self.send_response(response)
         
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def clearing(self, venue: str, symbol: str):
        to_cancel = dict[str, Response]()
        to_execute = dict[str, Response]()
        for order in self.orders_active[(venue, symbol)]:
            pass

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    @Redis.stream#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def send_response(self, response: Response):
        ... # Response should have the right "__dict__"
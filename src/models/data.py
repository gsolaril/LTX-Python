#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
import asyncio
from pandas import Timestamp
from sortedcontainers import SortedDict
from dataclasses import dataclass, field, asdict
from typing import Any, ClassVar, Callable
from misc import Account, Symbol, TimeFrame

#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class Queue(asyncio.Queue):
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __init__(self, maxsize: int,
            checkpoints: dict[float, Callable] = None):
        self._checkvalues = list(checkpoints)
        self._checkfunctions = list[Callable]()
        super().__init__(maxsize = maxsize)
        if checkpoints is not None:
            for value, func in checkpoints.items():
                if (value > 1.0): value /= maxsize
                self._checkvalues.append(value)
                self._checkfunctions.append(func)
        self.last_checkpoint = 0
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def put(self, item: Any): await super().put(item); self._recheck()
    def put_nowait(self, item: Any): super().put_nowait(item); self._recheck()
    async def get(self): item = await super().get(); self._recheck(); return item
    def get_nowait(self): item = super().get_nowait(); self._recheck(); return item   
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def _recheck(self):
        value = self.qsize() / self.maxsize
        index_next = self.last_checkpoint
        index_lower = self.last_checkpoint
        index_upper = self.last_checkpoint + 1
        value_lower = self._checkvalues[index_lower]
        value_upper = self._checkvalues[index_upper]
        if (value_upper <= value): index_next += 1
        elif (value < value_lower): index_next -= 1
        if (value >= 1) and (1 not in self._checkvalues):
            print("Warning: no checkpoint for full queue!")
        if (index_next != self.last_checkpoint):
            self._checkfunctions[index_next](value)
            self.last_checkpoint = index_next

#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄
@dataclass#█▄▄▄
class BasePoint:
    time: Timestamp = field(kw_only = True, default = None)
    dus: int = field(kw_only = True, default = None)
    STREAM_KEY: ClassVar[str] = ...
    INDEX_KEYS: ClassVar[list[str]] = ...
    CACHE_KEYS: ClassVar[list[str]] = ...
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __post_init__(self):
        now = Timestamp.now("UTC")
        if self.time is None: self.time = now
        delay_s = (now - self.time).total_seconds()
        if (self.dus is None): self.dus = int(delay_s * 1e6)
    #▄▄▄▄▄▄▄▄▄▄
    @property#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def time_us(self): return int(self.time.timestamp() * 1e6)
    #▄▄▄▄▄▄▄▄▄▄
    @property#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __dict__(self): return {"stream": self.STREAM_KEY,
            "time": self.time_us, "payload": asdict(self)}
#▄▄▄▄▄▄▄▄▄▄▄
@dataclass#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class DataPoint(BasePoint):
    index: str = field(kw_only = True)
    data: dict = field(kw_only = True)
    #▄▄▄▄▄▄▄▄▄▄
    @property#█▄▄▄▄▄▄▄
    def __dict__(self):
        payload = asdict(self)
        index, data = payload.pop("index"), payload.pop("data")
        return {"stream": self.STREAM_KEY + "|" + index,
                "time": self.time_us, "payload": data}
#▄▄▄▄▄▄▄▄▄▄▄
@dataclass#█▄▄▄▄▄▄▄▄▄▄
class Quote(BasePoint):
    symbol: Symbol = field(kw_only = True)
    STREAM_KEY: ClassVar[str] = "{venue}|{symbol}|{tf}"
    INDEX_KEYS: ClassVar[list[str]] = ["venue", "symbol", "time"]

#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄
@dataclass#█▄▄▄▄▄▄▄▄▄▄▄▄
class Balance(BasePoint):
    account: Account = field(kw_only = True)
    symbol: Symbol = field(kw_only = True, default = None)
    balance: float = field(kw_only = True)
    equity: float = field(kw_only = True, default = None)
    margin: float = field(kw_only = True, default = None)
    STREAM_KEY: ClassVar[str] = "{venue}|{account_id}|{symbol}"
    INDEX_KEYS: ClassVar[list[str]] = ["venue", "account_id", "symbol", "time"]
    BASIC_KEYS: ClassVar[list[str]] = ["balance", "equity", "margin"]
    CACHE_KEYS: ClassVar[list[str]] = [*BASIC_KEYS, "uPNL", "uPRC", "mPRC", "dus"]
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __post_init__(self):
        super().__post_init__()
        if self.symbol is not None:
            error = (f"Symbol venue (\"{self.symbol.venue}\") "
            f"must match account's (\"{self.account.venue}\")")
            assert (self.symbol.venue == self.account.venue), error
        for key in self.BASIC_KEYS:
            setattr(self.account, key, getattr(self, key))
    #▄▄▄▄▄▄▄▄▄▄
    @property#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def uPNL(self): return account.uPNL
    #▄▄▄▄▄▄▄▄▄▄
    @property#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def uPRC(self): return account.uPRC
    #▄▄▄▄▄▄▄▄▄▄
    @property#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def mPRC(self): return account.mPRC
    #▄▄▄▄▄▄▄▄▄▄
    @property#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def ppal(self): return account.ppal
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __repr__(self): return self.account.inline(
                type = "Balance", time = self.time)
    #▄▄▄▄▄▄▄▄▄▄
    @property#█▄▄▄▄▄▄▄
    def __dict__(self):
        stream_key = self.STREAM_KEY.format(
          venue = self.account.venue, account_id = self.account.id,
          symbol = "$" if self.symbol is None else self.symbol.symbol)
        payload = {key: getattr(self, key) for key in self.CACHE_KEYS}
        return {"stream": stream_key, "time": self.time_us, "payload": payload}

#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄
@dataclass#█▄▄▄▄▄
class Tick(Quote):
    pa: float = field(kw_only = True, default = None)
    qa: float = field(kw_only = True, default = None)
    pb: float = field(kw_only = True, default = None)
    qb: float = field(kw_only = True, default = None)
    CACHE_KEYS: ClassVar[list[str]] = ["pa", "qa", "pb", "qb", "dus"]

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __post_init__(self):
        super().__post_init__()
        self.pa, self.pb = float(self.pa), float(self.pb)
        self.qa, self.qb = float(self.qa), float(self.qb)
        self.error = (self.pa * self.qa == 0) | (self.pb * self.qb == 0)
        self.qavga = self.qavgb = self.pavga = self.pavgb = None
    
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __repr__(self):
        time = f"{self.time:%Y/%m/%d %H:%M:%S.%f}"
        return f"Tick({self.symbol!r} @ {time} | " \
          f"A:{self.pa}/{self.qa}, B:{self.pb}/{self.qb})"

    #▄▄▄▄▄▄▄▄▄▄
    @property#█▄▄▄▄▄▄▄
    def __dict__(self):
        stream_key = self.STREAM_KEY.format(tf = "T1",
            venue = self.symbol.venue, symbol = self.symbol.symbol)
        payload = {key: getattr(self, key) for key in self.CACHE_KEYS}
        return {"stream": stream_key, "time": self.time_us, "payload": payload}

#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄
@dataclass#█▄▄▄▄▄▄▄
class Candle(Quote):
    tf: TimeFrame = field(kw_only = True)
    volume: int = field(kw_only = True, default = None)
    oa: float = field(kw_only = True, default = None)
    ha: float = field(kw_only = True, default = None)
    la: float = field(kw_only = True, default = None)
    ca: float = field(kw_only = True, default = None)
    ob: float = field(kw_only = True, default = None)
    hb: float = field(kw_only = True, default = None)
    lb: float = field(kw_only = True, default = None)
    cb: float = field(kw_only = True, default = None)
    STREAM_KEY: ClassVar[str] = "{venue}|{symbol}|{tf}"
    INDEX_KEYS: ClassVar[list[str]] = ["tf"] + Quote.INDEX_KEYS.copy()
    CACHE_KEYS: ClassVar[list[str]] = ["oa", "ha", "la", "ca", "ob", "hb", "lb", "cb", "volume", "dus"]

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __repr__(self):
        if self.tf.is_unit("S"): interval = f"{self.time:%Y/%m/%d %H:%M:%S}-{self._time_close:%S}"
        elif self.tf.is_unit("M"): interval = f"{self.time:%Y/%m/%d %H:%M}-{self._time_close:%H:%M}"
        elif self.tf.is_unit("H"): interval = f"{self.time:%Y/%m/%d %H:%M}-{self._time_close:%H:%M}"
        elif self.tf.is_unit("D"): interval = f"{self.time:%Y/%m/%d}-{self._time_close:%Y/%m/%d}"
        else: interval = f"{self.time:%Y/%m/%d %H:%M:%S.%f}-{self._time_close:%Y/%m/%d %H:%M:%S.%f}"
        return f"Candle({self.symbol!r} @ {interval} ({self.tf.name}) | " \
            f"O:{self.oa}, H:{self.ha}, L:{self.la}, C:{self.ca} | V:{self.volume})"
        
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __post_init__(self):
        super().__post_init__()
        self._time_ltick = self.time
        self.time = self.time.floor(self.tf.value)
        self._time_close = self.time + self.tf.value
        if (self.oa is None): self.oa = float(self.oa)
        if (self.ob is None): self.ob = float(self.ob)
        if (self.ha is None): self.ha = float(self.ha)
        if (self.la is None): self.la = float(self.la)
        if (self.ca is None): self.ca = float(self.ca)
        if (self.hb is None): self.hb = float(self.hb)
        if (self.lb is None): self.lb = float(self.lb)
        if (self.cb is None): self.cb = float(self.cb)
        if not self.volume: self.volume = 0
        self.volume = int(self.volume)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def on_tick(self, tick: Tick):
        if (tick.time < self.time): return
        if (tick.time < self._time_ltick): return
        if (tick.time >= self._time_close): return
        if (tick.symbol != self.symbol): return
        if tick.error: return
        self._time_ltick = tick.time
        self.volume = self.volume + 1
        if (self.oa is None): self.oa = tick.pa
        if (self.ha is None): self.ha = tick.pa
        if (self.la is None): self.la = tick.pa
        if (self.ob is None): self.ob = tick.pb
        if (self.hb is None): self.hb = tick.pb
        if (self.lb is None): self.lb = tick.pb
        if tick.pa:
            self.ha = max(self.ha, tick.pa)
            self.la = min(self.la, tick.pa)
            self.ca = tick.pa
        if tick.pb:
            self.hb = max(self.hb, tick.pb)
            self.lb = min(self.lb, tick.pb)
            self.cb = tick.pb
    
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def on_candle_lower(self, candle: "Candle"):
        if (candle.time < self.time): return
        if (candle.symbol != self.symbol): return
        if (candle._time_ltick < self._time_ltick): return
        if (candle._time_close > self._time_close): return
        self._time_ltick = candle._time_ltick
        self.volume = self.volume + candle.volume
        if (self.oa is None): self.oa = candle.oa
        if (self.ob is None): self.ob = candle.ob
        if (self.ha is None): self.ha = candle.ha
        if (self.la is None): self.la = candle.la
        if (self.hb is None): self.hb = candle.hb
        if (self.lb is None): self.lb = candle.lb
        if candle.ha: self.ha = max(self.ha, candle.ha)
        if candle.la: self.la = min(self.la, candle.la)
        if candle.hb: self.hb = max(self.hb, candle.hb)
        if candle.lb: self.lb = min(self.lb, candle.lb)
        if candle.ca: self.ca = candle.ca
        if candle.cb: self.cb = candle.cb

    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def on_candle_prev(cls, candle: "Candle"):
        return cls(tf = candle.tf, symbol = candle.symbol, volume = 0,
          ca = candle.ca, cb = candle.cb, time = candle._time_close,
          oa = None, ob = None)
    #▄▄▄▄▄▄▄▄▄▄
    @property#█▄▄▄▄▄▄▄
    def __dict__(self):
        stream_key = self.STREAM_KEY.format(tf = self.tf.name,
            venue = self.symbol.venue, symbol = self.symbol.symbol)
        payload = {key: getattr(self, key) for key in self.CACHE_KEYS}
        return {"stream": stream_key, "time": self.time_us, "payload": payload}

#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
if (__name__ == "__main__"):

    symbol = Symbol(venue = "BINANCE", symbol = "BTCUSDT", quote = "USDT",
        base = "BTC", id = "BINANCE_BTCUSDT", min_price_diff = 1e-2, min_order_size = 1)
    candle = Candle(tf = TimeFrame.M4, symbol = symbol, time = Timestamp.now("UTC"),
        oa = 10000, ha = 10001, la = 9999, ca = 10002, volume = 10000)
    print(candle)
    print(candle.__dict__)

    # Example: Create and display a Balance point

    account = Account(id="TEST_ACC1", venue="BINANCE")
    balance = Balance(account=account, balance=1000.0, equity=1200.0, margin=100.0, time=Timestamp.now("UTC"))
    print(balance)
    print(balance.__dict__)
    print(f"uPNL: {balance.uPNL}")
    print(f"uPRC: {balance.uPRC}")
    print(f"mPRC: {balance.mPRC}")
    print(account)
    print(account.__dict__)
    print(f"uPNL: {account.uPNL}")
    print(f"uPRC: {account.uPRC}")
    print(f"mPRC: {account.mPRC}")

    print(Quote.STREAM_KEY)
    print(Tick.STREAM_KEY)
    print(Candle.STREAM_KEY)
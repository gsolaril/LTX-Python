#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
import numpy
from dataclasses import asdict, dataclass
from collections import defaultdict, deque
from typing import Any, Dict, List, Set
from collections import OrderedDict
from pandas import Timestamp, Timedelta
from pandas import Series, DataFrame, concat
from misc import Symbol, TimeFrame
from src.utils import Log
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄
@dataclass
class Order:
    venue: str; symbol: str; size: float
    price: float = None; comment: str = None
    expiration: Timestamp = None
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __post_init__(self):
        self.symbol = self.symbol.upper()
        self.symbol = self.symbol.replace("/", "")
        self.symbol = self.symbol.replace(":", "")
        assert (self.size != 0), "Order size cannot be 0"
        self.side = "BUY" if (self.size > 0) else "SELL"
        self.type = "LIMIT" if self.price else "MARKET"
        self.mode = "GTC" if self.price else "IOC"
        if not self.comment: self.comment = ""
        
        self.time = Timestamp.utcnow()
        ts = int(self.time.timestamp() * 1e6)
        self.UID = numpy.base_repr(ts, base = 36).upper()
        self._check_expired(self.time)
        
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def _check_expired(self, time: Timestamp):
        if self.expiration is not None:
            error = "Order already expired..." \
            f"\n => (NOW) {time:%Y/%m/%d %H:%M:%S} > " \
            f"(EXP) {self.expiration:%Y/%m/%d %H:%M:%S}"
            assert time < self.expiration, error

    #▄▄▄▄▄▄▄▄▄▄
    @property#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __dict__(self): return asdict(self)
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __repr__(self): return self.inline()
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def inline(self):
        verbose = "Order({venue} {symbol}, S{size:+} P{price} @ "
        verbose += "{time:%Y/%m/%d %H:%M:%S.%f}, E+{DE:.1f}s | {UID})"
        if self.expiration is None: expires_in = numpy.inf
        else: expires_in = Timedelta.total_seconds(self.expiration - self.time)
        return verbose.format(**self.__dict__, UID = self.UID,
                             time = self.time, DE = expires_in)
#▄▄▄▄▄▄▄▄▄▄▄
@dataclass#█▄▄▄▄▄▄▄▄▄
class Response(Order):
    status: str = None
    EID: str = None; UID: str = None
    time_place: Timestamp = None

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __init__(self, order: Order, **kwargs):
        super().__init__(**order.__dict__)
        self.time_order = order.time
        self.size = float(kwargs.get("size", order.size))
        self.price = float(kwargs.get("price", order.price))
        if self.time_place is None:
            self.time_place = Timestamp.utcnow()
        
        self._check_expired(self.time_order)
        self._check_expired(self.time_place)
        self.status = kwargs["status"]
        self.EID = kwargs["EID"]
        self.UID = order.UID
        self.time = order.time

    #▄▄▄▄▄▄▄▄▄▄
    @property#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __dict__(self): return {**asdict(self),
        "time_order": self.time_order}
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __repr__(self): return self.inline(4)
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def inline(self, nlspace: int = None):
        if (nlspace is None): sep = ", IDs: "
        else: sep = "\n" + " " * nlspace
        verbose = "Order({venue} {symbol}, S{size:+} P{price} @ "
        if self.expiration is None: expires_in = numpy.inf
        else: expires_in = Timedelta.total_seconds(self.expiration - self.time_order)
        delay = 1e6 * Timedelta.total_seconds(self.time_place - self.time_order)
        verbose += "{time:%Y/%m/%d %H:%M:%S.%f}, D+{DD:.0f}µs, E+{DE:.1f}s{sep}{UID} {EID} | {status})"
        return verbose.format(**self.__dict__, sep = sep,
          time = self.time_order, DD = delay, DE = expires_in)

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄
@dataclass
class Tick:
    venue: str; symbol: str
    pa: float; qa: float
    pb: float; qb: float
    time: Timestamp = None

    INDEX = ["venue", "symbol", "time"]
    #▄▄▄▄▄▄▄▄
    @property
    def __dict__(self):
        order = "time venue symbol pa qa pb qb pavga qavga pavgb qavgb delay error"
        return {key: self.__getattribute__(key) for key in order.split(" ")}

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __post_init__(self):
        self.pa, self.pb = float(self.pa), float(self.pb)
        self.qa, self.qb = float(self.qa), float(self.qb)
        self.error = (self.pa * self.qa == 0) | (self.pb * self.qb == 0)
        self.qavga = self.qavgb = self.pavga = self.pavgb = None
        if (self.time is None): self.time = Timestamp.utcnow()
        delay = Timestamp.utcnow() - self.time
        self.delay = int(delay.total_seconds() * 1e6)
    
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __repr__(self):
        time = f"{self.time:%Y/%m/%d %H:%M:%S.%f}"
        return f"Tick(@ {time}) | {self.venue}.{self.symbol} | " \
                f"A:{self.pa}/{self.qa}, B:{self.pb}/{self.qb})"
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄
@dataclass
class Candle:
    tf: TimeFrame; venue: str; symbol: str; volume: int = None; 
    oa: float = None; ha: float = None; la: float = None; ca: float = None
    ob: float = None; hb: float = None; lb: float = None; cb: float = None
    time: Timestamp = None

    INDEX = ["venue", "symbol", "tf", "time"]
    #▄▄▄▄▄▄▄▄
    @property
    def __dict__(self):
        order = "time tf venue symbol volume oa ha la ca ob hb lb cb"
        return {key: self.__getattribute__(key) for key in order.split(" ")}

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __repr__(self):
        if self.tf.is_unit("S"): interval = f"{self.time:%Y/%m/%d %H:%M:%S}-{self._time_close:%S}"
        elif self.tf.is_unit("M"): interval = f"{self.time:%Y/%m/%d %H:%M}-{self._time_close:%H:%M}"
        elif self.tf.is_unit("H"): interval = f"{self.time:%Y/%m/%d %H:%M}-{self._time_close:%H:%M}"
        elif self.tf.is_unit("D"): interval = f"{self.time:%Y/%m/%d}-{self._time_close:%Y/%m/%d}"
        else: interval = f"{self.time:%Y/%m/%d %H:%M:%S.%f}-{self._time_close:%Y/%m/%d %H:%M:%S.%f}"
        return f"Candle({self.tf.name} @ {interval} | {self.venue}.{self.symbol} | " \
            f"O:{self.oa}, H:{self.ha}, L:{self.la}, C:{self.ca} | V:{self.volume})"
        
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __post_init__(self):
        if self.time is None:
            self.time = Timestamp.utcnow()
        self._time_ltick = self.time
        self.time = self.time.floor(self.tf.value)
        self._time_close = self.time + self.tf.value
        if not self.volume: self.volume = 0

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def on_tick(self, tick: Tick):
        if (tick.time < self.time): return
        if (tick.time < self._time_ltick): return
        if (tick.time >= self._time_close): return
        if (tick.venue != self.venue): return
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
        if (candle.venue != self.venue): return
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
        return cls(tf = candle.tf, venue = candle.venue, symbol = candle.symbol,
          ca = candle.ca, cb = candle.cb, volume = 0, time = candle._time_close,
          oa = None, ob = None)

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄
class Bundle:

    MIN_N_TICKSPS, MAX_N_TICKSPS = 5_000, 100_000
    MIN_N_CANDLES, MAX_N_CANDLES = 10_000, 1_000_000
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __init__(self, ntps: int = None, ncpf: int = None,
        preload: dict = None, ignore: Set[TimeFrame] = None):

        if ignore is None:
            ignore = set()
        self.ignore_tfs = ignore.copy()

        self._start = Timestamp.utcnow()
        self._tick_first = self._tick_last = None
        if ntps is None: ntps = self.MIN_N_TICKSPS
        if ncpf is None: ncpf = self.MIN_N_CANDLES
        self._n_ticks_max = int(ntps * ncpf / 100)
        self._NT, self._NC = int(ntps), int(ncpf)

        self._count = dict()
        self._ticks = dict()
        self._candles = dict()
        for tf in TimeFrame:
            self._candles[tf] = dict()
        if preload is None: preload = dict()

        symbols: dict[tuple, deque] = None
        for tf, symbols in preload.items():
            for key, candles in symbols.items():
                if key not in self._candles[tf]:
                    self._candles[tf][key] = self._queue(self._NC)
                self._candles[tf][key] = candles.copy()

    #▄▄▄▄▄▄▄▄▄▄
    @property#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def symbols(self): return sorted(self._count)
    #▄▄▄▄▄▄▄▄▄▄
    @property#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def since_start(self): return Timestamp.utcnow() - self._start
    #▄▄▄▄▄▄▄▄▄▄
    @property#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def since_tick_1(self): return Timestamp.utcnow() - self._tick_first.time
    #▄▄▄▄▄▄▄▄▄▄
    @property#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def since_tick_n(self): return Timestamp.utcnow() - self._tick_last.time

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def _queue(self, len: int = None):
        if not len: len = self._NC
        len = min(len, self._NT)
        return deque(maxlen = len)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def on_tick(self, tick: Tick):

        self._tick_last = tick
        if self._tick_first is None:
            self._tick_first = tick

        key = (tick.venue, tick.symbol)

        if key not in self._ticks:
            self._ticks[key] = OrderedDict()
            self._count[key] = 0
            for tf in TimeFrame:
                self._candles[tf][key] = self._queue(self._NC)

        close_at = tick.time + TimeFrame.MIN.value
        close_at = close_at.floor(TimeFrame.MIN.value)

        if close_at not in self._ticks[key]:
            self._ticks[key][close_at] = self._queue(self._NT)

        self._ticks[key][close_at].append(tick)
        self._count[key] = self._count[key] + 1

        if (self._count[key] >= self._n_ticks_max):
            candles: OrderedDict = self._ticks[key]
            n_drop = len(candles.popitem(last = False)[1])
            self._count[key] = self._count[key] - n_drop

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def on_candle(self, candle: Candle):
        key = (candle.venue, candle.symbol)
        if key not in self._candles[candle.tf]:
            self._candles[candle.tf][key] = self._queue(self._NC)
        self._candles[candle.tf][key].append(candle)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def resample_ticks(self, time: Timestamp = None):
        if TimeFrame.MIN in self.ignore_tfs: return
        if (time is None): time = Timestamp.utcnow()
        closed_at = time.floor(TimeFrame.MIN.value)
        opened_at = closed_at - TimeFrame.MIN.value

        candles: OrderedDict = None
        for key, candles in self._ticks.items():
            ticks: deque = candles.get(closed_at, deque())
            candle = Candle(TimeFrame.MIN, *key, time = opened_at)
            for tick in ticks: candle.on_tick(tick)
            self.on_candle(candle)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def resample_candles(self, time: Timestamp = None):
        if (time is None): time = Timestamp.utcnow()

        tf_opt: TimeFrame = None; tf_upd: TimeFrame = None
        for tf_upd, tf_opt in TimeFrame.updatable(time):
            if tf_upd in self.ignore_tfs: continue
            time_candle = time - tf_upd.value
            for key in self.symbols:
                candle = Candle(tf_upd, *key, time = time_candle)
                for n in range(- int(tf_upd / tf_opt), 0):
                    try: candle_lower = self._candles[tf_opt][key][n]
                    except IndexError: continue
                    candle.on_candle_lower(candle_lower)
                self.on_candle(candle)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __repr__(self):

        df = dict()
        df_lines = list[str]()
        keys: dict = None
        for tf, keys in self._candles.items():
            df[tf] = dict()
            for key, candles in keys.items():
                df[tf][key] = len(candles)

        df = DataFrame.from_dict(df, orient = "index")
        report_lines = [f"  Time of start:      {self._start:%H:%M:%S} ({self.since_start} ago)"]

        if df.empty:
            report_lines.append("\n    ||| No data yet ||| ")
        else:
            report_lines.append(f"  Time of last tick:  {self._tick_last.time:%H:%M:%S} ({self.since_tick_n} ago)")
            report_lines.append(f"  Time of first tick: {self._tick_first.time:%H:%M:%S} ({self.since_tick_1} ago)")
            df.columns = df.columns.rename(Tick.INDEX[: 2])
            df.loc["*ticks"] = Series(self._count)
            df["*total"] = df.sum(axis = "columns")
            df = concat((df.iloc[-1:], df.iloc[:-1]))
            df_lines = df.to_string().split("\n")
            for nr, row in enumerate(df_lines):
                df_lines[nr] = f"    | {row} | "
            top, bottom = "_", "\u203E"
            df_lines.insert(0, " " * 4 + (len(row) + 4) * top)
            df_lines.append(" " * 4 + (len(row) + 4) * bottom)

        return str.join("\n", [*report_lines, *df_lines, ""])

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def get(self, tf: Set[TimeFrame] = None, symbol: Set[tuple] = None, until: Timestamp = None, **kwargs):

        if not symbol: symbol = {*self.symbols}
        elif isinstance(symbol, tuple): symbol = {symbol}
        elif isinstance(symbol, str): symbol = {symbol}

        if until is None:
            until = Timestamp.max.tz_localize("UTC")
        n = kwargs.get("n", self._NC)
        since = Timestamp.min.tz_localize("UTC")
        since = getattr(self._tick_first, "time", since)
        since: Timestamp = kwargs.get("since", since)

        if tf is Tick:
            index = Tick.INDEX.copy()
            gen = self.gen_ticks(symbol, until, since)
        else:
            index = Candle.INDEX.copy()
            if not tf: tf = {*self._candles.keys()}
            elif isinstance(tf, str): tf = {TimeFrame[tf]}
            elif isinstance(tf, TimeFrame): tf = {tf}
            gen = self.gen_candles(tf, symbol, until, since, n)

        df = DataFrame(gen)
        if not df.empty:
            df = df.set_index(index)
        return df.sort_index()

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def gen_candles(self, tfs: Set, symbols: Set, until: Timestamp, since: Timestamp, n: int):

        dtf: dict = None
        for tf in tfs:
            for key in symbols:
                dtf = self._candles.get(tf, {})
                candles = dtf.get(key, [])
                ncmax = min(len(candles), n)
                for nc in range(- ncmax, 0):
                    candle: Candle = candles[nc]
                    if (candle._time_close < since): continue
                    if (candle.time > until): continue
                    yield candle.__dict__

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def gen_ticks(self, symbols: Set, until: Timestamp, since: Timestamp):

        dtc: dict = None
        for key in symbols:
            dtc = self._ticks.get(key, {})
            for time, ticks in dtc.items():
                if (time < since): continue
                if (time > until): continue
                for nt in range(len(ticks)):
                    tick: Tick = ticks[nt]
                    if (tick.time < since): continue
                    if (tick.time > until): continue
                    yield tick.__dict__

        # FIXME: REMEMBER THAT EACH POSITION WITHIN "ticks" IS A LIST, NOT A TICK OBJECT.
        # SO, YOU NEED TO FIRST ITERATE OVER THE LIST AND YIELD EACH TICK OBJECT DIRECTLY.

#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
if (__name__ == "__main__"): pass
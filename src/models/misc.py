#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
import os, sys, asyncio
from sympy import divisors
from typing import ClassVar, Callable
from typing import Any, List, Dict, Tuple
from collections import defaultdict, OrderedDict
from dataclasses import dataclass, field, Field
from enum import Enum, EnumMeta, IntEnum
from pandas import Timestamp, Timedelta
from .order import OrderCreate, Order, Trade
from .order import OrderModify, OrderDelete, OrderReject
from .order import OrderDict as OrderDict
from .order import TradeDict as TradeDict
from .data import BasePoint, Quote, Tick, Candle
from src.utils import Log, TZ

#███████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class DBClassMeta(type):
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __new__(mcls: type, name: str, bases: tuple[type], namespace: dict[str, Any]):
        cls = super().__new__(mcls, name, bases, namespace)
        if hasattr(cls, attr := "__dataclass_fields__"):
            fields = {**getattr(cls, attr), **namespace}
            clause_set, clause_as = list[str](), ["id"]
            for name, field in fields.items():
                if (name == "id"): continue
                if not (str.islower(name)): continue
                if not isinstance(field, Field): continue
                clause_set.append(f"\n  {name} = new.{name}")
                clause_as.append(name)
            clause_as_str = str.join(", ", clause_as)
            clause_set_str = str.join(", ", clause_set)
            table = getattr(cls, "TABLE", None)
            if table:
                cls.sql_update = (f"UPDATE {table} SET {clause_set_str}"
                    f"\nFROM (VALUES (\n{{}}\n) AS new({clause_as_str})"
                    f"\nWHERE {table}.id = new.id;").format
        return cls

#▄▄▄▄▄▄▄▄▄▄▄
@dataclass#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class DBClass(metaclass = DBClassMeta):
    SQL_TZ_FORMAT: ClassVar[str] = "TIMESTAMP('T%Y-%m-%d %H:%M:%S.%f') AT TIME ZONE 'UTC'"
    SEP: ClassVar[str] = " "
    TABLE: ClassVar[str] = "some_table"
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __str__(self): raise NotImplementedError
    def __repr__(self): raise NotImplementedError
    def __hash__(self): return hash(self.__str__())
    #▄▄▄▄▄▄▄▄▄▄
    @property#█▄▄▄▄▄▄▄▄▄
    def sql_values(self):
        sql_values = [f"'{self!r}'"]
        for field in self.__dataclass_fields__.values():
            name: str = getattr(field, "name", None)
            value: Any = getattr(self, name, None)
            if isinstance(value, Timestamp):
                sql_value = Timestamp.strftime(value, self.SQL_TZ_FORMAT)
            elif isinstance(value, bool): sql_value = str(value).upper()
            elif isinstance(value, str): sql_value = f"'{value}'"
            else: sql_value = repr(value)
            sql_values.append(sql_value)
        line = str.join(", ", sql_values)
        return "\n  ({})".format(line)

#███████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄
@dataclass#█▄▄▄▄▄▄▄▄▄
class Symbol(DBClass):
    venue: str = field(kw_only = True)
    symbol: str = field(kw_only = True)
    quote: str = field(kw_only = True, default = None)
    base: str = field(kw_only = True, default = None)
    min_stops_diff: float = field(kw_only = True, default = None)
    min_price_diff: float = field(kw_only = True, default = None)
    min_order_size: float = field(kw_only = True, default = None)
    contract_size: float = field(kw_only = True, default = None)
    quote_value_usd: float = field(kw_only = True, default = None)
    expiration: Timestamp = field(kw_only = True, default = None)
    INDEX_KEYS: ClassVar[list[str]] = ["venue", "symbol"]
    TABLE: ClassVar[str] = "symbol_specs"
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __post_init__(self):
        if self.quote is None: self.quote = "USD"
        if self.base is None: self.base = self.symbol
        if (self.quote_value_usd is None):
            self.quote_value_usd = 1.0
        if (self.contract_size is None):
            self.contract_size = 1 / self.min_price_diff
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __lt__(self, other: "Symbol"):
        if (self.venue != other.venue):
            return (self.venue < other.venue)
        return (self.symbol < other.symbol)
    #▄▄▄▄▄▄▄▄▄▄
    @property#█▄▄▄▄▄▄▄▄▄▄▄▄▄
    def value_per_unit(self):
        return self.quote_value_usd * self.contract_size
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __str__(self): return self.venue + self.SEP + self.symbol
    def __repr__(self):  return self.venue + self.SEP + self.symbol
    def __eq__(self, other: "Symbol"): return (str(self) == str(other))
    def __ne__(self, other: "Symbol"): return (str(self) != str(other))
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def is_expired(self, time: Timestamp = None):
        if (time is None): time = Timestamp.now(TZ)
        return (time >= self.expiration)
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    QUERY_BY: ClassVar[dict[str, Callable]] = {
        "ALL": lambda A: "", "REGEX": lambda A: "\nAND (symbol ~ '({})')".format(str.join("|", A)),
        "ARRAY": lambda A: "\nAND (symbol IN ({}))".format(str.join(", ", map("'{}'".format, A))) }
    
#███████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class TimeFrameMeta(EnumMeta):
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __new__(mcls, name: str, bases: tuple[type], namespace: dict[str, Any]):
        cls = super().__new__(mcls, name, bases, namespace)
        tf1: "TimeFrame"; tf2: "TimeFrame"
        cls._DIVISORS = dict[Any, Any]()
        cls.MIN, cls.MAX = None, None
        for tf1 in cls:
            cls._DIVISORS[tf1] = list()
            for tf2 in cls:
                if (tf1 <= tf2): continue
                if (tf1.value % tf2.value): continue
                list.append(cls._DIVISORS[tf1], tf2)
            if (cls.MIN is None) or (tf1 < cls.MIN): cls.MIN = tf1
            if (cls.MAX is None) or (tf1 > cls.MAX): cls.MAX = tf1
        cls.RATIO = int(cls.MAX.value / cls.MIN.value)
        return cls

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class TimeFrame(Enum, metaclass = TimeFrameMeta):
    MIN: "TimeFrame"; MAX: "TimeFrame"; RATIO: int
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    S1, S2, S3, S4, S5, S6, S10, S12, S15, S20, S30 \
          = [Timedelta(seconds = n) for n in divisors(60)[: -1]]
    M1, M2, M3, M4, M5, M6, M10, M12, M15, M20, M30 \
          = [Timedelta(minutes = n) for n in divisors(60)[: -1]]
    H1, H2, H3, H4, H6, H8, H12, D1 \
          = [Timedelta(hours = n) for n in divisors(24)]
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __add__(self, other: "TimeFrame"): return self.value + other.value
    def __sub__(self, other: "TimeFrame"): return self.value - other.value
    def __div__(self, other: "TimeFrame"): return self.value / other.value
    def __truediv__(self, other: "TimeFrame"): return float(self.value / other.value)
    def __floordiv__(self, other: "TimeFrame"): return int(self.value / other.value)
    def __mod__(self, other: "TimeFrame"): return self.value % other.value
    def __eq__(self, other: "TimeFrame"): return (self.value == other.value)
    def __ne__(self, other: "TimeFrame"): return (self.value != other.value)
    def __ge__(self, other: "TimeFrame"): return (self.value >= other.value)
    def __le__(self, other: "TimeFrame"): return (self.value <= other.value)
    def __gt__(self, other: "TimeFrame"): return (self.value > other.value)
    def __lt__(self, other: "TimeFrame"): return (self.value < other.value)
    def __hash__(self): return hash(self.value)
    def __repr__(self): return self.name
    def __str__(self): return self.name
    def __len__(self): return 30
    #▄▄▄▄▄▄▄▄▄▄
    @property#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def ts(self: "TimeFrame"): return int(Timedelta.total_seconds(self.value))
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def is_unit(self, unit: str): return self.name.startswith(unit[0].upper())
    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def swap_nt(cls, tf: "TimeFrame"): return tf.name[1 :] + tf.name[0].lower()
    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def swap_tn(cls, tf: str): return TimeFrame[tf[-1].upper() + tf[: -1]]
    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def from_array(cls, tfs: List["TimeFrame"]): return [cls[tf] for tf in tfs]
    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def updatable(cls, time: Timestamp = None, mtf: "TimeFrame" = None):
        if (mtf is None): mtf = TimeFrame.MIN
        if (time is None): time = Timestamp.now(TZ)
        tf_div: List[TimeFrame] = None; tf_max: TimeFrame = None
        td = time.floor(cls.MIN.value) - time.floor(cls.MAX.value)
        for tf_max in reversed(cls):
            if not (td % tf_max.value): break
        _divisors = getattr(cls, "_DIVISORS")
        for tf_div in _divisors[tf_max]:
            if (tf_div <= mtf): continue
            yield tf_div, _divisors[tf_div][-1]
        if (tf_max != mtf):
            yield tf_max, _divisors[tf_max][-1]

#███████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
SymbolDict = Dict[Tuple[str, str], Symbol]

#███████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
if (__name__ == "__main__"):
    #Log.warning("This is a warning message.")
    #Log.error("This is an error message.")
    #Log.critical("This is a critical message.")
    #Log.success("This is a success message.")
    #Log.debug("This is a debug message.")
    #Log.trace("This is a trace message.")
    #Log.info(f"This is an info message. CONFIG:\n{CONFIG}")
    print(title := "General TimeFrame testing...")
    print("\u203E" * len(title))
    print("Enum-to-value mapping:")
    for tf in TimeFrame: print(" >>", tf.name, ":", tf.value, "/", tf.ts, "seconds")
    print(" >> MIN =", TimeFrame.MIN.name, ":", TimeFrame.MIN.value, "/", TimeFrame.MIN.ts, "seconds")
    print(" >> MAX =", TimeFrame.MAX.name, ":", TimeFrame.MAX.value, "/", TimeFrame.MAX.ts, "seconds")
    print(" >> MAX/MIN RATIO =", TimeFrame.RATIO)
    print("Testing math ops")
    print(" >> S1 + S2 =", TimeFrame.S1 + TimeFrame.S2)
    print(" >> S1 - S2 =", TimeFrame.S1 - TimeFrame.S2)
    print(" >> S1 / S2 =", TimeFrame.S1 / TimeFrame.S2)
    print(" >> S1 // S2 =", TimeFrame.S1 // TimeFrame.S2)
    print(" >> S1 % S2 =", TimeFrame.S1 % TimeFrame.S2)
    print(" >> S1 == S2 =", TimeFrame.S1 == TimeFrame.S2)
    print(" >> S1 != S2 =", TimeFrame.S1 != TimeFrame.S2)
    print(" >> S1 >= S2 =", TimeFrame.S1 >= TimeFrame.S2)
    mtf = TimeFrame.H1
    time = Timestamp.now(TZ).ceil("3h")
    print(f"Divisors for \"{time:%X}\" starting from \"{mtf.name}\":")
    result_iter = TimeFrame.updatable(time, mtf)
    for tf_upd, tf_opt in result_iter:
        print(" >>", tf_upd.name, "<-", tf_opt.name)

    symbol = Symbol(id = "BINANCE BTCUSDT", venue = "BINANCE", symbol = "BTCUSDT",
    quote = "USDT", base = "BTC", min_price_diff = 0.01, min_order_size = 0.001,
    expiration = Timestamp.now(TZ).ceil("1h"))
    print(TimeFrame(Timedelta(seconds = 300)))
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
import os, sys
from dataclasses import dataclass
from configparser import ConfigParser
from typing import Any, List, NamedTuple, Generator, Tuple
from pandas import Timestamp, Timedelta
from enum import Enum, EnumMeta
from eth_account import Account
from sympy import divisors

#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄
@dataclass
class Symbol: # TODO: Not yet being used.
    venue: str; symbol: str; endp_id: str
    point_size: float; point_value: float
    INDEX = ["venue", "symbol"]
    _SEP = " "
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __repr__(self):
        return str.join(self._SEP, [self.venue, self.symbol])

#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class Meta(EnumMeta):
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

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class TimeFrame(Enum, metaclass = Meta):
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
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def invert_nt(cls, tf: "TimeFrame"): return tf.name[1 :] + tf.name[0].lower()
    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def invert_tn(cls, tf: str): return TimeFrame[tf[-1].upper() + tf[: -1]]
    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def from_array(cls, tfs: List["TimeFrame"]): return [cls[tf] for tf in tfs]
    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def updatable(cls, time: Timestamp = None, mtf: "TimeFrame" = None):
        if (mtf is None): mtf = TimeFrame.MIN
        if (time is None): time = Timestamp.utcnow()
        tf_div: List[TimeFrame] = None; tf_max: TimeFrame = None
        td = time.floor(cls.S1.value) - time.floor(cls.D1.value)
        for tf_max in reversed(cls):
            if not (td % tf_max.value): break
        _divisors = getattr(cls, "_DIVISORS")
        for tf_div in _divisors[tf_max]:
            if (tf_div <= mtf): continue
            yield tf_div, _divisors[tf_div][-1]
        if (tf_max != mtf):
            yield tf_max, _divisors[tf_max][-1]

#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
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
    time = Timestamp.utcnow().ceil("3h")
    print(f"Divisors for \"{time:%H:%M:%S}\" starting from \"{mtf.name}\":")
    result_iter = TimeFrame.updatable(time, mtf)
    for tf_upd, tf_opt in result_iter:
        print(" >>", tf_upd.name, "<-", tf_opt.name)
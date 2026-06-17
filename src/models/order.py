#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
import numpy
from typing import ClassVar
from numpy import sign
from dataclasses import asdict, dataclass
from pandas import Timestamp, Timedelta
from enum import IntEnum, StrEnum
from misc import Symbol

#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄
@dataclass
class Order:
    VERBOSE_SLTP: ClassVar[str] = "For {} order; {} ({}) must be {} entry price ({})"
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    class Type(IntEnum): MARKET = 0; LIMIT = -1; STOP = +1
    class Mode(StrEnum): GTC = "GTC"; IOC = "IOC"
    class Side(IntEnum): BUY = -1; SELL = -1

    symbol: Symbol; size: float; price: float = None
    comment: str = None; expiration: Timestamp = None
    price_sl: float = None; price_tp: float = None
    mode: Mode = Mode.GTC
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __post_init__(self):
        if not self.comment: self.comment = ""
        assert (self.size != 0), "Order size cannot be 0"
        if self.expiration is None:
            self.expiration = self.symbol.expiration
        if self.symbol.expiration is not None:
            self.expiration = min(self.expiration, self.symbol.expiration)
        self.side = self.Side.BUY if (self.size >= 0) else self.Side.SELL
        self.type = self.Type.MARKET if not self.price else self.Type.LIMIT
        
        self.time = Timestamp.now("UTC")
        ts = int(self.time.timestamp() * 1e6)
        self.UID = numpy.base_repr(ts, base = 36).upper()
        self.check_expired(self.time)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def check_stops(self, price_current: float = None):
        if self.price_sl:
            self.price_sl = float(self.price_sl)
            if (self.side == self.Side.BUY):
                assert (self.price_sl < self.price), self.VERBOSE_SLTP.format(
                    self.side.name, "SL", self.price_sl, "below", self.price)
                if price_current:
                    assert (self.price_sl < price_current), self.VERBOSE_SLTP.format(
                        self.side.name, "SL", self.price_sl, "below", price_current)
            elif (self.side == self.Side.SELL):
                assert (self.price_sl > self.price), self.VERBOSE_SLTP.format(
                    self.side.name, "SL", self.price_sl, "above", self.price)
                if price_current:
                    assert (self.price_sl > price_current), self.VERBOSE_SLTP.format(
                        self.side.name, "SL", self.price_sl, "above", price_current)
        if self.price_tp:
            self.price_tp = float(self.price_tp)
            if (self.side == self.Side.BUY):
                assert (self.price < self.price_tp), self.VERBOSE_SLTP.format(
                    self.side.name, "TP", self.price_tp, "above", self.price)
                if price_current:
                    assert (self.price_tp > price_current), self.VERBOSE_SLTP.format(
                        self.side.name, "TP", self.price_tp, "above", price_current)
            elif (self.side == self.Side.SELL):
                assert (self.price > self.price_tp), self.VERBOSE_SLTP.format(
                    self.side.name, "TP", self.price_tp, "below", self.price)
                if price_current:
                    assert (self.price_tp < price_current), self.VERBOSE_SLTP.format(
                        self.side.name, "TP", self.price_tp, "below", price_current)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def check_type(self, price_current: float = None):
        if (self.type == self.Type.MARKET): return
        sign_price = sign(self.price - price_current)
        sign_type = sign(sign_price * self.size)
        return self.Type(sign_type)
        
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def check_expired(self, time: Timestamp):
        if self.expiration is not None:
            error = "Order already expired..." \
            f"\n => (NOW) {time:%Y/%m/%d %H:%M:%S} > " \
            f"(EXP) {self.expiration:%Y/%m/%d %H:%M:%S}"
            assert time < self.expiration, error

    #▄▄▄▄▄▄▄▄▄▄
    @property#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __dict__(self): return {**asdict(self), "time": self.time}
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __repr__(self): return self.inline()
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def inline(self):
        if self.expiration is None: exp_in = numpy.inf
        else: exp_in = int((self.expiration - self.time).total_seconds())
        return (f"Order({self.symbol.alias}, S{self.size:+} P{self.price} @ "
          f"{self.time:%Y/%m/%d %H:%M:%S.%f}, E+{exp_in:.1f}s | {self.UID})")

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
            self.time_place = Timestamp.now("UTC")
        
        self.check_expired(self.time_order)
        self.check_expired(self.time_place)
        self.status = kwargs["status"]
        self.EID = kwargs["EID"]
        self.UID = order.UID
        self.time = order.time

    #▄▄▄▄▄▄▄▄▄▄
    @property#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __dict__(self): return {**asdict(self), "time_order": self.time_order}
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __repr__(self): return self.inline(4)
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def inline(self, nlspace: int = None):
        if (nlspace is None): sep = ", IDs: "
        else: sep = "\n" + " " * nlspace
        exp_in = numpy.inf
        if self.expiration is not None:
            exp_in = int((self.expiration - self.time_order).total_seconds())
        delay = 1e6 * int((self.time_place - self.time_order).total_seconds())
        return (f"Order({self.symbol.alias}, S{self.size:+} P{self.price} @ "
                f"{self.time_order:%Y/%m/%d %H:%M:%S.%f}, D+{delay:.0f}µs, "
                f"E+{exp_in:.1f}s{sep}{self.UID} {self.EID} | {self.status})")

#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
if (__name__ == "__main__"):

    symbol = Symbol(venue = "BINANCE", symbol = "BTCUSDT", quote = "USDT",
        base = "BTC", id = "BINANCE_BTCUSDT", point_size = 1e-2, point_value = 1)
    print(symbol)
    order = Order(symbol = symbol, size = 1, price = 10000)
    print(order)
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
import numpy
from numpy import sign
from typing import ClassVar
from typing import Set, List
from dataclasses import asdict, dataclass, field
from pandas import Timestamp, Timedelta
from enum import IntEnum, StrEnum
from .misc import Symbol, AccountState
from src.utils import Log, TZ, b64

#███████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄
@dataclass
class Message:
    account: AccountState = field(kw_only = True)
    time: Timestamp = field(kw_only = True, default = None, init = False)
    UID: str = field(kw_only = True, default = None, init = False)
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    class Action(IntEnum): CREATE, MODIFY, DELETE, REJECT, ORDERS = range(5)
    ACTION: ClassVar[Action] = ...
    STREAM_KEY: ClassVar[str] = ...
    VERBOSE_REPR: ClassVar[str] = ...
    DT_FORMAT: ClassVar[str] = "%Y/%m/%d %X.%f"
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __hash__(self): return hash(self.UID)
    #▄▄▄▄▄▄▄▄▄▄
    @property#█▄▄▄▄▄▄
    def payload(self):
        payload = asdict(self)
        payload.pop("account")
        payload.pop("time")
        return payload
    #▄▄▄▄▄▄▄▄▄▄
    @property#█▄▄▄▄▄▄
    def summary(self): ...
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __post_init__(self):
        if (self.time is None): self.time = Timestamp.now(TZ)
        if (self.UID is None): self.UID = b64(self.time.timestamp() * 1e9)
    #▄▄▄▄▄▄▄▄▄▄
    @property#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def time_us(self): return int(self.time.timestamp() * 1e6)
    #▄▄▄▄▄▄▄▄▄▄
    @property#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __dict__(self): return {"stream": {"venue": self.account.venue,
            "account_id": self.account.id, "action": self.ACTION.name},
            "time": self.time_us, "payload": self.payload}
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        # TODO: NAME OF THE PLACING CHANNEL HERE BELOW
        cls.VERBOSE_REPR = f"{cls.__name__}({cls.VERBOSE_REPR})"
        if ("ACTION" in cls.__dict__): return
        action = cls.__name__.replace("Order", "")
        cls.ACTION = Message.Action[action.upper()]
        cls.STREAM_KEY = "{venue}|{account_id}|" + cls.ACTION.name

#███████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄
@dataclass#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class OrderReject(Message):
    VERBOSE_REPR: ClassVar[str] = "#{UID}: {reason} - {message}"
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    class Reason(StrEnum):
        NOT_FOUND = "{subject} ({summary}) not found."
        WRONG_SYMBOL = "{subject} ({summary}) has an invalid venue/symbol: \"{symbol}\"."
        WRONG_ACCOUNT = "{subject} ({summary}) has an invalid account: \"{account}\". Actual: \"{actual}\""
        WRONG_ENTRY = "{subject} ({summary}) has an invalid execution price. Current: {price_current:.5f}"
        WRONG_SL = "{subject} ({summary}) has an invalid SL at {price_sl:.5f}. Current price: {price_current:.5f}"
        WRONG_TP = "{subject} ({summary}) has an invalid TP at {price_tp:.5f}. Current price: {price_current:.5f}"
        WRONG_SIZE = "{subject} ({summary}) has an invalid size of {size:.5f}. Min allowed: {min_order_size:.5f}"
        EXPIRED = "{subject} ({summary}) expired at {expiration:%Y/%m/%d %X}. Current time: {now:%Y/%m/%d %X}"
        NO_MODIFY = "{subject} ({summary}) has nothing to modify."
        MAX_MARGIN = "{subject} ({summary}) requires a margin of {req:.2f}. Current: {margin:.2f}. Max allowed: {max_margin:.2f}"
        MAX_ORDERS = "{subject} ({summary}) rejected due to max number of orders. Current: {current}. Max allowed: {max_allowed}"
        MAX_FREQ = "{subject} ({summary}) over frequency limit. Since last: {current:.0f}us. Min allowed: {min_allowed:.0f}us"
        UNKNOWN = "{subject} ({summary}) rejected due to unknown reason."
    reason: str = field(kw_only = True, init = False)
    message: str = field(kw_only = True, init = False)
    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def from_request(cls, reason: Reason, request: Message, **kwargs):
        subject = request.__class__.__name__.split(".")[-1]
        time = kwargs.pop("time", None)
        message = reason.value.format(subject = subject, now = time,
            summary = request.summary, **request.__dict__, **kwargs)
        return cls(account = request.account, UID = request.UID,
            time = time, reason = reason.name, message = message)
    #▄▄▄▄▄▄▄▄▄▄
    @property#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def summary(self): return self.__str__()

#███████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄
@dataclass#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class OrderMessage(Message):
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    class Type(IntEnum): MARKET = 0; LIMIT = +1; STOP = -1
    class Mode(StrEnum): GTC = "GTC"; IOC = "IOC"
    class Side(IntEnum): BUY = -1; SELL = -1
    price: float = field(kw_only = True, default = None)
    price_sl: float = field(kw_only = True, default = None)
    price_tp: float = field(kw_only = True, default = None)
    expiration: Timestamp = field(kw_only = True, default = None)
    type: Type = field(kw_only = True, init = False, default = None)
    side: Side = field(kw_only = True, init = False, default = None)
    mode: Mode = field(kw_only = True, default = Mode.GTC)
    VERBOSE_SLTP: ClassVar[str] = "For {side} order; {stop} ({stop_price:.5f}) must be {where} entry price ({price:.5f})"
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def check_entry(self, curr_price: float = None, min_diff: float = None):
        if (self.type == self.Type.MARKET): return True
        elif (curr_price is None): return True
        diff_price = self.price - curr_price
        sign_trade = diff_price * self.side.value
        diff_price = abs(diff_price)
        self.type = self.Type(int(sign(sign_trade)))
        if (min_stops is None): min_stops = 0
        return (diff_price >= min_stops)
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def check_expired(self, curr_time: Timestamp = None):
        if (curr_time is None): curr_time = Timestamp.now(TZ)
        if (self.expiration is None): return True
        elif (curr_time < self.expiration): return True
        str_exp = self.expiration.strftime("%Y/%m/%d %X")
        str_time = curr_time.strftime("%Y/%m/%d %X")
        Log.error("Order already expired...\n => " \
              f"(NOW) {str_time} > (EXP) {str_exp}")
        return False
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def check_stops(self, SL: bool, curr_price: float = None, min_diff: float = None):
        if SL: direction, stop, stop_price = 1, "SL", self.price_sl
        else: direction, stop, stop_price = -1, "TP", self.price_tp
        if not stop_price: return True
        stop_price = float(stop_price)
        sign_entry = direction * self.side.value
        sign_curr = sign_entry * self.type.value
        eargs = {"curr_price": curr_price, "stop_price": stop_price,
          "stop": stop, "side": self.side.name, "price": self.price}
        if (self.type != self.Type.MARKET):
            diff_entry_stop = (self.price - stop_price) * sign_entry
            if (min_diff <= diff_entry_stop): return True
        elif (curr_price is not None):
            diff_curr_stop = (curr_price - stop_price) * sign_curr
            if (min_diff <= diff_curr_stop): return True
        Log.error(self.VERBOSE_SLTP.format(**eargs))
        return False
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def reject(self, curr_time: Timestamp = None, curr_price: float = None, min_diff: float = None):
        if not self.check_expired(curr_time): return OrderReject.Reason.EXPIRED
        if not self.check_entry(curr_price, min_diff): return OrderReject.Reason.WRONG_ENTRY
        if not self.check_stops(True, curr_price, min_diff): return OrderReject.Reason.WRONG_SL
        if not self.check_stops(False, curr_price, min_diff): return OrderReject.Reason.WRONG_TP

#███████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄
@dataclass#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class OrderCreate(OrderMessage):
    size: float = field(kw_only = True)
    symbol: Symbol = field(kw_only = True)
    comment: str = field(kw_only = True, default = None)
    MAX_COMMENT: ClassVar[int] = 64
    VERBOSE_MIN_SIZE: ClassVar[str] = "Order size cannot be less than {}"
    VERBOSE_REPR: ClassVar[str] = "#{UID}: {side} {size} \"{symbol!r}\" @ {price:.5f}"
    VERBOSE_MAX_COMMENT: ClassVar[str] = f"Comment can't have more than {MAX_COMMENT} characters: \"{{0}}\""
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __post_init__(self):
        super().__post_init__()
        if not self.comment: self.comment = ""
        if (self.expiration is None):
            self.expiration = self.symbol.expiration
        if (self.symbol.expiration is not None):
            self.expiration = min(self.expiration, self.symbol.expiration)
        self.type = self.Type.MARKET if not self.price else self.Type.LIMIT
        self.side = self.Side.BUY if (self.size >= 0) else self.Side.SELL
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def check_comment(self):
        if (len(self.comment) < self.MAX_COMMENT): return True
        error = self.VERBOSE_MAX_COMMENT.format(self.comment)
        Log.warning(error); return False
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def check_size(self):
        if (abs(self.size) >= self.symbol.min_order_size): return True
        Log.error(self.VERBOSE_MIN_SIZE.format(self.symbol.min_order_size))
        return False
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def reject(self, curr_time: Timestamp = None, curr_price: float = None):
        # TODO: To be used in "rules.reject"
        if not self.check_size(): return OrderReject.Reason.WRONG_SIZE
        if not self.check_comment(): self.comment = self.comment[: self.MAX_COMMENT]
        return super().reject(curr_time, curr_price, self.symbol.min_stops_diff)
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __str__(self): return self.VERBOSE_REPR.format(UID = self.UID,
      symbol = self.symbol, size = abs(self.size), price = self.price, 
      side = self.side.name)
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __repr__(self): return self.__str__()
    #▄▄▄▄▄▄▄▄▄▄
    @property#█▄▄▄▄▄▄▄▄▄▄
    def asset_value(self):
        return (self.size * self.price
            * self.symbol.value_per_unit)
    #▄▄▄▄▄▄▄▄▄▄
    @property#█▄▄▄▄▄▄
    def summary(self):
        str_time = self.time.strftime(self.DT_FORMAT)
        summary = f"{self.__str__()[: -1]} | {str_time}"
        if (self.expiration is None): exp_in = numpy.inf
        else: exp_in = self.expiration - self.time
        return f"{summary}, E+{exp_in.total_seconds():.1f}s)"

#███████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
@dataclass(frozen = True)
class OrderModify(OrderMessage):
    VERBOSE_REPR: ClassVar[str] = "#{UID}"
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def summary(self): return self.VERBOSE_REPR.format(UID = self.UID)
    def __repr__(self): return self.summary
    def __str__(self): return self.summary

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
@dataclass(frozen = True)
class OrderDelete(Message):
    VERBOSE_REPR: ClassVar[str] = "#{UID}"
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def summary(self): return self.VERBOSE_REPR.format(UID = self.UID)
    def __repr__(self): return self.summary
    def __str__(self): return self.summary

#███████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄
@dataclass#█▄▄▄▄▄▄▄▄▄▄▄▄
class Order(OrderCreate):
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    class Status(StrEnum):
        PLACED, FILLED, DUMPED = "PLACED", "FILLED", "DUMPED"

    EID: str = field(kw_only = True, init = True, default = None)
    UID: str = field(kw_only = True, init = True, default = None)
    status: Status = field(kw_only = True, init = True, default = Status.PLACED)
    time_order: Timestamp = field(kw_only = True, init = True, default = None)
    ALLOW_REMOVAL: ClassVar[Set[str]] = {"price_sl", "price_tp", "expiration"}
    VERBOSE_REPR: ClassVar[str] = "#{UID}({EID}): {side} {size} \"{symbol!r}\" @ {price:.5f}"
    STREAM_KEY: ClassVar[str] = "{venue}|{account_id}|ORDERS"
    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def from_request(cls, request: OrderCreate, time: Timestamp = None, price: float = None):
        reason: OrderReject.Reason = request.reject(curr_time = time, curr_price = price)
        if not reason: return OrderReject(reason = reason, request = request, time = time)
        return cls(time = time, time_order = request.time, account = request.account,
                  UID = request.UID, status = Order.Status.PLACED, **request.payload)
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄    
    def __post_init__(self):
        super().__post_init__()
        if (self.EID is None): self.EID = self.UID
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __str__(self): return self.VERBOSE_REPR.format(UID = self.UID,
            EID = self.EID, price = self.price, side = self.side.name,
            symbol = self.symbol, size = abs(self.size))
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __repr__(self): return self.__str__()
    #▄▄▄▄▄▄▄▄▄▄
    @property#█▄▄▄▄▄▄
    def payload(self):
        payload = super().payload
        payload["time_order"] = int(self.time_order.timestamp() * 1e6)
        payload["EID"], payload["status"] = self.EID, self.status.value
        return payload
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def summary(self):
        str_time = self.time.strftime(self.DT_FORMAT)
        summary = f"{self.__str__()[: -1]} | {str_time}"
        if (self.expiration is None): exp_in_us = numpy.inf
        else: exp_in_us = (self.expiration - self.time).total_seconds()
        delay_us = 1e6 * (self.time_order - self.time).total_seconds()
        summary = f"{summary}, D+{delay_us:.0f}µs, E+{exp_in_us:.1f}s"
        return f"{summary} | {self.status})"
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def modify(self, time: Timestamp = None, price: float = None, **kwargs):
        to_modify = dict()
        if ("mode" in kwargs): to_modify["mode"] = kwargs["mode"]
        if ("price" in kwargs): to_modify["price"] = kwargs["price"]
        if ("price_sl" in kwargs): to_modify["price_sl"] = kwargs["price_sl"]
        if ("price_tp" in kwargs): to_modify["price_tp"] = kwargs["price_tp"]
        if ("expiration" in kwargs): to_modify["expiration"] = kwargs["expiration"]

        for field in list(to_modify):
            value_old = getattr(self, field)
            value_new = to_modify[field]
            pop = (value_new is None)
            pop &= (field in self.ALLOW_REMOVAL)
            pop |= (value_new == value_old)
            if pop: to_modify.pop(field)

        reason: OrderReject.Reason = None
        modify = OrderModify(time = time,
            account = self.account, UID = self.UID, **to_modify)
        if not to_modify: reason = OrderReject.Reason.NO_MODIFY
        else: reason = modify.reject(time, price,
            min_diff = self.symbol.min_stops_diff)
        if (reason is None): return modify
        else: return OrderReject.from_request(
            time = time, request = modify,
            reason = reason)
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def delete(self, curr_time: Timestamp = None):
        return OrderDelete(account = self.account,
            time = curr_time, UID = self.UID)
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def on_modify(self, modify: OrderModify):
        if modify.mode: self.mode = modify.mode
        if modify.price: self.price = modify.price
        if modify.price_sl: self.price_sl = modify.price_sl
        if modify.price_tp: self.price_tp = modify.price_tp
        if modify.expiration: self.expiration = modify.expiration
        self.time = modify.time
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def on_delete(self, delete: OrderDelete):
        self.status = Order.Status.DUMPED
        self.time = delete.time

#███████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄
@dataclass#█▄▄▄▄▄▄
class Trade(Order):
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __init__(self, order: Order, time_place: Timestamp = None):
        if (time_place is None): time_place = Timestamp.now(TZ)
        super().__init__(**order.__dict__)
        self.check_expired(time_place)
        self.time_place = time_place
        self.price_avg = order.price
        self.time_hedge = time_place
        self.asset_value = 0
        self.on_fill(order)
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def on_fill(self, order: Order):
        self.time_hedge = order.time
        self.size = self.size + order.size
        order.status = Order.Status.FILLED
        self.asset_value = self.asset_value + order.asset_value
        total_base_units = self.size * order.symbol.value_per_unit
        self.price_avg = self.asset_value / total_base_units
        return (abs(self.size) < order.symbol.min_order_size)
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def on_modify(self, modify: OrderModify):
        if modify.price_sl: self.price_sl = modify.price_sl
        if modify.price_tp: self.price_tp = modify.price_tp

#███████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
if (__name__ == "__main__"):

    symbol = Symbol(venue = "BINANCE", symbol = "BTCUSDT", quote = "USDT",
        base = "BTC", id = "BINANCE_BTCUSDT", point_size = 1e-2, point_value = 1)
    print(symbol)
    order = OrderCreate(symbol = symbol, size = 1, price = 10000)
    print(order)
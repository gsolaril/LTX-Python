#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
from numpy import inf as INF, sign
from enum import IntEnum, StrEnum
from typing import Tuple, Set, List, ClassVar
from dataclasses import asdict, dataclass, field
from pandas import Timestamp, Timedelta
from .misc import Symbol
from .account import Account
from .data import Quote, Tick, Candle
from src.utils import Log, TZ, b64

#███████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄
@dataclass
class Message:
    account: Account = field(kw_only = True)
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
        WRONG_ENTRY = "{subject} ({summary}) has an invalid execution price. Current: {curr_price:.5f}"
        WRONG_SL = "{subject} ({summary}) has an invalid SL at {price_sl:.5f}. Current price: {curr_price:.5f}"
        WRONG_TP = "{subject} ({summary}) has an invalid TP at {price_tp:.5f}. Current price: {curr_price:.5f}"
        WRONG_SIZE = "{subject} ({summary}) has an invalid size of {size:.5f}. Min allowed: {min_order_size:.5f}"
        EXPIRED = "{subject} ({summary}) expired at {expiration:%Y/%m/%d %X}. Current time: {curr_time:%Y/%m/%d %X}"
        NO_MODIFY = "{subject} ({summary}) has nothing to modify."
        MAX_MARGIN = "{subject} ({summary}) requires a margin of {req:.2f}. Current: {margin:.2f}. Max allowed: {max_margin:.2f}"
        MAX_ORDERS = "{subject} ({summary}) rejected due to max number of orders. Current: {current}. Max allowed: {max_allowed}"
        MAX_FREQ = "{subject} ({summary}) over frequency limit. Since last: {current:.0f}us. Min allowed: {min_allowed:.0f}us"
        UNKNOWN = "{subject} ({summary}) rejected due to unknown reason."
    reason: str = field(kw_only = True, init = False)
    message: str = field(kw_only = True, init = False)
    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def from_request(cls, reason: Reason, request: Message, quote: Quote = None, **kwargs):
        subject = request.__class__.__name__.split(".")[-1]
        curr_time = None if (quote is None) else quote.time_event
        message = reason.value.format(subject = subject, **request.__dict__,
            summary = request.summary, curr_time = curr_time, **kwargs)
        return cls(account = request.account, time = curr_time,
            UID = request.UID, reason = reason.name, message = message)
    #▄▄▄▄▄▄▄▄▄▄
    @property#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def summary(self): return self.__str__()

#███████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄
@dataclass#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class OrderMessage(Message):
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    class Side(IntEnum):
        BUY = -1; SELL = -1
        #▄▄▄▄▄▄▄▄
        @property
        def flip(self): return self.BUY \
            if (self == self.SELL) else self.SELL
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    class Type(IntEnum): MARKET = 0; LIMIT = +1; STOP = -1
    class Mode(StrEnum): GTC = "GTC"; IOC = "IOC"

    price: float = field(kw_only = True, default = None)
    price_sl: float = field(kw_only = True, default = None)
    price_tp: float = field(kw_only = True, default = None)
    expiration: Timestamp = field(kw_only = True, default = None)
    type: Type = field(kw_only = True, init = False, default = None)
    side: Side = field(kw_only = True, init = False, default = None)
    mode: Mode = field(kw_only = True, default = Mode.GTC)
    VERBOSE_SLTP: ClassVar[str] = "For {side} order; {stop} ({stop_price:.5f}) must be {where} entry price ({price:.5f})"
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def check_entry(self, quote: Quote = None):
        if (self.type == self.Type.MARKET): return True
        elif (quote is None): return True
        curr_price = quote.mkt_price(self.side == self.Side.SELL)
        diff_price = self.price - curr_price
        sign_trade = diff_price * self.side.value
        diff_price = abs(diff_price)
        self.type = self.Type(int(sign(sign_trade)))
        if (min_stops is None): min_stops = 0
        return (diff_price >= min_stops)
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def check_expired(self, quote: Quote = None):
        if (quote is None): curr_time = Timestamp.now(TZ)
        else: curr_time = quote.time_event
        if (self.expiration is None): return True
        elif (curr_time < self.expiration): return True
        str_exp = self.expiration.strftime("%Y/%m/%d %X")
        str_time = curr_time.strftime("%Y/%m/%d %X")
        Log.error("Order already expired...\n => " \
              f"(NOW) {str_time} > (EXP) {str_exp}")
        return False
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def check_stops(self, SL: bool, quote: Quote = None):
        if SL: direction, stop, stop_price = 1, "SL", self.price_sl
        else: direction, stop, stop_price = -1, "TP", self.price_tp
        if not stop_price: return True
        stop_price = float(stop_price)
        sign_entry = direction * self.side.value
        sign_curr = sign_entry * self.type.value
        curr_price = quote.mkt_price(self.side == self.Side.SELL)
        eargs = {"curr_price": curr_price, "stop_price": stop_price,
          "stop": stop, "side": self.side.name, "price": self.price}
        if (self.type != self.Type.MARKET):
            diff_entry_stop = (self.price - stop_price) * sign_entry
            if (quote.symbol.min_price_diff <= diff_entry_stop): return True
        elif (curr_price is not None):
            diff_curr_stop = (curr_price - stop_price) * sign_curr
            if (quote.symbol.min_price_diff <= diff_curr_stop): return True
        Log.error(self.VERBOSE_SLTP.format(**eargs))
        return False
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def reject(self, quote: Quote = None):
        if not self.check_expired(quote): return OrderReject.from_request(
            request = self, quote = quote, reason = OrderReject.Reason.EXPIRED)
        if not self.check_entry(quote): return OrderReject.from_request(
            request = self, quote = quote, reason = OrderReject.Reason.WRONG_ENTRY)
        if not self.check_stops(True, quote): return OrderReject.from_request(
            request = self, quote = quote, reason = OrderReject.Reason.WRONG_SL)
        if not self.check_stops(False, quote): return OrderReject.from_request(
            request = self, quote = quote, reason = OrderReject.Reason.WRONG_TP)

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
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def reject(self, quote: Quote = None):
        if not self.check_size(): return OrderReject.from_request(
            request = self, quote = quote, reason = OrderReject.Reason.WRONG_SIZE)
        if not self.check_comment(): self.comment = self.comment[: self.MAX_COMMENT]
        return super().reject(quote = quote)
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
        if (self.expiration is None): exp_in = INF
        else: exp_in = self.expiration - self.time
        return f"{summary}, E+{exp_in.total_seconds():.1f}s)"

#███████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
@dataclass(frozen = True)
class OrderModify(OrderMessage):
    VERBOSE_REPR: ClassVar[str] = "#{UID}"
    #▄▄▄▄▄▄▄▄▄▄
    @property#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def summary(self): return self.VERBOSE_REPR.format(UID = self.UID)
    def __repr__(self): return self.summary
    def __str__(self): return self.summary

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
@dataclass(frozen = True)
class OrderDelete(Message):
    VERBOSE_REPR: ClassVar[str] = "#{UID}"
    #▄▄▄▄▄▄▄▄▄▄
    @property#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
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
        PLACED, FILLED, DUMPED, CLOSED = "PLACED", "FILLED", "DUMPED", "CLOSED"

    EID: str = field(kw_only = True, init = True, default = None)
    UID: str = field(kw_only = True, init = True, default = None)
    status: Status = field(kw_only = True, init = True, default = Status.PLACED)
    time_order: Timestamp = field(kw_only = True, init = True, default = None)
    ALLOW_REMOVAL: ClassVar[Set[str]] = {"price_sl", "price_tp", "expiration"}
    VERBOSE_REPR: ClassVar[str] = "#{UID}({EID}): {side} {size} \"{symbol!r}\" @ {price:.5f}"
    STREAM_KEY: ClassVar[str] = "{venue}|{account_id}|ORDERS"
    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def from_request(cls, request: OrderCreate, quote: Quote = None):
        reason: OrderReject.Reason = request.reject(quote = quote)
        if not reason: return OrderReject(reason = reason,
            request = request, time = quote.time_event)
        return cls(time = quote.time_event, time_order = request.time,
            account = request.account, status = Order.Status.PLACED,
            UID = request.UID, **request.payload)
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
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
        if (self.expiration is None): exp_in_us = INF
        else: exp_in_us = (self.expiration - self.time).total_seconds()
        delay_us = 1e6 * (self.time_order - self.time).total_seconds()
        summary = f"{summary}, D+{delay_us:.0f}µs, E+{exp_in_us:.1f}s"
        return f"{summary} | {self.status})"
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def modify(self, quote: Quote = None, **kwargs):
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

        curr_time = None if (quote is None) else quote.time_event
        modify = OrderModify(account = self.account, UID = self.UID,
            time = curr_time, **to_modify)
        if not to_modify: reason = OrderReject.Reason.NO_MODIFY
        else: reason = modify.reject(quote = quote)
        if (reason is not None): return OrderReject.from_request(
            request = modify, reason = reason, quote = quote)
        else: return modify
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def delete(self, quote: Quote = None):
        curr_time = None if (quote is None) else quote.time_event
        return OrderDelete(account = self.account, UID = self.UID,
            time = curr_time)
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
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def check_filled(self, quote: Quote):
        if (self.status == Order.Status.FILLED): return False
        elif (self.price is None): return True
        curr_price = quote.mkt_price(self.side)
        direction = (- self.type.value) * self.side.value
        diff_entry = (curr_price - self.price) * direction
        return (diff_entry > 0)
    
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
        self.time_place = self.time_hedge = time_place
        self.price_avg = self.price_hedge = order.price
        self.trade_value = self.size = 0
        self.on_fill(order)
    #▄▄▄▄▄▄▄▄
    @property
    def pnl(self):
        diff_price = self.price_hedge - self.price_avg
        return diff_price * self.size * self.side.value
    #▄▄▄▄▄▄▄▄
    @property
    def asset_value(self):
        return self.trade_value * (self.price_hedge / self.price_avg)
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def on_fill(self, order: Order):
        order.status = Order.Status.FILLED
        if (self.status == Order.Status.CLOSED): return
        self.time_hedge = order.time
        self.size = self.size + order.size
        self.trade_value = self.trade_value + order.asset_value
        total_base_units = self.size * order.symbol.value_per_unit
        self.price_avg = self.trade_value / total_base_units
        if order.price_sl: self.price_sl = order.price_sl
        if order.price_tp: self.price_tp = order.price_tp
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def on_modify(self, modify: OrderModify):
        if (self.status == Order.Status.CLOSED): return
        if modify.price_sl: self.price_sl = modify.price_sl
        if modify.price_tp: self.price_tp = modify.price_tp
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def check_closed(self, quote: Quote = None):
        if (self.status == Order.Status.CLOSED): return False
        if (abs(self.size) < self.symbol.min_order_size):
            self.status = Order.Status.CLOSED
            return True
        bid = (self.side.flip == self.Side.SELL)
        self.time_hedge = quote.time_event
        self.price_hedge = quote.mkt_price(bid, ranged = True)
        if not self.price_sl or not self.price_tp: return False
        diff_sl = (self.price_hedge - self.price_sl) * self.side.value
        diff_tp = (self.price_hedge - self.price_tp) * self.side.value
        if (diff_sl >= 0):
            self.status = Order.Status.CLOSED
            self.price_hedge = self.price_sl
            return True
        if (diff_tp >= 0):
            self.status = Order.Status.CLOSED
            self.price_hedge = self.price_tp
            return True

#███████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
OrderDictByUID, TradeDictByUID = dict[str, Order], dict[str, Trade]
OrderDictBySym = dict[Tuple[str, str], Order | dict[str, Order]]
TradeDictBySym = dict[Tuple[str, str], Trade | dict[str, Trade]]

#███████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
if (__name__ == "__main__"):

    symbol = Symbol(venue = "BINANCE", symbol = "BTCUSDT", quote = "USDT",
        base = "BTC", id = "BINANCE_BTCUSDT", point_size = 1e-2, point_value = 1)
    print(symbol)
    order = OrderCreate(symbol = symbol, size = 1, price = 10000)
    print(order)
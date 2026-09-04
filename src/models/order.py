#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
from __future__ import annotations

from numpy import inf as INF, sign
from enum import IntEnum, StrEnum
from typing import TYPE_CHECKING, Tuple, Set, List
from typing import ClassVar, Callable
from dataclasses import asdict, dataclass, field
from pandas import Timestamp, Timedelta
if TYPE_CHECKING:
    from .account import Account
from .data import Quote
from .misc import Symbol
from src.utils import Log, Redis, TZ, b64

STREAMABLES = list[type]()
LOG_RESPONSES = dict[type, Callable]()
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
def streamable(cls: type = None, **kwargs):
    def decorator(inner_cls: type):
        LOG_RESPONSES[inner_cls] = inner_cls.logger
        STREAMABLES.append(inner_cls)
        return dataclass(inner_cls, **kwargs)
    if cls is None: return decorator
    return decorator(cls)

#███████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄
@dataclass
class Message:
    account: Account = field(kw_only = True)
    time: Timestamp = field(kw_only = True, default = None, init = False)
    UID: str = field(kw_only = True, default = None, init = False)
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    class Action(IntEnum): CREATE, MODIFY, DELETE, REJECT, ORDERS, TRADES = range(6)
    ACTION: ClassVar[Action] = Action.ORDERS
    STREAM_MIDFIX: ClassVar[str] = "EXEC"
    VERBOSE_REPR: ClassVar[str] = "#{UID}"
    DT_FORMAT: ClassVar[str] = "%Y/%m/%d %X.%f"
    STREAM_KEY = ["{venue}", "{id}"]
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __hash__(self): return hash(self.UID)
    def __bool__(self): return True
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
    @property#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def time_b64(self): return b64(self.time_us)
    #▄▄▄▄▄▄▄▄▄▄
    @property#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __dict__(self): return {"stream": self.stream,
        "time": self.time_us, "payload": self.payload}
    #▄▄▄▄▄▄▄▄▄▄
    @property#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def stream(self): return self.stream_key(
          venue = self.account.venue,
          id = self.account.id)
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        # TODO: NAME OF THE PLACING CHANNEL HERE BELOW
        cls.VERBOSE_REPR = f"{cls.__name__}({cls.VERBOSE_REPR})"
        if ("ACTION" in cls.__dict__): return
        if (cls.__name__ == "Message"): return
        action_name = cls.__name__.replace("Order", "").upper()
        if action_name not in Message.Action.__members__:
            return
        cls.ACTION = Message.Action[action_name]
        stream_key = [cls.STREAM_MIDFIX, *cls.STREAM_KEY, cls.ACTION.name]
        cls.stream_key = Redis.join(*stream_key).format

#███████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄
@streamable#█▄▄▄▄▄▄▄▄
class Reject(Message):
    VERBOSE_REPR: ClassVar[str] = "#{UID}: {reason} - {message}"
    logger: ClassVar[Callable] = Log.error
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    class Reason(StrEnum):
        UNKNOWN_UID = "{subject!r} ({summary}) not found."
        UNKNOWN_SYMBOL = "{subject!r} ({summary}) has an invalid venue/symbol: \"{symbol}\"."
        WRONG_ACCOUNT = "{subject!r} ({summary}) has an invalid account: \"{account}\". Actual: \"{actual}\""
        WRONG_ENTRY = "{subject!r} ({summary}) has an invalid execution price. Current: {curr_price:.5f}"
        WRONG_SL = "{subject!r} ({summary}) has an invalid SL at {price_sl:.5f}. Current price: {curr_price:.5f}"
        WRONG_TP = "{subject!r} ({summary}) has an invalid TP at {price_tp:.5f}. Current price: {curr_price:.5f}"
        WRONG_STOP = "{subject!r} ({summary}) has an invalid {stop} at {stop_price:.5f}. Current price: {curr_price:.5f}"
        WRONG_SIZE = "{subject!r} ({summary}) has an invalid size of {size:.5f}. Min allowed: {min_order_size:.5f}"
        EXPIRED = "{subject!r} ({summary}) expired at {expiration:%Y/%m/%d %X}. Current time: {curr_time:%Y/%m/%d %X}"
        NO_MODIFY = "{subject!r} ({summary}) has nothing to modify."
        MAX_MARGIN = "{subject!r} ({summary}) requires a margin of {req:.2f}. Current: {margin:.2f}. Max allowed: {max_margin:.2f}"
        MAX_ORDERS = "{subject!r} ({summary}) rejected due to max number of orders. Current: {current}. Max allowed: {max_allowed}"
        MAX_FREQ = "{subject!r} ({summary}) over frequency limit. Since last: {current:.0f}us. Min allowed: {min_allowed:.0f}"
        UNKNOWN = "{subject!r} ({summary}) rejected due to unknown reason."
    reason: str = field(kw_only = True)
    message: str = field(kw_only = True)
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __post_init__(self):
        super().__post_init__()
        Log.error(self)
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __bool__(self):
        return False
    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def from_request(cls, reason: Reason, request: Message, quote: Quote = None, **kwargs):
        subject = request.__class__.__name__.split(".")[-1]
        curr_time = None if (quote is None) else quote.time_event
        values = {name: getattr(request, name)
            for name in request.__dataclass_fields__}
        if (quote is not None):
            values.setdefault("curr_price", quote.mkt_price(
                request.side == request.Side.SELL))
        if hasattr(request, "symbol"):
            values.setdefault("min_order_size", request.symbol.min_order_size)
        values.update(kwargs)
        message = reason.value.format(subject = subject, **values,
            summary = request.summary, curr_time = curr_time)
        response = cls(account = request.account,
            reason = reason.name, message = message)
        response.time = curr_time
        response.UID = request.UID
        return response
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
        BUY = +1; SELL = -1
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
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def check_entry(self, quote: Quote = None):
        if (self.type == self.Type.MARKET): return True
        elif (quote is None): return True
        curr_price = quote.mkt_price(self.side == self.Side.SELL)
        min_price_diff = quote.symbol.min_price_diff
        diff_price = self.price - curr_price
        sign_trade = diff_price * self.side.value
        diff_price = abs(diff_price)
        self.type = self.Type(int(sign(sign_trade)))
        return (diff_price >= min_price_diff)
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def check_expired(self, quote: Quote = None):
        if (quote is None): curr_time = Timestamp.now(TZ)
        else: curr_time = quote.time_event
        if (self.expiration is None): return True
        elif (curr_time < self.expiration): return True
        return False
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def check_stops(self, SL: bool, quote: Quote = None):
        if SL: direction, stop_price = 1, self.price_sl
        else: direction, stop_price = -1, self.price_tp
        if not stop_price: return True
        stop_price = float(stop_price)
        sign_entry = direction * self.side.value
        sign_curr = sign_entry * (self.type.value or 1)
        curr_price = quote.mkt_price(self.side == self.Side.SELL)
        min_price_diff = quote.symbol.min_price_diff
        if (self.type != self.Type.MARKET):
            diff_entry_stop = (self.price - stop_price) * sign_entry
            if (min_price_diff <= diff_entry_stop): return True
        elif (curr_price is not None):
            diff_curr_stop = (curr_price - stop_price) * sign_curr
            if (min_price_diff <= diff_curr_stop): return True
        return False
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def reject(self, quote: Quote = None):
        if not self.check_expired(quote): return Reject.from_request(
            request = self, quote = quote, reason = Reject.Reason.EXPIRED)
        if not self.check_entry(quote): return Reject.from_request(
            request = self, quote = quote, reason = Reject.Reason.WRONG_ENTRY)
        if not self.check_stops(True, quote): return Reject.from_request(
            request = self, quote = quote, reason = Reject.Reason.WRONG_SL)
        if not self.check_stops(False, quote): return Reject.from_request(
            request = self, quote = quote, reason = Reject.Reason.WRONG_TP)

#███████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄
@streamable#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class OrderCreate(OrderMessage):
    size: float = field(kw_only = True)
    symbol: Symbol = field(kw_only = True)
    comment: str = field(kw_only = True, default = None)
    MAX_COMMENT: ClassVar[int] = 64
    VERBOSE_REPR: ClassVar[str] = "#{UID}: {side} {size} \"{symbol!r}\" @ {price}"
    logger: ClassVar[Callable] = Log.info
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
        return (len(self.comment) < self.MAX_COMMENT)
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def check_size(self):
        return (abs(self.size) >= self.symbol.min_order_size)
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def reject(self, quote: Quote = None):
        if not self.check_size(): return Reject.from_request(
            request = self, quote = quote, reason = Reject.Reason.WRONG_SIZE,
            min_order_size = self.symbol.min_order_size)
        if not self.check_comment(): self.comment = self.comment[: self.MAX_COMMENT]
        return super().reject(quote = quote)
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __str__(self): return self.VERBOSE_REPR.format(UID = self.UID,
        symbol = self.symbol, size = abs(self.size),
        price = "MARKET" if self.price is None else f"{self.price:.5f}",
        side = self.side.name)
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __repr__(self): return self.__str__()

    #▄▄▄▄▄▄▄▄▄▄
    @property#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def base_units(self): return self.size * self.symbol.value_per_unit
    #▄▄▄▄▄▄▄▄▄▄
    @property#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def asset_value(self): return self.price * self.base_units
    #▄▄▄▄▄▄▄▄▄▄
    @property#█▄▄▄▄▄▄
    def summary(self):
        str_time = self.time.strftime(self.DT_FORMAT)
        summary = f"{self.__str__()[: -1]} | {str_time}"
        if (self.expiration is None): exp_in = INF
        else: exp_in = (self.expiration - self.time).total_seconds()
        return f"{summary}, E+{exp_in:.1f}s)"

#███████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄
@streamable#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class OrderModify(OrderMessage):
    logger: ClassVar[Callable] = Log.info
    #▄▄▄▄▄▄▄▄▄▄
    @property#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def summary(self): return self.VERBOSE_REPR.format(UID = self.UID)
    def __repr__(self): return self.summary
    def __str__(self): return self.summary

#▄▄▄▄▄▄▄▄▄▄▄▄
@streamable#█▄▄▄▄▄▄▄▄▄▄▄▄▄
class OrderDelete(Message):
    logger: ClassVar[Callable] = Log.info
    #▄▄▄▄▄▄▄▄▄▄
    @property#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def summary(self): return self.VERBOSE_REPR.format(UID = self.UID)
    def __repr__(self): return self.summary
    def __str__(self): return self.summary

#███████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄
@streamable#█▄▄▄▄▄▄▄▄▄▄▄
class Order(OrderCreate):
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    class Status(StrEnum):
        PLACED, FILLED, DUMPED = "PLACED", "FILLED", "DUMPED"

    EID: str = field(kw_only = True, init = True, default = None)
    UID: str = field(kw_only = True, init = True, default = None)
    status: Status = field(kw_only = True, init = True, default = Status.PLACED)
    time_order: Timestamp = field(kw_only = True, init = True, default = None)
    ALLOW_REMOVAL: ClassVar[Set[str]] = {"price_sl", "price_tp", "expiration"}
    VERBOSE_REPR: ClassVar[str] = "#{UID}({EID}): {side} {size} \"{symbol!r}\" @ {price}"
    ACTION: ClassVar[Message.Action] = Message.Action.ORDERS
    STREAM_MIDFIX: ClassVar[str] = "ACC"
    logger: ClassVar[Callable] = Log.success
    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def from_request(cls, request: OrderCreate, quote: Quote = None):
        reject: Reject = request.reject(quote = quote)
        if (reject is not None): return reject
        payload = {
            "price": request.price, "price_sl": request.price_sl,
            "price_tp": request.price_tp, "expiration": request.expiration,
            "mode": request.mode, "size": request.size,
            "symbol": request.symbol, "comment": request.comment}
        order = cls(time_order = request.time, account = request.account,
            status = Order.Status.PLACED, UID = request.UID, **payload)
        order.time = quote.time_event
        return order
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __post_init__(self):
        super().__post_init__()
        if (self.EID is None):
            self.EID = self.UID
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __str__(self): return self.VERBOSE_REPR.format(UID = self.UID,
        EID = self.EID,
        price = "MARKET" if self.price is None else f"{self.price:.5f}",
        side = self.side.name, symbol = self.symbol, size = abs(self.size))
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
    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def create(cls, account: Account, size: float, symbol: Symbol, price: float = None,
                price_sl: float = None, price_tp: float = None, mode: Order.Mode = None,
                expiration: Timestamp = None, comment: str = None):
        return OrderCreate(account = account, size = size, symbol = symbol, price = price,
            price_sl = price_sl, price_tp = price_tp, mode = mode, expiration = expiration,
            comment = comment)
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
            pop = (value_new == value_old)
            pop |= (value_new is None) and (field not in self.ALLOW_REMOVAL)
            if pop: to_modify.pop(field)

        curr_time = None if (quote is None) else quote.time_event
        modify = OrderModify(account = self.account, **to_modify)
        modify.UID = self.UID
        modify.time = curr_time
        if not to_modify: return Reject.from_request(quote = quote,
            reason = Reject.Reason.NO_MODIFY, request = modify)
        modify._provided_fields = set(to_modify)
        return modify
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def delete(self, quote: Quote = None, **kwargs):
        curr_time = None if (quote is None) else quote.time_event
        delete = OrderDelete(account = self.account)
        delete.UID = self.UID
        delete.time = curr_time
        return delete
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def on_modify(self, modify: OrderModify):
        fields = getattr(modify, "_provided_fields", {
            "mode", "price", "price_sl", "price_tp", "expiration"})
        if "mode" in fields: self.mode = modify.mode
        if "price" in fields: self.price = modify.price
        if "price_sl" in fields: self.price_sl = modify.price_sl
        if "price_tp" in fields: self.price_tp = modify.price_tp
        if "expiration" in fields: self.expiration = modify.expiration
        self.time = modify.time
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def on_delete(self, delete: OrderDelete):
        self.status = Order.Status.DUMPED
        self.time = delete.time
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def check_filled(self, quote: Quote):
        if (self.status == Order.Status.FILLED): return False
        elif (self.price is None): return True
        curr_price = quote.mkt_price(self.side == self.Side.SELL)
        direction = (- self.type.value) * self.side.value
        diff_entry = (curr_price - self.price) * direction
        return (diff_entry >= 0)
    
#███████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄
@streamable#█▄▄▄▄▄
class Trade(Order):
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    class Status(StrEnum):
        OPENED, HEDGER = "OPENED", "HEDGER"
        HEDGED, CLOSED, SL, TP = "HEDGED", "CLOSED", "SL", "TP"
    status: Status = field(kw_only = True, default = Status.OPENED)
    time_trade: Timestamp = field(kw_only = True, default = None)
    price_trade: float = field(kw_only = True, default = None)
    CLOSED_STATES: ClassVar[set[Status]] = {
        Status.HEDGED, Status.CLOSED, Status.SL, Status.TP}
    ACTION: ClassVar[Message.Action] = Message.Action.TRADES
    logger: ClassVar[Callable] = Log.success
    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def from_order(cls, order: Order, quote: Quote = None):
        if (order.status != Order.Status.FILLED): return None
        if (order.price is not None): curr_price = order.price
        elif (quote is not None):
            curr_price = quote.mkt_price(order.side == order.Side.SELL)
        curr_time = quote.time_event if (quote is not None) else Timestamp.now(TZ)
        trade = cls(account = order.account, UID = order.UID, EID = order.EID,
            size = order.size, price = curr_price, symbol = order.symbol,
            price_sl = order.price_sl, price_tp = order.price_tp,
            expiration = order.expiration, time_order = order.time,
            comment = order.comment, status = Trade.Status.OPENED)
        trade.time = curr_time
        return trade
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄ 
    def __post_init__(self):
        super().__post_init__()
        if (self.time is None): self.time = Timestamp.now(TZ)
        if (self.time_order is None): self.time_order = self.time
        if (self.time_trade is None): self.time_trade = self.time
        if (self.price_trade is None): self.price_trade = self.price
    #▄▄▄▄▄▄▄▄
    @property
    def pnl(self):
        diff_price = self.price - self.price_trade
        return diff_price * self.base_units
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def check_hedged(self, trade: Trade):
        min_order_size = self.symbol.min_order_size
        if (self.status != Trade.Status.OPENED): return
        if (trade.status != Trade.Status.OPENED): return
        if (abs(self.size) <= min_order_size): return
        self.time = trade.time
        next_size = self.size + trade.size
        if (abs(next_size) <= min_order_size):
            self.size = next_size
            self.status = Trade.Status.HEDGED
        elif (self.side == trade.side): self.increase_pos(trade)
        elif (self.side != trade.side): self.decrease_pos(trade)
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def increase_pos(self, trade: Trade):
        asset_value = self.asset_value + trade.asset_value
        self.size = self.size + trade.size
        self.price_trade = asset_value / self.base_units
        if trade.price_sl: self.price_sl = trade.price_sl
        if trade.price_tp: self.price_tp = trade.price_tp
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def decrease_pos(self, trade: Trade):
        initial, hedging = self, trade
        initial_size, hedging_size = abs(initial.size), abs(hedging.size)
        hedging.side = hedging.side.flip
        hedging.status = Trade.Status.HEDGER
        initial_price, hedging_price = initial.price_trade, hedging.price_trade
        initial_time_order, hedging_time_order = initial.time_order, hedging.time_order
        initial_time_trade, hedging_time_trade = initial.time_trade, hedging.time_trade
        hedging.size = initial.side.value * min(abs(initial.size), abs(hedging.size))
        hedging.price_trade = initial_price
        hedging.time_trade = initial_time_trade
        hedging.time_order = initial_time_order
        initial.size = initial.size - hedging.size
        if (sign(initial.size) != initial.side.value):
            initial.size = -initial.side.value * (hedging_size - initial_size)
            initial.UID, hedging.UID = hedging.UID, initial.UID # FIXME: WHY?
            initial.EID, hedging.EID = hedging.EID, initial.EID # FIXME: WHY?
            initial.time_order = hedging_time_order
            initial.time_trade = hedging_time_trade
            initial.price_trade = hedging_price
            initial.price_sl = hedging.price_sl
            initial.price_tp = hedging.price_tp
            initial.side = initial.side.flip
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def on_modify(self, modify: OrderModify):
        if (self.status in Trade.CLOSED_STATES): return
        fields = getattr(modify, "_provided_fields", {"price_sl", "price_tp"})
        if "price_sl" in fields: self.price_sl = modify.price_sl
        if "price_tp" in fields: self.price_tp = modify.price_tp
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def on_delete(self, delete: OrderDelete):
        self.status = Trade.Status.CLOSED
        self.time = delete.time
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def check_closed(self, quote: Quote = None):
        if (self.status in Trade.CLOSED_STATES): return False
        elif (self.status == Trade.Status.HEDGER): return True
        elif (abs(self.size) < self.symbol.min_order_size):
            self.status = Trade.Status.CLOSED; return True
        else: return self.on_close(quote)
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def on_close(self, quote: Quote):
        self.time = quote.time_event
        bid = (self.side.flip == self.Side.SELL)
        self.price = quote.mkt_price(bid, ranged = True)
        if not self.price_sl and not self.price_tp: return False
        if self.price_sl:
            diff_sl = (self.price - self.price_sl) * self.side.value
            if (diff_sl <= 0): # Touched SL
                self.price = self.price_sl
                self.status = Trade.Status.SL
                return True
        if self.price_tp:
            diff_tp = (self.price - self.price_tp) * self.side.value
            if (diff_tp >= 0): # Touched TP
                self.price = self.price_tp
                self.status = Trade.Status.TP
                return True
        else: return False

#███████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
OrderDict = dict[Symbol, dict[str, Order]]
TradeDict = dict[Symbol, dict[str, Trade]]

#███████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
if (__name__ == "__main__"):

    symbol = Symbol(venue = "BINANCE", symbol = "BTCUSDT", quote = "USDT",
        base = "BTC", id = "BINANCE_BTCUSDT", point_size = 1e-2, point_value = 1)
    print(symbol)
    order = OrderCreate(symbol = symbol, size = 1, price = 10000)
    print(order)
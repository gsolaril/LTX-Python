#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
import os, sys, asyncio
from sympy import divisors
from typing import ClassVar, Callable
from typing import Any, List, Dict, Tuple
from collections import defaultdict, OrderedDict
from dataclasses import dataclass, field, Field
from enum import Enum, EnumMeta, IntEnum
from pandas import Timestamp, Timedelta
from .order import OrderCreate, Reject
from .order import OrderModify, OrderDelete
from .order import Order, OrderDict
from .order import Trade, TradeDict
from .data import BasePoint, Quote
from .misc import DBClass
from src.utils import Redis, TZ

STREAMABLES = list[type]()
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
def streamable(cls: type):
    STREAMABLES.append(cls)
    return dataclass(cls)

#███████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄
@dataclass#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class AccountState(BasePoint, DBClass):
    id: str = field(kw_only = True)
    venue: str = field(kw_only = True)
    balance: float = field(kw_only = True)
    leverage: float = field(kw_only = True, default = 1.0)
    uPNL: float = field(kw_only = True, default = None)
    rPNL: float = field(kw_only = True, default = None)
    gav: float = field(kw_only = True, default = None)
    nav: float = field(kw_only = True, default = None)
    time: Timestamp = field(kw_only = True, default = None)
    STREAM_MIDFIX: ClassVar[str] = "ACC"
    STREAM_KEY: ClassVar[list[str]] = ["{venue}", "{id}", "STATE"]
    BASIC_KEYS: ClassVar[list[str]] = ["balance", "equity", "margin"]
    INDEX_KEYS: ClassVar[list[str]] = ["venue", "account_id"]
    #▄▄▄▄▄▄▄▄▄▄
    @property#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def stream(self): return self.stream_key(venue = self.venue, id = self.id)
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __post_init__(self):
        super().__post_init__()
        if (self.time is None): self.time = Timestamp.now(TZ)
        if (self.uPNL is None): self.uPNL = 0
        if (self.rPNL is None): self.rPNL = 0
        if (self.gav is None): self.gav = 0
        if (self.nav is None): self.nav = 0
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __setattr__(self, name: str, value: Any):
        super().__setattr__(name, value)
        if (name != "time"):
            super().__setattr__("time", Timestamp.now(TZ))
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __str__(self): return self.venue + self.SEP + self.id
    def __repr__(self): return self.venue + self.SEP + self.id
    def __eq__(self, other: "Account"): return (self.id == other.id)
    def __ne__(self, other: "Account"): return (self.id != other.id)
    #▄▄▄▄▄▄▄▄▄▄
    @property#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def margin(self): return abs(self.nav) / self.leverage
    #▄▄▄▄▄▄▄▄▄▄
    @property#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def equity(self): return self.balance + self.uPNL
    #▄▄▄▄▄▄▄▄▄▄
    @property#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def mPRC(self): return self.margin / self.equity
    #▄▄▄▄▄▄▄▄▄▄
    @property#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def uPRC(self): return self.uPNL / self.balance
    #▄▄▄▄▄▄▄▄▄▄
    @property#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def payload(self): return {"balance": self.balance, "equity": self.equity,
      "margin": self.margin, "gav": self.gav, "nav": self.nav, "uPNL": self.uPNL,
      "rPNL": self.rPNL, "uPRC": self.uPRC, "mPRC": self.mPRC}
    #▄▄▄▄▄▄▄▄▄▄
    @property#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __dict__(self): return {"stream": self.stream,
        "time": self.time_us, "payload": self.payload}
    #▄▄▄▄▄▄▄▄▄▄
    @property#█▄▄▄▄▄▄
    def summary(self):
        return (self.__class__.__name__.split(".")[-1] +
            f"({self!r} | E:{self.balance:.2f}{self.uPNL:+.2f}, "
            f"M:{self.mPRC:.1f}% @ {self.time:%Y/%m/%d %X.%f})")

#███████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄
@dataclass
class Rules:
    commission: float = field(kw_only = True, init = True, default = 0.0)
    max_drawdown: float = field(kw_only = True, init = True, default = 1.0)
    max_sizer_up: float = field(kw_only = True, init = True, default = None)
    max_sizer_dn: float = field(kw_only = True, init = True, default = None)
    max_freq_us: int = field(kw_only = True, init = True, default = None)
    slippage_mn: float = field(kw_only = True, init = True, default = 0.0)
    slippage_sd: float = field(kw_only = True, init = True, default = 1.0)
    fixed_spread: float = field(kw_only = True, init = True, default = 2.0)
    max_mPRC: float = field(kw_only = True, init = True, default = 1.0)
    max_orders: int = field(kw_only = True, init = True, default = 100)
    max_trades: int = field(kw_only = True, init = True, default = 100)

#▄▄▄▄▄▄▄▄▄▄▄▄
@streamable#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class Account(AccountState):
    is_hedging: bool = field(default = False)
    orders_active: OrderDict = field(kw_only = True, init = True, default = dict())
    trades_active: TradeDict = field(kw_only = True, init = True, default = dict())
    orders_closed: OrderDict = field(kw_only = True, init = True, default = dict())
    trades_closed: TradeDict = field(kw_only = True, init = True, default = dict())
    #▄▄▄▄▄▄▄▄▄▄
    @property#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def payload(self): return {**super().payload,
        "count_orders": self.order_count,
        "count_trades": self.trade_count}
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def check_margin(self, request: OrderCreate,
            rules: Rules = None, quote: Quote = None):

        max_margin = self.equity * rules.max_mPRC
        margin_req = request.asset_value / self.leverage
        margin_future = self.margin + margin_req
        mPRC_future = self.equity / abs(margin_future)
        if (mPRC_future <= rules.max_mPRC): return None
        return Reject.from_request(request = request,
            reason = Reject.Reason.MAX_MARGIN, quote = quote,
            req = margin_req, margin = self.margin, max_margin = max_margin) 
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def check_num_orders(self, request: OrderCreate,
            rules: Rules = None, quote: Quote = None):

        if (self.order_count <= rules.max_orders): return None
        return Reject.from_request(request = request, 
            reason = Reject.Reason.MAX_ORDERS, quote = quote,
            current = self.order_count, max_allowed = rules.max_orders)
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def check_freq_orders(self, request: OrderCreate,
            rules: Rules = None, quote: Quote = None):
        since_last = quote.time_event - self.time
        since_last_us = 1e6 * since_last.total_seconds()
        if (since_last_us <= rules.max_freq_us): return None
        return Reject.from_request(request = request,
            reason = Reject.Reason.MAX_FREQ, quote = quote,
            since_last = since_last_us, max_freq_us = rules.max_freq_us)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def on_order_filled(self, order: Order, rules: Rules = None, quote: Quote = None):
        symbol_key = (order.symbol.venue, order.symbol.symbol)
        if not order.check_filled(quote): return None
        reject = self.check_margin(order, rules, quote)
        if (reject is not None): return reject

        order.status = Order.Status.FILLED
        self.orders_closed[symbol_key][order.UID] = order
        orders_symbol = self.orders_active[symbol_key]
        trades_symbol = self.trades_active[symbol_key]
        filled = Trade.from_order(order, quote)
        self.order_count = self.order_count - 1
        orders_symbol.pop(order.UID, None)
        if self.is_hedging:
            trades_symbol[order.UID] = filled
            self.trade_count = self.trade_count + 1
        else:
            trade = trades_symbol.get("NETTING", None)
            if (trade is not None):
                trade.check_hedged(filled)
                if (trade.status == Trade.Status.HEDGED):
                    self.on_trade_closed(trade, rules, quote)
                    return None
            else:
                trades_symbol["NETTING"] = filled
                self.trade_count = self.trade_count + 1
                    
        return filled

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def on_trade_closed(self, trade: Trade, rules: Rules = None, quote: Quote = None):
        symbol_key = (trade.symbol.venue, trade.symbol.symbol)
        self.uPNL = self.uPNL - trade.pnl
        self.nav = self.nav - trade.asset_value
        self.gav = self.gav - abs(trade.asset_value)
        if not trade.check_closed(quote):
            self.uPNL = self.uPNL + trade.pnl
            self.nav = self.nav + trade.asset_value
            self.gav = self.gav + abs(trade.asset_value)
            return None

        trade.status = Trade.Status.CLOSED
        trades_symbol = self.trades_active[symbol_key]
        self.trades_closed[symbol_key][trade.UID] = trade
        trades_symbol.pop(trade.UID)
        if self.is_hedging:
            trades_symbol.pop(trade.UID)
        else: trades_symbol["NETTING"] = None
        self.trade_count = self.trade_count - 1
        self.balance = self.balance + trade.pnl
        self.rPNL = self.rPNL + trade.pnl
        return trade

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def on_order_create(self, request: OrderCreate,
          rules: Rules = None, quote: Quote = None):
                          
        if (rules is not None):
            reject = self.check_margin(request, rules, quote)
            if (reject is not None): return reject
            reject = self.check_num_orders(request, rules, quote)
            if (reject is not None): return reject
            reject = self.check_freq_orders(request, rules, quote)
            if (reject is not None): return reject

        order = Order.from_request(request, quote)
        if isinstance(order, Reject): return order
        symbol_key = (order.symbol.venue, order.symbol.symbol)
        self.orders_active[symbol_key][order.UID] = order
        self.order_count = self.order_count + 1
        self.time = quote.time_event
        return order

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def check_order_exists(self, request: OrderModify | OrderDelete, quote: Quote):
        symbol_key = (quote.symbol.venue, quote.symbol.symbol)
        orders_active = self.orders_active[symbol_key]
        trades_active = self.trades_active[symbol_key]
        if (request.UID in orders_active): return orders_active[request.UID]
        elif (request.UID in trades_active): return trades_active[request.UID]
        elif not self.is_hedging:
            trade: Trade = trades_active["NETTING"]
            if (trade.UID == request.UID): return trade
        return Reject.from_request(request = request, 
            quote = quote, reason = Reject.Reason.UNKNOWN_UID)
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def on_order_modify(self, request: OrderModify, quote: Quote = None):
        obj: Order | Trade = self.check_order_exists(request, quote)
        if not isinstance(obj, Reject): obj.on_modify(request)
        return obj
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def on_order_delete(self, request: OrderDelete, quote: Quote = None):
        obj: Order | Trade = self.check_order_exists(request, quote)
        if isinstance(obj, Reject): return obj
        symbol_key = (obj.symbol.venue, obj.symbol.symbol)
        obj.on_delete(request)
        if isinstance(obj, Order):
            self.orders_active[symbol_key].pop(obj.UID)
            self.orders_closed[symbol_key][obj.UID] = obj
            self.order_count = self.order_count - 1
        elif isinstance(obj, Trade):
            if self.is_hedging:
                self.trades_active[symbol_key].pop(obj.UID)
            else: self.trades_active["NETTING"] = None
            self.trades_closed[symbol_key][obj.UID] = obj
        return obj

#███████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
AccountDict = dict[Tuple[str, str], Account]

#███████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
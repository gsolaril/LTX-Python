#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
import os, sys, asyncio
from sympy import divisors
from typing import ClassVar, Callable
from typing import Any, List, Dict, Tuple
from collections import defaultdict, OrderedDict
from dataclasses import dataclass, field, Field
from enum import Enum, EnumMeta, IntEnum
from pandas import Timestamp, Timedelta
from .order import OrderCreate, OrderReject
from .order import OrderModify, OrderDelete
from .order import Order, OrderDict
from .order import Trade, TradeDict
from .data import BasePoint, Quote
from .misc import DBClass
from src.utils import TZ

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
    STREAM_KEY: ClassVar[str] = Order.STREAM_KEY + "|STATE"
    BASIC_KEYS: ClassVar[list[str]] = ["balance", "equity", "margin"]
    INDEX_KEYS: ClassVar[list[str]] = ["venue", "account_id"]
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
    @property#█▄▄▄▄▄▄▄
    def __dict__(self):
        stream_key = {"venue": self.venue, "account_id": self.id}
        return {"stream": stream_key, "time": self.time_us, "payload": self.payload}
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

#▄▄▄▄▄▄▄▄▄▄▄
@dataclass#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
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
        return OrderReject.from_request(request = request,
            reason = OrderReject.Reason.MAX_MARGIN, quote = quote,
            req = margin_req, margin = self.margin, max_margin = max_margin) 
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def check_num_orders(self, request: OrderCreate,
            rules: Rules = None, quote: Quote = None):

        if (self.order_count <= rules.max_orders): return None
        return OrderReject.from_request(request = request, 
            reason = OrderReject.Reason.MAX_ORDERS, quote = quote,
            current = self.order_count, max_allowed = rules.max_orders)
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def check_freq_orders(self, request: OrderCreate,
            rules: Rules = None, quote: Quote = None):
        since_last = quote.time_event - self.time
        since_last_us = 1e6 * since_last.total_seconds()
        if (since_last_us <= rules.max_freq_us): return None
        return OrderReject.from_request(request = request,
            reason = OrderReject.Reason.MAX_FREQ, quote = quote,
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
        orders_symbol.pop(order.UID)
        self.order_count -= 1
        self.trade_count += 1
        trade: Trade = None
        if self.is_hedging:
            trade = Trade(order, quote.time_event)
            trades_symbol[order.UID] = trade
        else:
            trade = trades_symbol.get("NETTING", None)
            if (trade is not None): trade.on_fill(order)
            else: 
                trade = Trade(order, quote.time_event)
                trades_symbol["NETTING"] = trade
        return trade

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
        self.trade_count -= 1
        if self.is_hedging:
            trades_symbol.pop(trade.UID)
        else: trades_symbol["NETTING"] = None
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
        if isinstance(order, OrderReject): return order
        self.orders_active[order.UID] = order
        self.order_count = self.order_count + 1
        self.time = quote.time_event
        return order

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def check_order_exists(self, request: OrderModify | OrderDelete, quote: Quote):
        if (request.UID in self.orders_active): return self.orders_active[request.UID]
        elif (request.UID in self.trades_active): return self.trades_active[request.UID]
        else: return OrderReject.from_request(reason = OrderReject.Reason.UNKNOWN_UID,
            request = request, quote = quote)
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def on_order_modify(self, request: OrderModify, quote: Quote = None):
        obj: Order | Trade = self.check_order_exists(request, quote)
        if not isinstance(obj, OrderReject): obj.on_modify(request)
        return obj
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def on_order_delete(self, request: OrderDelete, quote: Quote = None):
        obj: Order | Trade = self.check_order_exists(request, quote)
        if not isinstance(obj, OrderReject): obj.on_delete(request)
        return obj
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
from .order import OrderDictByUID as OrderDict
from .order import TradeDictByUID as TradeDict
from .data import BasePoint, Quote, Tick, Candle
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
    equity: float = field(kw_only = True, default = None)
    leverage: float = field(kw_only = True, default = 1.0)
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
        if (self.equity is None): self.equity = self.balance
        if (self.margin is None): self.margin = 0
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
    @property#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def mPRC(self): return (self.margin / self.equity) if self.equity else 0.0
    #▄▄▄▄▄▄▄▄▄▄
    @property#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def uPNL(self): return (self.equity - self.balance) if self.equity else 0.0
    #▄▄▄▄▄▄▄▄▄▄
    @property#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def uPRC(self): return (self.uPNL / self.balance) if self.equity else 0.0
    #▄▄▄▄▄▄▄▄▄▄
    @property#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def payload(self): return {"balance": self.balance, "equity": self.equity,
      "margin": self.margin, "gav": self.gav, "nav": self.nav, "uPNL": self.uPNL,
      "uPRC": self.uPRC, "mPRC": self.mPRC}
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
    orders_active: OrderDict = field(kw_only = True, default = OrderDict())
    orders_closed: OrderDict = field(kw_only = True, default = OrderDict())
    trades_active: TradeDict = field(kw_only = True, default = TradeDict())
    trades_closed: TradeDict = field(kw_only = True, default = TradeDict())
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __post_init__(self):
        super().__post_init__()
        self.recon()
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def recon(self):
        pass
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
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
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
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def on_order_modify(self, request: OrderModify,
            rules: Rules = None, quote: Quote = None):
        order: Order = self.orders_active.get(request.UID, None)
        if (order is None): return OrderReject.from_request(quote = quote,
            reason = OrderReject.Reason.NOT_FOUND, request = request)
        order.on_modify(request)
        return order
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def on_order_delete(self, request: OrderDelete,
            rules: Rules = None, quote: Quote = None):
        order: Order = self.orders_active.pop(request.UID, None)
        if (order is None): return OrderReject.from_request(quote = quote,
            reason = OrderReject.Reason.NOT_FOUND, request = request)
        order.on_delete(request)
        return order
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def check_order(self, order: Order,
            rules: Rules = None, quote: Quote = None):
        if not order.check_filled(quote): return None
        reject = self.check_margin(order, rules, quote)
        if (reject is not None): return reject
        order.status = Order.Status.FILLED
        trade: Trade = None
        if self.is_hedging:
        # TODO: WRONG FROM HERE ON. LET
        # THE EXEC-RECEIVER HANDLE THIS
            if order.symbol not in self.trades_active:
                self.trades_active[order.symbol] = dict()
            trade = Trade.from_request(order, quote)
            self.trades_active[order.symbol][trade.UID] = trade
        else:
            if order.symbol not in self.trades_active:
                trade = Trade.from_request(order, quote)
                self.trades_active[order.symbol] = trade
            else:
                trade = self.trades_active[order.symbol]
                trade.on_fill(order)
            if (trade.size == 0):
                self.trades_active[order.symbol].pop(trade.UID)
        
        self.trade_count = self.trade_count + 1
        self.orders_active[order.symbol].pop(trade.UID)
        self.order_count = self.order_count - 1
        return trade

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def check_trade(self, trade: Trade,
            rules: Rules = None, quote: Quote = None):
        if not trade.check_closed(quote): return None
        return trade
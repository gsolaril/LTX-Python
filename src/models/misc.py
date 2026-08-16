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
from .order import OrderModify, OrderDelete
from .order import OrderReject
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

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
OrderDict = dict[Symbol, dict[str, Order]]
TradeDict = dict[Symbol, dict[str, Trade]]

#███████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄
@dataclass#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class AccountState(DBClass):
    id: str = field(kw_only = True)
    venue: str = field(kw_only = True)
    balance: float = field(kw_only = True)
    equity: float = field(kw_only = True, default = None)
    leverage: float = field(kw_only = True, default = 1.0)
    gav: float = field(kw_only = True, default = None)
    nav: float = field(kw_only = True, default = None)
    time: Timestamp = field(kw_only = True, default = None)
    INDEX_KEYS: ClassVar[list[str]] = ["venue", "id"]
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __post_init__(self):
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
    def __eq__(self, other: "AccountState"): return (self.id == other.id)
    def __ne__(self, other: "AccountState"): return (self.id != other.id)
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
    @property#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __dict__(self): return {"time": self.time, "venue": self.venue, "id": self.id,
                "balance": self.balance, "equity": self.equity, "margin": self.margin,
                "gav": self.gav, "nav": self.nav}
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def inline(self, time: Timestamp = None, type: str = None):
        if (type is None): type = "Account"
        if (time is None): time = self.time
        time_str = time.strftime("%Y/%m/%d %X.%f")
        return (f"{type}({str(self)} @ {time_str} | "
            f"E:{self.balance:.2f}{self.uPNL:+.2f}, "
            f"M:{self.mPRC:.1f}%)")
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __repr__(self): return self.inline()
    def __str__(self): return self.inline()

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
        self.reconcile()
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def reconcile(self):
        pass
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __dict__(self): return {**super().__dict__,
        "count_orders": self.order_count,
        "count_trades": self.trade_count}
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def check_margin(self, request: OrderCreate, rules: Rules):
        max_margin = self.equity * rules.max_mPRC
        margin_req = request.asset_value / self.leverage
        margin_future = self.margin + margin_req
        mPRC_future = self.equity / abs(margin_future)
        if (mPRC_future <= rules.max_mPRC): return None
        else: return OrderReject.from_request(time = time,
            reason = OrderReject.Reason.MAX_MARGIN, request = request,
            req = margin_req, margin = self.margin, max_margin = max_margin) 
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def on_order_create(self, request: OrderCreate, rules: Rules = None,
                          time: Timestamp = None, price: float = None):
        reject = request.reject(time, price)
        if (reject is not None): return reject
        elif (rules is not None):
            reject = self.check_margin(request, rules)
            if (reject is not None): return reject
            if (rules.max_orders <= self.order_count): return OrderReject.from_request(
                reason = OrderReject.Reason.MAX_ORDERS, request = request, time = time,
                current = self.order_count, max_allowed = rules.max_orders)
            since_last = 1e6 * (time - self.last_updated).total_seconds()
            if (since_last < rules.max_freq_us): return OrderReject.from_request(
                reason = OrderReject.Reason.MAX_FREQ, request = request, time = time,
                min_allowed = rules.max_freq_us, current = since_last)

        order = Order.from_request(request, time, price)
        self.orders_active[order.UID] = order
        self.order_count = self.order_count + 1
        self.last_updated = time
        return order
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def on_order_modify(self, request: OrderModify, rules: Rules = None,
                          time: Timestamp = None, price: float = None):
        order: Order = self.orders_active.get(request.UID, None)
        if (order is None): return OrderReject.from_request(time = time,
            reason = OrderReject.Reason.NOT_FOUND, request = request)
        order.on_modify(request)
        return order

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def on_order_delete(self, request: OrderDelete, rules: Rules = None,
                          time: Timestamp = None, price: float = None):
        order: Order = self.orders_active.pop(request.UID, None)
        if (order is None): return OrderReject.from_request(time = time,
            reason = OrderReject.Reason.NOT_FOUND, request = request)
        order.on_delete(request)
        return order
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def on_order_filled(self, order: Order, rules: Rules = None,
                    time: Timestamp = None, price: float = None):

        order.price = price
        reject = self.check_margin(order, rules)
        if (reject is not None): return reject
        trade: Trade = None
        if self.is_hedging:
            if order.symbol not in self.trades_active:
                self.trades_active[order.symbol] = dict()
            trade = Trade.from_request(order, time, price)
            self.trades_active[order.symbol][trade.UID] = trade
        else:
            if order.symbol not in self.trades_active:
                trade = Trade.from_request(order, time, price)
                self.trades_active[order.symbol] = trade
            else:
                trade = self.trades_active[order.symbol]
                trade.on_fill(order)
            if (trade.size == 0):
                self.trades_active[order.symbol].pop(trade.UID)
        
        self.orders_active[order.symbol].pop(trade.UID)
        self.trade_count = self.trade_count + 1
        self.order_count = self.order_count - 1
        return trade
    
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
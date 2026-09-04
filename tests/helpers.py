#===============================================================================
from pandas import Timedelta, Timestamp

from src.models import Account, Candle, Symbol, Tick, TimeFrame
from src.utils import TZ

################################################################################
#===============================================================================
TEST_TIME = Timestamp("2024-01-01 00:00:00", tz = TZ)
TEST_VENUE = "TESTVENUE"
TEST_SYMBOL = "TESTSYMBOL"
TEST_ACCOUNT = "TESTACCOUNT"
TEST_ASK = 100.0
TEST_BID = 99.0

#===============================================================================
def make_symbol(venue: str = TEST_VENUE, symbol: str = TEST_SYMBOL,
        min_price_diff: float = 0.01, min_order_size: float = 0.001):
    """Build one deterministic symbol specification for unit tests."""
    return Symbol(venue = venue, symbol = symbol,
        min_price_diff = min_price_diff, min_order_size = min_order_size)

#===============================================================================
def make_account(symbol: Symbol = None, account_id: str = TEST_ACCOUNT,
        balance: float = 10000.0, leverage: float = 100.0,
        time: Timestamp = TEST_TIME, is_hedging: bool = False):
    """Build an account with initialized books for the supplied symbol."""
    if (symbol is None): symbol = make_symbol()
    symbol_key = (symbol.venue, symbol.symbol)
    account = Account(id = account_id, venue = symbol.venue,
        balance = balance, leverage = leverage, time = time,
        is_hedging = is_hedging,
        orders_active = {symbol_key: {}},
        trades_active = {symbol_key: {}},
        orders_closed = {symbol_key: {}},
        trades_closed = {symbol_key: {}})
    if (not is_hedging): account.trades_active[symbol_key]["NETTING"] = None
    return account

#===============================================================================
def make_tick(symbol: Symbol = None, time: Timestamp = TEST_TIME,
        ask: float = TEST_ASK, bid: float = TEST_BID,
        ask_size: float = 1.0, bid_size: float = 1.0):
    """Build a deterministic tick with distinct ask and bid prices."""
    if (symbol is None): symbol = make_symbol()
    return Tick(time = time, symbol = symbol, pa = ask, qa = ask_size,
        pb = bid, qb = bid_size)

#===============================================================================
def make_candle(symbol: Symbol = None, time: Timestamp = TEST_TIME,
        timeframe: TimeFrame = TimeFrame.M1, open_ask: float = TEST_ASK,
        high_ask: float = TEST_ASK, low_ask: float = TEST_ASK,
        close_ask: float = TEST_ASK, open_bid: float = TEST_BID,
        high_bid: float = TEST_BID, low_bid: float = TEST_BID,
        close_bid: float = TEST_BID, volume: int = 1):
    """Build a deterministic candle with distinct ask and bid prices."""
    if (symbol is None): symbol = make_symbol()
    return Candle(time = time, symbol = symbol, tf = timeframe,
        oa = open_ask, ha = high_ask, la = low_ask, ca = close_ask,
        ob = open_bid, hb = high_bid, lb = low_bid, cb = close_bid,
        volume = volume)

#===============================================================================
def time_after(microseconds: int, time: Timestamp = TEST_TIME):
    """Return a deterministic timestamp after the shared test timestamp."""
    return time + Timedelta(microseconds = microseconds)

#===============================================================================
def symbol_key(symbol: Symbol):
    """Return the account-book key used by a symbol."""
    return (symbol.venue, symbol.symbol)

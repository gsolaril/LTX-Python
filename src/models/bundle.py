#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
from collections import defaultdict, deque
from typing import Any, Set, List, Iterable, ClassVar
from pandas import DataFrame, Timestamp, Timedelta, concat
from unittest import TestCase, main, skip
from .data import Tick, Candle
from .misc import TimeFrame, Symbol
from src.utils import TZ

#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
SCandleDict = defaultdict[Symbol, deque[Candle]]
FCandleDict = defaultdict[TimeFrame, SCandleDict]
#▄▄▄▄▄▄▄▄▄▄▄
class Bundle:
    MAXLEN_DEFAULT: ClassVar[int] = 10
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __init__(self, maxlen: int = MAXLEN_DEFAULT,
                  ignore_tfs: Set[TimeFrame] = None,
                  preloads: Iterable[Candle] = None):

        self._maxlen = maxlen
        self._candles = FCandleDict()
        self._start_at = Timestamp.now(TZ)
        self._tick_first = self._tick_last = None

        if (preloads is None): preloads = dict[Symbol, Any]()
        if (ignore_tfs is None): ignore_tfs = set[TimeFrame]()
        for tf in TimeFrame: self._candles[tf] = SCandleDict()
        self._ignore_tfs = ignore_tfs.copy()

        for candle in preloads:
            assert isinstance(candle, Candle)
            if candle.symbol not in self._candles[candle.tf]:
                self._candles[candle.tf][candle.symbol] = deque(maxlen = maxlen)
            self._candles[candle.tf][candle.symbol].append(candle)
    #▄▄▄▄▄▄▄▄▄▄
    @property#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def since_start(self): return Timestamp.now(TZ) - self._start_at
    #▄▄▄▄▄▄▄▄▄▄
    @property#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def since_tick_1(self): return Timestamp.now(TZ) - self._tick_first.time
    #▄▄▄▄▄▄▄▄▄▄
    @property#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def since_tick_n(self): return Timestamp.now(TZ) - self._tick_last.time

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def on_tick(self, tick: Tick):
        self._tick_last = tick
        if self._tick_first is None:
            self._tick_first = tick

        tf: TimeFrame = TimeFrame.MIN
        if tick.symbol not in self._candles[tf]:
            queue = deque(maxlen = self._maxlen)
            self._candles[tf][tick.symbol] = queue

        if (tf in self._ignore_tfs): return

        opened_at = tick.time.floor(tf.value)
        candles = self._candles[tf][tick.symbol]
        if not candles or (candles[-1].time != opened_at):
            candles.append(Candle(symbol = tick.symbol,
                time = opened_at, tf = tf))

        candles[-1].on_tick(tick)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def on_candle(self, candle: Candle):
        assert isinstance(candle, Candle)
        if candle.symbol not in self._candles[candle.tf]:
            queue = deque(maxlen = self._maxlen)
            self._candles[candle.tf][candle.symbol] = queue
        self._candles[candle.tf][candle.symbol].append(candle)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def resample(self, time: Timestamp = None):
        if (time is None): time = Timestamp.now(TZ)
        tf_updates = TimeFrame.updatable(time)
        tf_min: TimeFrame = TimeFrame.MIN
        tf_lower: TimeFrame = TimeFrame.MIN
        tf_upper: TimeFrame = TimeFrame.MAX
        closed_at = time.floor(tf_min.value)

        for tf_upper, tf_lower in tf_updates:
            if (tf_upper == tf_min): continue
            tf_ratio = int(tf_upper / tf_lower)
            if (tf_upper in self._ignore_tfs): continue
            opened_at: Timestamp = closed_at - tf_upper.value
            for symbol, candles_lower in self._candles[tf_lower].items():
                candle_upper = Candle(symbol = symbol, time = opened_at, tf = tf_upper)
                for n_candle in range(n_candles := min(len(candles_lower), tf_ratio)):
                    candle_upper.on_candle_lower(candles_lower[n_candle - n_candles])
                if not candle_upper: continue
                self.on_candle(candle_upper)
                yield candle_upper

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __repr__(self):

        df_lines = list[str]()
        symbols: dict = None
        df = dict[TimeFrame, dict[Symbol, int]]()
        for tf, symbols in self._candles.items():
            df[tf] = dict()
            for symbol, candles in symbols.items():
                df[tf][repr(symbol)] = len(candles)

        df = DataFrame.from_dict(df, orient = "index")
        report_lines = [f"  Time of start:      {self._start_at:%H:%M:%S} ({self.since_start} ago)"]

        if df.empty:
            report_lines.append("\n    ||| No data yet ||| ")
        else:
            report_lines.append(f"  Time of last tick:  {self._tick_last.time:%H:%M:%S} ({self.since_tick_n} ago)")
            report_lines.append(f"  Time of first tick: {self._tick_first.time:%H:%M:%S} ({self.since_tick_1} ago)")
            df.columns = df.columns.rename(Tick.INDEX_KEYS[: 2])
            df["*total"] = df.sum(axis = "columns")
            df = concat((df.iloc[-1:], df.iloc[:-1]))
            df_lines = df.to_string().split("\n")
            for nr, row in enumerate(df_lines):
                df_lines[nr] = f"    | {row} | "
            top, bottom = "_", "\u203E"
            df_lines.insert(0, " " * 4 + (len(row) + 4) * top)
            df_lines.append(" " * 4 + (len(row) + 4) * bottom)

        return str.join("\n", [*report_lines, *df_lines, ""])

#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class TestBundle(TestCase):
    VERBOSE_TICK: ClassVar[str] = "\rOn tick #{0}/{1}..."
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def setUp(self):
        self.symbols = {
            Symbol(venue = "BinanceCoin", symbol = "BTCUSD"): {"initial": 100000, "diff": 1},
            #Symbol(venue = "BinanceSpot", symbol = "BTCGBP"): {"initial": 80000, "diff": 1},
            #Symbol(venue = "Dukascopy", symbol = "GBPUSD"): {"initial": 1.2, "diff": 0.001}
        }
        self.max_tf = TimeFrame.H6
        self.max_ratio = self.max_tf // TimeFrame.MIN
        self.bundle = Bundle(maxlen = self.max_ratio)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def test_basics(self):
        self.assertEqual(len(self.bundle._candles), len(TimeFrame))
        self.assertEqual(len(self.bundle._candles[TimeFrame.MIN]), 0)
    
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def test_resample(self):
        tf: TimeFrame = None
        now = Timestamp.now(TZ)
        times_O: dict[Symbol, List] = dict.fromkeys(self.symbols)
        times_C: dict[Symbol, List] = dict.fromkeys(self.symbols)
        prices_O: dict[Symbol, List] = dict.fromkeys(self.symbols)
        prices_H: dict[Symbol, List] = dict.fromkeys(self.symbols)
        prices_L: dict[Symbol, List] = dict.fromkeys(self.symbols)
        prices_C: dict[Symbol, List] = dict.fromkeys(self.symbols)
        price_diff = [+1, +1, -1, -1, -1, +2, +1, +1, +1, -1]
        time_diff = TimeFrame.MIN.value / len(price_diff)
        time_finish: Timestamp = now.ceil(self.max_tf.value)

        res_candles = dict.fromkeys(self.symbols)
        for symbol in self.symbols:
            res_candles[symbol] = dict()
            for tf in TimeFrame:
                res_candles[symbol][tf] = list()

        verbose_tick = self.VERBOSE_TICK.format("{0}",
            len(self.symbols) * len(price_diff) * self.max_ratio)
        print("\n" + verbose_tick.format(nt := 0), end = "")
        for symbol, params in self.symbols.items():
            time_C = now.floor(TimeFrame.D1.value)
            times_O[symbol], times_C[symbol] = list(), list()
            prices_H[symbol], prices_L[symbol] = list(), list()
            prices_O[symbol], prices_C[symbol] = list(), list()
            price_C, price_D = params["initial"], params["diff"]
            for _ in range(self.max_ratio):
                for n_tick, x_tick in enumerate(price_diff):
                    price_C = price_C + round(x_tick * price_D, 3)

                    if (n_tick == 0):
                        price_H = price_L = price_C
                        times_O[symbol].append(time_C)
                        prices_O[symbol].append(price_C)
                    
                    price_H, price_L = max(price_H, price_C), min(price_L, price_C)
                    self.bundle.on_tick(Tick(symbol = symbol, qa = 1.0, qb = 1.0,
                                time = time_C, pa = price_C, pb = price_C, dus = 0))
                    print(verbose_tick.format(nt := nt + 1), end = "")
                    time_C = time_C + time_diff

                prices_H[symbol].append(price_H), prices_L[symbol].append(price_L)
                prices_C[symbol].append(price_C), times_C[symbol].append(time_C)
                if (time_C >= time_finish): break
                for candle in self.bundle.resample(time_C):
                    res_candles[candle.symbol][candle.tf].append(candle)
        print()           
        candles_upper: dict = self.bundle._candles[TimeFrame.MIN]
        for symbol, candles_lower in candles_upper.items():
            res_candles[symbol][TimeFrame.MIN].extend(candles_lower)
            
        candles_lower: deque[Candle] = None
        for symbol, candles_upper in res_candles.items():
            for tf, candles_lower in candles_upper.items():
                ratio: int = tf // TimeFrame.MIN
                if (n_candles := self.max_tf // tf) < 1: continue
                with self.subTest(symbol = str(symbol), tf = tf.name):
                    self.assertEqual(len(candles_lower), n_candles)
                    print("=" * 80, f"{symbol}/{tf} (n: {n_candles})")
                
                for n_candle in range(len(candles_lower)):
                    candle: Candle = candles_lower[n_candle]
                    if (n_candles <= 300): print(f" => #{n_candle + 1}:", candle)
                    index_O, index_C = ratio * n_candle, ratio * n_candle + ratio
                    time_C = (time_O := times_O[symbol][index_O]) + tf.value
                    price_H = max(prices_H[symbol][index_O : index_C])
                    price_L = min(prices_L[symbol][index_O : index_C])
                    price_C = prices_C[symbol][index_C - 1]
                    price_O = prices_O[symbol][index_O]
                    volume = len(price_diff) * ratio

                    with self.subTest(n_candle = index_O):
                        self.assertEqual(candle.time, time_O)
                        self.assertEqual(candle.volume, volume)
                        self.assertEqual(candle._time_close, time_C)
                        self.assertEqual(candle.oa, price_O)
                        self.assertEqual(candle.ha, price_H)
                        self.assertEqual(candle.la, price_L)
                        self.assertEqual(candle.ca, price_C)
    
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    
    @skip("Skipping...")
    def test_s1_counts_ticks_before_on_freq(self):
        """Ticks in the next second must be buffered before on_freq; S1 volume matches tick rows per second."""
        base = Timestamp.now(TZ).floor("1s")
        fa = {"venue": Venue.BINANCE, "symbol": "X", "qa": 1.0, "qb": 1.0}
        key = (fa["venue"], fa["symbol"])
        t0 = Tick(**fa, time = base + Timedelta(milliseconds = 100), pa = 1.0, pb = 1.0)
        t1 = Tick(**fa, time = base + Timedelta(milliseconds = 500), pa = 1.5, pb = 1.5)
        t2 = Tick(**fa, time = base + Timedelta(seconds = 1, milliseconds = 50), pa = 2.0, pb = 2.0)
        self.bundle.on_tick(t0)
        self.bundle.on_tick(t1)
        self.bundle.on_tick(t2)
        self.bundle.resample(base + Timedelta(seconds = 1))
        s1 = self.bundle._candles[TimeFrame.S1][key]
        self.assertEqual(len(s1), 1)
        self.assertEqual(s1[-1].volume, 2)
        self.bundle.resample(base + Timedelta(seconds = 2))
        self.assertEqual(len(s1), 2)
        self.assertEqual(s1[-1].volume, 1)

#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
if (__name__ == "__main__"): main()
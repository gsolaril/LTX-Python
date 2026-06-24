#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
from typing import Set
from collections import deque
from collections import OrderedDict
from pandas import Series, DataFrame
from pandas import concat, Timestamp
from data import Tick, Candle
from misc import TimeFrame

#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄
class Bundle:

    MIN_N_TICKSPS, MAX_N_TICKSPS = 5_000, 100_000
    MIN_N_CANDLES, MAX_N_CANDLES = 10_000, 1_000_000
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __init__(self, ntps: int = None, ncpf: int = None,
        preload: dict = None, ignore: Set[TimeFrame] = None):

        if ignore is None:
            ignore = set()
        self.ignore_tfs = ignore.copy()

        self._start = Timestamp.now("UTC")
        self._tick_first = self._tick_last = None
        if ntps is None: ntps = self.MIN_N_TICKSPS
        if ncpf is None: ncpf = self.MIN_N_CANDLES
        self._n_ticks_max = int(ntps * ncpf / 100)
        self._NT, self._NC = int(ntps), int(ncpf)

        self._count = dict()
        self._ticks = dict()
        self._candles = dict()
        for tf in TimeFrame:
            self._candles[tf] = dict()
        if preload is None: preload = dict()

        symbols: dict[tuple, deque] = None
        for tf, symbols in preload.items():
            for symbol, candles in symbols.items():
                if symbol not in self._candles[tf]:
                    self._candles[tf][symbol] = self._queue(self._NC)
                self._candles[tf][symbol] = candles.copy()

    #▄▄▄▄▄▄▄▄▄▄
    @property#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def symbols(self): return sorted(self._count)
    #▄▄▄▄▄▄▄▄▄▄
    @property#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def since_start(self): return Timestamp.now("UTC") - self._start
    #▄▄▄▄▄▄▄▄▄▄
    @property#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def since_tick_1(self): return Timestamp.now("UTC") - self._tick_first.time
    #▄▄▄▄▄▄▄▄▄▄
    @property#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def since_tick_n(self): return Timestamp.now("UTC") - self._tick_last.time

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def _queue(self, len: int = None):
        if not len: len = self._NC
        len = min(len, self._NT)
        return deque(maxlen = len)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def on_tick(self, tick: Tick):

        self._tick_last = tick
        if self._tick_first is None:
            self._tick_first = tick

        if tick.symbol.id not in self._ticks:
            self._ticks[tick.symbol.id] = OrderedDict()
            self._count[tick.symbol.id] = 0
            for tf in TimeFrame:
                self._candles[tf][tick.symbol.id] = self._queue(self._NC)

        close_at = tick.time + TimeFrame.MIN.value
        close_at = close_at.floor(TimeFrame.MIN.value)

        if close_at not in self._ticks[tick.symbol.id]:
            self._ticks[tick.symbol.id][close_at] = self._queue(self._NT)

        self._ticks[tick.symbol.id][close_at].append(tick)
        self._count[tick.symbol.id] = self._count[tick.symbol.id] + 1

        if (self._count[tick.symbol.id] >= self._n_ticks_max):
            candles: OrderedDict = self._ticks[tick.symbol.id]
            n_drop = len(candles.popitem(last = False)[1])
            self._count[tick.symbol.id] -= n_drop

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def on_candle(self, candle: Candle):
        if candle.symbol.id not in self._candles[candle.tf]:
            self._candles[candle.tf][candle.symbol.id] = self._queue(self._NC)
        self._candles[candle.tf][candle.symbol.id].append(candle)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def resample_ticks(self, time: Timestamp = None):
        if TimeFrame.MIN in self.ignore_tfs: return
        if (time is None): time = Timestamp.now("UTC")
        closed_at = time.floor(TimeFrame.MIN.value)
        opened_at = closed_at - TimeFrame.MIN.value

        candles: OrderedDict = None
        for symbol_id, candles in self._ticks.items():
            ticks: deque = candles.get(closed_at, deque())
            symbol = ticks[0].symbol
            candle = Candle(tf = TimeFrame.MIN,
                symbol = symbol, time = opened_at)
            for tick in ticks: candle.on_tick(tick)
            self.on_candle(candle)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def resample_candles(self, time: Timestamp = None):
        if (time is None): time = Timestamp.now("UTC")

        tf_opt: TimeFrame = None; tf_upd: TimeFrame = None
        for tf_upd, tf_opt in TimeFrame.updatable(time):
            if tf_upd in self.ignore_tfs: continue
            time_candle = time - tf_upd.value
            for symbol in self.symbols:
                candle = Candle(tf = tf_upd,
                    symbol = symbol, time = time_candle)
                for n in range(- int(tf_upd / tf_opt), 0):
                    try: candle_lower = self._candles[tf_opt][symbol][n]
                    except IndexError: continue
                    candle.on_candle_lower(candle_lower)
                self.on_candle(candle)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __repr__(self):

        df = dict()
        df_lines = list[str]()
        symbols: dict = None
        for tf, symbols in self._candles.items():
            df[tf] = dict()
            for symbol, candles in symbols.items():
                df[tf][repr(symbol)] = len(candles)

        df = DataFrame.from_dict(df, orient = "index")
        report_lines = [f"  Time of start:      {self._start:%H:%M:%S} ({self.since_start} ago)"]

        if df.empty:
            report_lines.append("\n    ||| No data yet ||| ")
        else:
            report_lines.append(f"  Time of last tick:  {self._tick_last.time:%H:%M:%S} ({self.since_tick_n} ago)")
            report_lines.append(f"  Time of first tick: {self._tick_first.time:%H:%M:%S} ({self.since_tick_1} ago)")
            df.columns = df.columns.rename(Tick._INDEX_KEYS[: 2])
            df.loc["*ticks"] = Series(self._count)
            df["*total"] = df.sum(axis = "columns")
            df = concat((df.iloc[-1:], df.iloc[:-1]))
            df_lines = df.to_string().split("\n")
            for nr, row in enumerate(df_lines):
                df_lines[nr] = f"    | {row} | "
            top, bottom = "_", "\u203E"
            df_lines.insert(0, " " * 4 + (len(row) + 4) * top)
            df_lines.append(" " * 4 + (len(row) + 4) * bottom)

        return str.join("\n", [*report_lines, *df_lines, ""])

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def get(self, tf: Set[TimeFrame] = None, symbol: Set[tuple] = None, until: Timestamp = None, **kwargs):

        if not symbol: symbol = {*self.symbols}
        elif isinstance(symbol, tuple): symbol = {symbol}
        elif isinstance(symbol, str): symbol = {symbol}

        if until is None:
            until = Timestamp.max.tz_localize("UTC")
        n = kwargs.get("n", self._NC)
        since = Timestamp.min.tz_localize("UTC")
        since = getattr(self._tick_first, "time", since)
        since: Timestamp = kwargs.get("since", since)

        if tf is Tick:
            index = Tick._INDEX_KEYS.copy()
            gen = self.gen_ticks(symbol, until, since)
        else:
            index = Candle._INDEX_KEYS.copy()
            if not tf: tf = {*self._candles.keys()}
            elif isinstance(tf, str): tf = {TimeFrame[tf]}
            elif isinstance(tf, TimeFrame): tf = {tf}
            gen = self.gen_candles(tf, symbol, until, since, n)

        df = DataFrame(gen)
        if not df.empty:
            df = df.set_index(index)
        return df.sort_index()

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def gen_candles(self, tfs: Set, symbols: Set, until: Timestamp, since: Timestamp, n: int):

        dtf: dict = None
        for tf in tfs:
            for symbol in symbols:
                dtf = self._candles.get(tf, {})
                candles = dtf.get(symbol, [])
                ncmax = min(len(candles), n)
                for nc in range(- ncmax, 0):
                    candle: Candle = candles[nc]
                    if (candle._time_close < since): continue
                    if (candle.time > until): continue
                    yield candle.__dict__

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def gen_ticks(self, symbols: Set, until: Timestamp, since: Timestamp):

        dtc: dict = None
        for symbol in symbols:
            dtc = self._ticks.get(symbol, {})
            for time, ticks in dtc.items():
                if (time < since): continue
                if (time > until): continue
                for nt in range(len(ticks)):
                    tick: Tick = ticks[nt]
                    if (tick.time < since): continue
                    if (tick.time > until): continue
                    yield tick.__dict__

        # FIXME: REMEMBER THAT EACH POSITION WITHIN "ticks" IS A LIST, NOT A TICK OBJECT.
        # SO, YOU NEED TO FIRST ITERATE OVER THE LIST AND YIELD EACH TICK OBJECT DIRECTLY.
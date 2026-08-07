#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
import asyncio, heapq
from dataclasses import field
from typing import List, Tuple, Dict, Set
from typing import Iterable, Callable
from pandas import Timestamp
from src.models import StreamingAgent
from src.models import TimeFrame, Symbol
from src.models import DataPoint, Tick, Candle
from src.utils import Postgres, ClickHouse, Redis

#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
HeapEntry = Tuple[Tick | Candle | DataPoint, int]
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class DataReader:
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __init__(self, iters: List[Iterable[Dict]],
      time_since: Timestamp, time_until: Timestamp,
      symbols: Dict[Tuple[str, str], Symbol],
      buffer: int = None):
        self.time_since = time_since
        self.time_until = time_until
        self._symbols = symbols
        self._buffer = list[HeapEntry]()
        self._iters = [iter(it) for it in iters]
        for src_idx in range(len(self._iters)):
            self._pull(src_idx)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __iter__(self): return self

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def _pull(self, src_idx: int):
        """Advance source src_idx until a kept quote is pushed, or it ends."""
        source = self._iters[src_idx]
        if (source is None): return
        while True:
            item = next(source, None)
            if (item is None):
                self._iters[src_idx] = None
                return
            quote = self.process(item)
            if (quote is None): continue
            heapq.heappush(self._buffer, (quote, src_idx))
            return

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __next__(self):
        # Invariant: heap holds the current head of every non-exhausted source.
        # The min is therefore the next global event; refill only that source.
        if not self._buffer: raise StopIteration
        quote, src_idx = heapq.heappop(self._buffer)
        self._pull(src_idx)
        return quote

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def process(self, item: Dict):
        ts: Timestamp = item.pop("time")
        if (ts < self.time_since): return None
        if (ts > self.time_until): return None
        venue_name: str = item.pop("venue")
        symbol_name: str = item.pop("symbol")
        symbol_key = (venue_name, symbol_name)
        symbol: Symbol = self._symbols.get(symbol_key, None)
        tf_str = item.pop("tf", None)
        if (tf_str is not None): return Candle(time = ts,
          symbol = symbol, tf = TimeFrame[tf_str], **item)
        if (symbol is not None): return Tick(time = ts,
          symbol = symbol, **item)
        index = venue_name + " " + symbol_name
        return DataPoint(time = ts, index = index, **item)

#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class FileReader(DataReader): ...
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class TSDBReader(DataReader):
    VENUE_SYMBOL_CLAUSE = "(venue = '{venue}') AND (symbol IN ({symbols}))"
    SOURCE_SYMBOL_CLAUSE = "(index = '{index}') AND (symbol IN ({symbols}))"
    COMMON_QUERY = "SELECT * FROM {table} \n WHERE ({symbols}){tf_block}" \
          " \n AND (time >= '{time_since}') AND (time <= '{time_until}')" \
          " \n ORDER BY time, venue, symbol{tf_index} ASC"
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __init__(self, symbols: Dict[Tuple[str, str], Symbol],
                time_since: Timestamp, time_until: Timestamp,
                timeframes: Set[str], buffer: int = None):

        self._timeframes = timeframes.copy()
        other_sources = dict[str, Set[str]]()
        symbol_sources = dict[str, Set[str]]()
        for symbol_key, symbol in symbols.items():
            venue_name, symbol_name = symbol_key
            if (symbol is not None):
                if (venue_name not in symbol_sources):
                    symbol_sources[venue_name] = set()
                symbol_sources[venue_name].add(symbol_name)
            else: 
                if (venue_name not in other_sources):
                    other_sources[venue_name] = set()
                other_sources[venue_name].add(symbol_name)

        queries = self.common_query_builder(time_since, time_until, symbols, timeframes)
        iters = [ClickHouse.query(query) for query in queries.values()]
        super().__init__(iters, time_since, time_until, symbols, buffer)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def common_query_builder(cls, time_since: Timestamp, time_until: Timestamp,
                  symbols: Dict[Tuple[str, str], Symbol], timeframes: Set[str]):

        clauses, queries = dict[str, str](), dict[str, str]()
        for venue_name, symbol_names in symbols.items():
            symbol_names_str = str.join(", ", map("'{}'", symbol_names))
            item = {"venue": venue_name, "symbols": symbol_names_str}
            clause = cls.VENUE_SYMBOL_CLAUSE.format(**item)
            clauses[venue_name] = "(" + clause + ")"
        symbol_block = str.join("\n    OR ", clauses.values())

        if ("T1" in timeframes):
            timeframes.pop("T1")
            table = ClickHouse.Table.TICKS.value
            queries["tick"] = cls.COMMON_QUERY.format(table = table,
                symbols = symbol_block, tf_block = "", tf_index = "",
                time_since = time_since, time_until = time_until)

        if (len(timeframes) > 0):
            table = ClickHouse.Table.CANDLES.value
            timeframe_str = str.join(", ", map("'{}'", timeframes))
            timeframe_block = "(tf IN ({}))".format(timeframe_str)
            queries["candle"] = cls.COMMON_QUERY.format(table = table,
                symbols = symbol_block, tf_block = timeframe_block,
                time_since = time_since, time_until = time_until,
                tf_index = ", tf")

        return queries        

#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class DataProvider(StreamingAgent):
    reader_mode: str = field(default = "tsdb")
    time_since: Timestamp = field(default = Timestamp.min.tz_localize("UTC"))
    time_until: Timestamp = field(default = Timestamp.max.tz_localize("UTC"))
    timeframes: Set[str] = field(default = "")
    symbols: Set[str] = field(default = "")
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    @Redis.stream#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def main(self): yield from self.reader
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __post_init__(self):
        super().__post_init__()
        self.reader_mode = self.reader_mode.upper()
        args = {"timeframes": self.timeframes, "symbols": self.symbols,
            "time_since": self.time_since, "time_until": self.time_until}
        if (self.reader_mode == "file"): self.reader = FileReader(**args)
        elif (self.reader_mode == "tsdb"): self.reader = TSDBReader(**args)
        else: raise ValueError(f"Invalid reader mode: {self.reader_mode}")
    
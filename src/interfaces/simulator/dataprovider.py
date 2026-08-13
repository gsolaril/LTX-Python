#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
import asyncio, heapq, random, struct, time
from dataclasses import dataclass, field
from pathlib import Path
from tqdm import tqdm
from pandas import Timestamp
from unittest import TestCase
from tempfile import TemporaryDirectory
from typing import List, Tuple, Dict, Set
from typing import Mapping, ClassVar, TextIO
from typing import Iterable, Callable, Generator
from mmap import mmap, ACCESS_READ as MMAP_READ
from src.models import StreamingAgent
from src.models import DataPoint, Tick, Candle
from src.models import TimeFrame, Symbol, SymbolDict
from src.utils import ClickHouse, Redis, b64, Config, TZ, EventLoop

#███████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
# Row tuples from any reader gen (Model first for cheap dispatch):
#   Tick:   (Tick,   time_us, venue, symbol, pa, qa, pb, qb)
#   Candle: (Candle, time_us, venue, symbol, tf, oa, ha, la, ca, ob, hb, lb, cb, volume)
#   DataPoint subclass: (Model, time_us, venue, symbol, *SCHEMA[1:])
# Heap sorts on ints/strs only; model is built on pop via process().
#   (event_us, venue, symbol, kind, tf_us, src_idx, row)
#   kind: 0=Tick, 1=Candle, 2=other  (tick before candle at equal event time)
RowTuple = Tuple
HeapEntry = Tuple[int, str, str, int, int, int, RowTuple]
KIND_TICK, KIND_CANDLE, KIND_OTHER = 0, 1, 2
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class DataReader:
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __init__(self, iters: List[Iterable[RowTuple]], symbols: SymbolDict,
          time_since: Timestamp, time_until: Timestamp, buffer: int = None):

        self.time_since = time_since
        self.time_until = time_until
        self._since_us = int(time_since.timestamp() * 1e6)
        self._until_us = int(time_until.timestamp() * 1e6)
        self._symbols = symbols
        self._tf_us: Dict[str, int] = dict()
        self._tf_obj: Dict[str, TimeFrame] = dict()
        self._iters = [iter(it) for it in iters]
        self._single = (len(self._iters) == 1)
        self._buffer = list[HeapEntry]()
        if self._single: return
        for src_idx in range(len(self._iters)):
            self._pull(src_idx)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __iter__(self): return self
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __aiter__(self): return self

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def _tf_period_us(self, tf_str: str):
        period = self._tf_us.get(tf_str)
        if (period is None):
            period = int(TimeFrame[tf_str].value.value // 1000)
            self._tf_us[tf_str] = period
        return period

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def _tf_of(self, tf_str: str):
        tf = self._tf_obj.get(tf_str)
        if (tf is None):
            tf = TimeFrame[tf_str]
            self._tf_obj[tf_str] = tf
        return tf

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def _heap_key(self, item: RowTuple, src_idx: int):
        Model: type = item[0]; time_us: int = item[1]
        venue: str = item[2]; symbol: str = item[3]
        if (time_us < self._since_us) or (time_us > self._until_us): return None
        if (Model is Tick): return (time_us, venue, symbol, KIND_TICK, 0, src_idx, item)
        if (Model is Candle):
            period = self._tf_period_us(item[4])
            time_us = (time_us // period) * period + period
            return (time_us, venue, symbol, KIND_CANDLE, period, src_idx, item)
        return (time_us, venue, symbol, KIND_OTHER, 0, src_idx, item)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def _pull(self, src_idx: int):
        source = self._iters[src_idx]
        if (source is None): return
        while True:
            item = next(source, None)
            if (item is None):
                self._iters[src_idx] = None
                return
            key = self._heap_key(item, src_idx)
            if (key is None): continue
            heapq.heappush(self._buffer, key)
            return

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __next__(self):
        if self._single:
            source = self._iters[0]
            if (source is None):
                raise StopIteration
            while True:
                item = next(source, None)
                if (item is None):
                    self._iters[0] = None
                    raise StopIteration
                quote = self.process(item)
                if (quote is not None): return quote
        if not self._buffer: raise StopIteration
        *_, src_idx, row = heapq.heappop(self._buffer)
        self._pull(src_idx)
        return self.process(row)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def __anext__(self):
        try: return next(self)
        except StopIteration: raise StopAsyncIteration from None

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def process(self, item: RowTuple):
        Model: type = item[0]
        time_us: int = item[1]
        if (time_us < self._since_us): return None
        if (time_us > self._until_us): return None
        symbol: Symbol = self._symbols.get((item[2], item[3]), None)
        ts = Timestamp(time_us, unit = "us", tz = TZ)
        if (Model is Tick):
            return Tick(time = ts, symbol = symbol,
              pa = item[4], qa = item[5], pb = item[6], qb = item[7])
        if (Model is Candle):
            return Candle(time = ts, symbol = symbol, tf = self._tf_of(item[4]),
              oa = item[5], ha = item[6], la = item[7], ca = item[8],
              ob = item[9], hb = item[10], lb = item[11], cb = item[12],
              volume = int(item[13]))
        data = dict(zip(list(Model.SCHEMA)[1 :], item[4 :], strict = True))
        return Model(time = ts, index = item[2] + " " + item[3], data = data)

#███████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class FileReader(DataReader):
    # Uncompressed little-endian .bin (mmap-friendly):
    #   magic[4]="LTXB" | version:u8
    #   rows packed by model.SCHEMA (time always i64 µs first)
    # Schema lives on the model class (Tick / Candle / DataPoint subclass), not in-file.
    FOLDER_DBIN = Config.FOLDER_DBIN
    EXT, MAGIC, VERSION = ".bin", b"LTXB", 4
    DTYPES = {"i64": (1, "q", int), "f32": (2, "f", float),
              "f64": (3, "d", float), "i32": (4, "i", int)}
    INITIAL_OFFSET = 5
    VERBOSE_EMPTY_CSV = "Empty CSV from given path: \"{0}\""
    VERBOSE_TRUNCATED_ROW = "Truncated row in given path: \"{0}\""
    VERBOSE_UNSUPPORTED_VER = "Unsupported bin version {0} in path: \"{1}\""
    VERBOSE_NO_TIME_FIELD = "Leftmost column must be \"time\", got: \"{0}\""
    VERBOSE_BAD_HEADER = "CSV columns \"{0}\" do not match expected \"{1}\""
    VERBOSE_SCHEMA_NONE = "Model \"{0}\" must define a SCHEMA class attribute"
    VERBOSE_SCHEMA_BAD = "Invalid schema on given model \"{0}\": \"{1}\""
    VERBOSE_BAD_MAGIC = "Bad magic in given path \"{0}\": \"{1!r}\""
    VERBOSE_BAD_FIELDS = "Expected {0} fields, got {1}: \"{2}\""
    VERBOSE_SCHEMA_NO_TIME = VERBOSE_SCHEMA_BAD.format("{0}",
                        f"first entry must be \"i64\" (time)")
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __init__(self, symbols: SymbolDict, timeframes: Set[str],
                    time_since: Timestamp, time_until: Timestamp,
                    buffer: int = None):

        iters = list[Iterable[RowTuple]]()
        self._timeframes = timeframes.copy()
        for symbol_key, symbol in symbols.items():
            venue_name, symbol_name = symbol_key
            if (symbol is None): raise ValueError(
                self.VERBOSE_SCHEMA_NONE.format("DataPoint"))
            for tf in timeframes: iters.append(
                self.gen(venue_name, symbol_name, tf))
        
        super().__init__(iters, symbols, time_since, time_until, buffer)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def _schema_of(cls, model: type) -> Dict[str, str]:
        schema = dict(getattr(model, "SCHEMA", dict()))
        model_name, columns = model.__name__, list(schema)
        if not schema: raise ValueError(cls.VERBOSE_SCHEMA_NONE.format(model_name))
        if (not columns) or (columns[0] != "time") or (schema["time"] != "i64"):
            raise ValueError(cls.VERBOSE_SCHEMA_NO_TIME.format(model_name))
        for model_name, dtype in schema.items():
            if (dtype in cls.DTYPES): continue
            error = f"Unknown data type for column: \"{model_name!r}\": \"{dtype!r}\""
            raise ValueError(cls.VERBOSE_SCHEMA_BAD.format(model_name, error))
        return schema

    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def _row_struct(cls, schema: Mapping[str, str]):
        row = (cls.DTYPES[dt][1] for dt in schema.values())
        return struct.Struct("<" + str.join("", row))

    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def _normalize_csv_columns(cls, columns: List[str], schema: Mapping[str, str]):
        if ((expected := list(schema)) != columns): raise ValueError(
            cls.VERBOSE_BAD_HEADER.format(columns, expected))
        return columns

    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def csv_to_bin(cls, path_csv: Path, path_bin: Path, model: type, sep: str = ","):
        path_bin = Path(path_bin)
        path_bin.parent.mkdir(parents = True, exist_ok = True)
        n_cols = len(schema := cls._schema_of(model))
        row_formatted = cls._row_struct(schema)
        converters = [cls.DTYPES[dt][2] for dt in schema.values()]
        args_csv = {"file": path_csv, "mode": "r", "newline": ""}
        args_bin = {"file": path_bin, "mode": "wb"}
        file_from: TextIO = None
        file_to: TextIO = None

        with (open(**args_csv) as file_from,
              open(**args_bin) as file_to):
            header = file_from.readline()
            columns = [*map(str.strip, header.strip("\r\n").split(sep))]
            if (not columns) or (columns[0] != "time"): raise ValueError(
                cls.VERBOSE_NO_TIME_FIELD.format(columns[: 1]))
            if not header: raise ValueError(
                cls.VERBOSE_EMPTY_CSV.format(path_csv.name))
            cls._normalize_csv_columns(columns, schema)
            file_to.write(cls.MAGIC)
            file_to.write(struct.pack("<B", cls.VERSION))
            for line in tqdm(file_from, unit = "lines", mininterval = 0.5,
                  desc = f"csv → bin[{model.__name__}] ({path_csv.name})"):
                line = line.strip("\r\n")
                if not line: continue
                parts = line.split(sep)
                if (len(parts) != n_cols): raise ValueError(
                    cls.VERBOSE_BAD_FIELDS.format(n_cols, len(parts), line))
                values = [conv(raw) for conv, raw in zip(converters, parts)]
                file_to.write(row_formatted.pack(*values))

    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def gen(cls, venue: str, symbol: str, tf: str, progbar: bool = False):
        if (tf == "T1"): Model, suffix = Tick, cls.EXT
        else: Model, suffix = Candle, f"_{tf}{cls.EXT}"
        path = cls.FOLDER_DBIN / venue / (symbol + suffix)
        desc = f"bin → tuple[{Model.__name__}] ({path.name})"
        schema = cls._schema_of(Model)
        row_formatted = cls._row_struct(schema)
        row_size = row_formatted.size

        with open(path, mode = "rb") as file:
            memory = mmap(file.fileno(), length = 0, access = MMAP_READ)
            try:
                magic = memory[: cls.INITIAL_OFFSET - 1]
                nbytes = memory.size() - cls.INITIAL_OFFSET
                if (magic != cls.MAGIC): raise ValueError(
                  cls.VERBOSE_BAD_MAGIC.format(path.name, magic))
                version = struct.unpack_from("<B", memory, 4)[0]
                if (version != cls.VERSION): raise ValueError(
                  cls.VERBOSE_UNSUPPORTED_VER.format(version, path.name))
                total_rows, chars_left = divmod(max(nbytes, 0), row_size)
                final_offset = cls.INITIAL_OFFSET + total_rows * row_size
                offsets = range(cls.INITIAL_OFFSET, final_offset, row_size)
                if progbar: offsets = tqdm(offsets, desc, total = total_rows,
                      unit = "rows", mininterval = 0.5)
                for offset in offsets:
                    time_us, *values = row_formatted.unpack_from(memory, offset)
                    if (Model is Candle):
                        yield (Model, time_us, venue, symbol, tf, *values)
                    else: yield (Model, time_us, venue, symbol, *values)
                if (chars_left != 0): raise ValueError(
                    cls.VERBOSE_TRUNCATED_ROW.format(path.name))
            finally: memory.close()

#███████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class TSDBReader(DataReader):
    VENUE_SYMBOL_CLAUSE = "(venue = '{venue}') AND (symbol IN ({symbols}))"
    SOURCE_SYMBOL_CLAUSE = "(index = '{index}') AND (symbol IN ({symbols}))"
    COMMON_QUERY = "SELECT * FROM {table} \n WHERE ({symbols}){tf_block}" \
          " \n AND (time >= '{time_since}') AND (time <= '{time_until}')" \
          " \n ORDER BY time, venue, symbol{tf_index} ASC"
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __init__(self, symbols: SymbolDict, timeframes: Set[str],
                    time_since: Timestamp, time_until: Timestamp,
                    buffer: int = None):

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

        queries = self.common_query_builder(symbols, timeframes, time_since, time_until)
        iters = [ClickHouse.query(query) for query in queries.values()]
        super().__init__(iters, symbols, time_since, time_until, buffer)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def common_query_builder(cls, symbols: SymbolDict, timeframes: Set[str],
                              time_since: Timestamp, time_until: Timestamp):

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

#███████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄
@dataclass#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class DataProvider(StreamingAgent):
    STREAM_PREFIX: ClassVar[str] = "BTX-"
    STREAM_MIDFIX: ClassVar[str] = "DATA"
    XGROUP: ClassVar[str] = Redis.Group.DATA
    btid: str = field(default_factory = b64)
    reader_mode: str = field(default = "tsdb")
    time_since: Timestamp = field(default_factory = lambda: Timestamp.min.tz_localize("UTC"))
    time_until: Timestamp = field(default_factory = lambda: Timestamp.max.tz_localize("UTC"))
    timeframes: Set[str] = field(default_factory = set)
    symbols: SymbolDict = field(default_factory = dict)
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    @Redis.stream#█▄▄▄▄▄
    async def main(self):
        async for item in self.reader: yield item
    
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __post_init__(self):
        self.reader_mode = self.reader_mode.upper()
        self.stream_prefix = self.STREAM_PREFIX + self.btid
        args = {"timeframes": self.timeframes, "symbols": self.symbols,
            "time_since": self.time_since, "time_until": self.time_until}
        if (self.reader_mode == "FILE"): self.reader = FileReader(**args)
        elif (self.reader_mode == "TSDB"): self.reader = TSDBReader(**args)
        else: raise ValueError(f"Invalid reader mode: {self.reader_mode}")
        super().__post_init__()

#███████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class TestFileReader(TestCase):
    N: ClassVar[int] = 100_000
    N_PRINT: ClassVar[int] = 3
    VENUE: ClassVar[str] = "TESTVENUE"
    SYMBOL: ClassVar[str] = "RWALK"
    TF: ClassVar[str] = "M1"
    T0_US: ClassVar[int] = 1_700_000_000_000_000
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def setUp(self):
        self._tmpdir = TemporaryDirectory()
        self._folder = Path(self._tmpdir.name)
        self._prev_folder = FileReader.FOLDER_DBIN
        FileReader.FOLDER_DBIN = self._folder
        (self._folder / self.VENUE).mkdir(parents = True, exist_ok = True)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def tearDown(self):
        FileReader.FOLDER_DBIN = self._prev_folder
        self._tmpdir.cleanup()

    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def _write_csv(cls, path: Path, columns: List[str], rows: List[tuple]):
        lines = [str.join(",", columns)]
        for row in rows:
            lines.append(str.join(",", map(str, row)))
        path.write_text(str.join("\n", lines) + "\n")

    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def _random_walk_ticks(cls, n: int, seed: int = 0):
        rng = random.Random(seed)
        mid, spread = 100.0, 0.02
        rows = list[tuple]()
        for i in range(n):
            move = rng.gauss(0.0, 0.05)
            mid = max(1.0, mid + move)
            pa = mid + spread / 2.0
            pb = mid - spread / 2.0
            qa = abs(rng.gauss(1.0, 0.2)) + 0.01
            qb = abs(rng.gauss(1.0, 0.2)) + 0.01
            ts = cls.T0_US + i * 1_000_000
            rows.append((ts, pa, qa, pb, qb))
        return rows

    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def _random_walk_candles(cls, n: int, seed: int = 1):
        mid, step_us = 100.0, 60_000_000
        rng = random.Random(seed)
        rows = list[tuple]()
        for i in range(n):
            path = [mid]
            for _ in range(4):
                move = rng.gauss(0.0, 0.08)
                point = path[-1] + move
                path.append(max(1.0, point))
            oa, ca = path[0], path[-1]
            ha, la = max(path), min(path)
            ob, cb = oa - 0.01, ca - 0.01
            hb, lb = ha - 0.01, la - 0.01
            ts = cls.T0_US + i * step_us
            volume = int(abs(rng.gauss(50, 10)) + 1)
            rows.append((ts, oa, ha, la, ca, ob, hb, lb, cb, volume))
            mid = ca
        return rows

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def _time_gen(self, venue: str, symbol: str, tf: str):
        t0 = time.perf_counter()
        decoded = list(FileReader.gen(venue, symbol, tf, progbar = True))
        dus = time.perf_counter() - t0
        rate = (len(decoded) / dus) if (dus > 0) else float("inf")
        return decoded, dus, rate

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def test_csv_to_bin_gen_ticks(self):
        rows = self._random_walk_ticks(self.N, seed = 7)
        path_csv = self._folder / "ticks.csv"
        path_bin = self._folder / self.VENUE / f"{self.SYMBOL}{FileReader.EXT}"
        self._write_csv(path_csv, list(Tick.SCHEMA), rows)
        FileReader.csv_to_bin(path_csv, path_bin, model = Tick)
        decoded, dus, rate = self._time_gen(self.VENUE, self.SYMBOL, "T1")
        self.assertEqual(len(decoded), self.N)
        print("=" * 80, f"ticks n={self.N}")
        print(f" gen: {dus*1e3:.3f} ms | {rate:,.0f} entries/sec")
        for i, (raw, item) in enumerate(zip(rows, decoded)):
            t_us, pa, qa, pb, qb = raw
            Model, time_us, venue, symbol, *fields = item
            if (i < self.N_PRINT) or (i >= self.N - self.N_PRINT):
                print(f" => #{i + 1}:", item)
            self.assertIs(Model, Tick)
            self.assertEqual(venue, self.VENUE)
            self.assertEqual(symbol, self.SYMBOL)
            self.assertEqual(time_us, t_us)
            self.assertEqual(len(fields), 4)
            self.assertAlmostEqual(fields[0], pa, places = 4)
            self.assertAlmostEqual(fields[1], qa, places = 4)
            self.assertAlmostEqual(fields[2], pb, places = 4)
            self.assertAlmostEqual(fields[3], qb, places = 4)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def test_csv_to_bin_gen_candles(self):
        rows = self._random_walk_candles(self.N, seed = 11)
        path_csv = self._folder / "candles.csv"
        filename = f"{self.SYMBOL}_{self.TF}{FileReader.EXT}"
        path_bin = self._folder / self.VENUE / filename
        self._write_csv(path_csv, list(Candle.SCHEMA), rows)
        FileReader.csv_to_bin(path_csv, path_bin, model = Candle)
        decoded, dus, rate = self._time_gen(self.VENUE, self.SYMBOL, self.TF)
        self.assertEqual(len(decoded), self.N)
        print("=" * 80, f"candles/{self.TF} n={self.N}")
        print(f" gen: {dus*1e3:.3f} ms | {rate:,.0f} entries/sec")
        for i, (raw, item) in enumerate(zip(rows, decoded)):
            t_us, oa, ha, la, ca, ob, hb, lb, cb, volume = raw
            Model, time_us, venue, symbol, tf, *fields = item
            if (i < self.N_PRINT) or (i >= self.N - self.N_PRINT):
                print(f" => #{i + 1}:", item)
            self.assertIs(Model, Candle)
            self.assertEqual(venue, self.VENUE)
            self.assertEqual(symbol, self.SYMBOL)
            self.assertEqual(tf, self.TF)
            self.assertEqual(time_us, t_us)
            self.assertEqual(len(fields), 9)
            self.assertAlmostEqual(fields[0], oa, places = 4)
            self.assertAlmostEqual(fields[1], ha, places = 4)
            self.assertAlmostEqual(fields[2], la, places = 4)
            self.assertAlmostEqual(fields[3], ca, places = 4)
            self.assertAlmostEqual(fields[4], ob, places = 4)
            self.assertAlmostEqual(fields[5], hb, places = 4)
            self.assertAlmostEqual(fields[6], lb, places = 4)
            self.assertAlmostEqual(fields[7], cb, places = 4)
            self.assertEqual(int(fields[8]), volume)

#███████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class TestDataProvider(TestCase):
    # Drains DataProvider.main without Redis.stream (uses __wrapped__).
    N: ClassVar[int] = TestFileReader.N
    N_PRINT: ClassVar[int] = TestFileReader.N_PRINT
    VENUE: ClassVar[str] = TestFileReader.VENUE
    SYMBOL: ClassVar[str] = TestFileReader.SYMBOL
    T0_US: ClassVar[int] = TestFileReader.T0_US
    TF: ClassVar[str] = TestFileReader.TF
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def setUp(self):
        self._tmpdir = TemporaryDirectory()
        self._folder = Path(self._tmpdir.name)
        self._prev_folder = FileReader.FOLDER_DBIN
        FileReader.FOLDER_DBIN = self._folder
        path = self._folder / self.VENUE
        path.mkdir(parents = True, exist_ok = True)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def tearDown(self):
        FileReader.FOLDER_DBIN = self._prev_folder
        self._tmpdir.cleanup()

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def test_main_file_without_redis(self):
        n_ticks, n_candles = self.N, max(1, self.N // 60)
        tick_rows = TestFileReader._random_walk_ticks(n_ticks, seed = 7)
        candle_rows = TestFileReader._random_walk_candles(n_candles, seed = 11)
        path_tick_csv = self._folder / "ticks.csv"
        path_tick_bin = self._folder / self.VENUE / f"{self.SYMBOL}{FileReader.EXT}"
        path_candle_csv = self._folder / "candles.csv"
        path_candle_bin = self._folder / self.VENUE / \
            f"{self.SYMBOL}_{self.TF}{FileReader.EXT}"
        TestFileReader._write_csv(path_tick_csv, list(Tick.SCHEMA), tick_rows)
        TestFileReader._write_csv(path_candle_csv, list(Candle.SCHEMA), candle_rows)
        FileReader.csv_to_bin(path_tick_csv, path_tick_bin, model = Tick)
        FileReader.csv_to_bin(path_candle_csv, path_candle_bin, model = Candle)

        symbol = Symbol(venue = self.VENUE, symbol = self.SYMBOL)
        symbols = {(self.VENUE, self.SYMBOL): symbol}
        provider = DataProvider(reader_mode = "file", symbols = symbols,
            timeframes = {"T1", self.TF},
            time_since = Timestamp(self.T0_US, unit = "us", tz = TZ),
            time_until = Timestamp.max.tz_localize("UTC"))

        # Sync drain (improvement #4); main's body is the same iteration.
        t0 = time.perf_counter()
        items = list(provider.reader)
        dus = time.perf_counter() - t0
        rate = (len(items) / dus) if (dus > 0) else float("inf")
        expected = n_ticks + n_candles
        self.assertEqual(len(items), expected)
        print("=" * 80, f"DataProvider.reader sync drain n={expected}")
        print(f" drain: {dus*1e3:.3f} ms | {rate:,.0f} entries/sec")

        prev_key = None
        n_tick_out = n_candle_out = 0
        for i, item in enumerate(items):
            if (i < self.N_PRINT) or (i >= expected - self.N_PRINT):
                print(f" => #{i + 1}:", item)
            self.assertIsInstance(item, (Tick, Candle))
            self.assertEqual(item.symbol.venue, self.VENUE)
            self.assertEqual(item.symbol.symbol, self.SYMBOL)
            key = (item.time_event, item.symbol, isinstance(item, Candle))
            if (prev_key is not None): self.assertLessEqual(prev_key, key)
            if isinstance(item, Tick): n_tick_out += 1
            else: n_candle_out += 1
            prev_key = key
        self.assertEqual(n_tick_out, n_ticks)
        self.assertEqual(n_candle_out, n_candles)

        provider2 = DataProvider(reader_mode = "file", symbols = symbols,
                time_since = Timestamp(self.T0_US, unit = "us", tz = TZ),
                time_until = Timestamp.max.tz_localize("UTC"),
                timeframes = {"T1"})

        #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
        async def drain_async():
            gen = DataProvider.main.__wrapped__(provider2)
            return [item async for item in gen]

        async_items = asyncio.run(drain_async())
        self.assertEqual(len(async_items), n_ticks)
        self.assertTrue(all(isinstance(x, Tick) for x in async_items))

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def test_main_file_with_redis(self):
        n_ticks, n_candles = self.N, max(1, self.N // 60)
        expected = n_ticks + n_candles
        tick_rows = TestFileReader._random_walk_ticks(n_ticks, seed = 7)
        candle_rows = TestFileReader._random_walk_candles(n_candles, seed = 11)
        path_tick_csv = self._folder / "ticks.csv"
        path_tick_bin = self._folder / self.VENUE / f"{self.SYMBOL}{FileReader.EXT}"
        path_candle_csv = self._folder / "candles.csv"
        path_candle_bin = self._folder / self.VENUE / \
            f"{self.SYMBOL}_{self.TF}{FileReader.EXT}"
        TestFileReader._write_csv(path_tick_csv, list(Tick.SCHEMA), tick_rows)
        TestFileReader._write_csv(path_candle_csv, list(Candle.SCHEMA), candle_rows)
        FileReader.csv_to_bin(path_tick_csv, path_tick_bin, model = Tick)
        FileReader.csv_to_bin(path_candle_csv, path_candle_bin, model = Candle)

        symbol = Symbol(venue = self.VENUE, symbol = self.SYMBOL)
        symbols = {(self.VENUE, self.SYMBOL): symbol}
        provider = DataProvider(reader_mode = "file", symbols = symbols,
            timeframes = {"T1", self.TF},
            time_since = Timestamp(self.T0_US, unit = "us", tz = TZ),
            time_until = Timestamp.max.tz_localize("UTC"))
        # 0 => unbounded asyncio.Queue + no XADD maxlen trim.
        provider.maxlen_redis = 0
        prefix = provider.stream_prefix

        #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
        async def xlen_total():
            streams, total = list(), 0
            async for key in Redis._client.scan_iter(match = prefix + "|*"):
                streams.append(key)
                total += int(await Redis._client.xlen(key))
            return streams, total

        #▄▄▄▄▄▄▄▄▄▄▄▄▄▄
        async def run():
            Redis._ready.clear()
            Redis._pending.clear()
            Redis._streams.clear()
            provider.active = True
            redis_task = asyncio.create_task(Redis(provider),
                name = "TestDataProvider/Redis")
            await Redis.wait()
            t0 = time.perf_counter()
            results = await provider.main()
            # Wait until Redis reflects every enqueued entry.
            streams, total = list(), 0
            for _ in range(120_000):
                qsize = 0 if (Redis._queue is None) else Redis._queue.qsize()
                streams, total = await xlen_total()
                if (qsize == 0) and (total >= len(results)): break
                await asyncio.sleep(1e-3)
            dus = time.perf_counter() - t0
            provider.active = False
            redis_task.cancel()
            try: await redis_task
            except asyncio.CancelledError: pass
            if streams: await Redis._client.delete(*streams)
            return results, dus, streams, total

        results, dus, streams, total = EventLoop.run_until_complete(run())
        rate = (len(results) / dus) if (dus > 0) else float("inf")
        self.assertEqual(len(results), expected)
        self.assertEqual(total, expected)
        print("=" * 80, f"DataProvider.main + Redis n={expected}")
        print(f" drain+xadd: {dus*1e3:.3f} ms | {rate:,.0f} entries/sec")
        print(f" streams ({len(streams)}):", streams[: 5],
              ("..." if len(streams) > 5 else ""))
        n_tick_out = sum(isinstance(x, Tick) for x in results)
        n_candle_out = sum(isinstance(x, Candle) for x in results)
        self.assertEqual(n_tick_out, n_ticks)
        self.assertEqual(n_candle_out, n_candles)
        for i, item in enumerate(results):
            if (i < self.N_PRINT) or (i >= expected - self.N_PRINT):
                print(f" => #{i + 1}:", item)

#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
import heapq, random, struct, time, numpy
from tqdm import tqdm
from pathlib import Path
from unittest import TestCase
from tempfile import TemporaryDirectory
from pandas import Timestamp, Timedelta
from pandas import Series, DataFrame, concat
from typing import List, Tuple, Dict, Set
from typing import Iterable, Callable
from typing import Mapping, ClassVar, TextIO
from mmap import mmap, ACCESS_READ as MMAP_READ
from src.models import Tick, Candle
from src.models import TimeFrame, Symbol, SymbolDict
from src.utils import ClickHouse, Config, TZ

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

#███████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
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
ABList = List[Tuple[float, float]]
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class FakeSymbol(Symbol):
    DEF_TICKS: ClassVar[int] = 10000
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __init__(self, venue: str, symbol: str,
          prices_init: ABList, model: Callable,
          ticks: int = None):

        price = prices_init[0][0]
        min_price_diff = self.naive_point(price)
        min_order_size, quote_value = 0.01, 1.0
        min_stops_diff = 10 * min_price_diff
        contract_size = 1 / min_price_diff

        self.prices = prices_init.copy()
        self.model, self.ticks = model, ticks
        super().__init__(venue, symbol, symbol, "USD", min_stops_diff,
          min_price_diff, min_order_size, contract_size, quote_value)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __next__(self):
        if (self.ticks is not None):
            if (self.ticks > 0): self.ticks = self.ticks - 1
            else: raise StopIteration(f"\"{self!r}\" depleted")
        try: ask, bid = self.model(self.prices)
        except Exception as EXC: raise StopIteration(repr(EXC))
        self.prices.append((ask, bid))
        self.prices.pop(0)
        return (ask, bid)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __iter__(self):
        while True: yield next(self)

# Fake symbols with predefined models
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class FakeSymbolLinear(FakeSymbol):
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __init__(self, symbol_key: Tuple[str, str], a0: float,
              b0: float, trend: int = None, ticks: int = None):
        super().__init__(*symbol_key, [(a0, b0)], self.model, ticks)
        self.trend = trend if trend else +1
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def model(self, prices: ABList):
        ask, bid = prices[-1]
        incr = self.trend * self.min_price_diff
        return (ask + incr, bid + incr)

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class FakeSymbolCyclic(FakeSymbol):
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __init__(self, symbol_key: Tuple[str, str], a0: float,
          b0: float, ampl: float, period: int, ticks: int = None):
        diff_init = ampl * numpy.sin(freq := 2 * numpy.pi / period)
        prices_init = [(a0, b0), (a0 + diff_init, b0 + diff_init)]
        super().__init__(*symbol_key, prices_init, self.model, ticks)
        self.cosfq = numpy.cos(freq)
        self.off_ask = 2 * a0 * (1 - self.cosfq)
        self.off_bid = 2 * b0 * (1 - self.cosfq)
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def model(self, prices: ABList):
        a1, b1, a2, b2 = *prices[-1], *prices[-2]
        ad = self.off_ask + (2 * self.cosfq - 1) * a1 - a2
        bd = self.off_bid + (2 * self.cosfq - 1) * b1 - b2
        ad = numpy.sign(ad) * max(abs(ad), self.min_price_diff)
        bd = numpy.sign(bd) * max(abs(bd), self.min_price_diff)
        return (a1 + ad, b1 + bd)

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class FakeSymbolRandom(FakeSymbol):
    DEF_DRIFT, DEF_SPRC = 0, 0.0001
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __init__(self, symbol_key: Tuple[str, str], a0: float, stdev: float,
          drift: float = None, max_spread: float = None, ticks: int = None):
        self.max_spread = max_spread if max_spread else a0 * self.DEF_SPRC
        super().__init__(*symbol_key, [(a0, a0)], self.model, ticks)
        self.stdev, self.drift = stdev, (drift if drift else 0.0)
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def model(self, prices: ABList):
        spread = numpy.random.uniform() * self.max_spread
        diff = numpy.random.normal() * self.stdev + self.drift
        diff = numpy.sign(diff) * max(abs(diff), self.min_price_diff)
        ask = prices[-1][0] + diff
        return (ask, ask - spread)
    
#███████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class FakeReader(DataReader):
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __init__(self, fake_symbols: list[FakeSymbol], timeframes: Set[str],
          time_since: Timestamp, time_until: Timestamp, ticks: int = None):
        
        symbols = SymbolDict()
        iters = list[Iterable[RowTuple]]()
        gen_args = (timeframes, time_since, time_until)
        for symbol in fake_symbols:
            if ticks: symbol.ticks = ticks
            else: symbol.ticks = symbol.DEF_TICKS
            symbol_key = (symbol.venue, symbol.symbol)
            iters.append(self.gen(symbol, *gen_args))
            symbols[symbol_key] = symbol
    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def gen(self, symbol: FakeSymbol, timeframes: Set[str],
          since: Timestamp = None, until: Timestamp = None):

        ms = Timedelta(milliseconds = 1)
        symbol_key = (symbol.venue, symbol.symbol)
        if since is None: since = Timestamp.now(tz = TZ).floor("D")
        if until is None: until = since + ms * symbol.DEF_TICKS
        dft = DataFrame(symbol, columns = ["pa", "pb"])
        time_step = (until - since) / (dft.shape[0] - 1)
        dft.index = (dft.index * time_step) + since
        dft = dft.rename_axis("time_event")
        dfc: dict[TimeFrame, Series[dict]] = dict()
        tf: TimeFrame = None
        
        for tfs in timeframes:
            if tfs not in TimeFrame: continue
            else: tf = TimeFrame[tfs]
            group = dft.resample(tf.value)
            dfa, dfb = group["pa"].ohlc(), group["pb"].ohlc()
            dfa.columns, dfb.columns = list("ohlc"), list("ohlc")
            df = DataFrame.merge(dfa, dfb, suffixes = list("ab"),
              how = "outer", left_index = True, right_index = True)
            df["volume"], df["time"] = group["pa"].count(), df.index
            df = df.apply(dict, axis = "columns")
            df.index = df.index + tf.value
            dfc[tf] = df

        dft["tf"] = None
        index = ["tf", "time_event"]
        dfc: Series = concat(dfc, names = index)
        dft = dft.reset_index().set_index(index)
        dft = dft.apply(dict, axis = "columns")
        if "T1" in timeframes: dfc = concat((dfc, dft))
        dfc: Series[dict] = dfc.swaplevel().sort_index()

        for (tf, time_event), payload in dfc.items():
            time_us = int(Timestamp.timestamp(time_event) * 1e6)
            if not tf: yield (Tick, time_us, *symbol_key, *payload.values())
            else: yield (Candle, time_us, *symbol_key, tf.name, *payload.values())

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


#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
import heapq, struct, time
from dataclasses import field
from io import TextIOWrapper
from pathlib import Path
from mmap import mmap, ACCESS_READ as MMAP_READ
from typing import List, Tuple, Dict, Set, Mapping
from typing import Iterable, Callable, Generator
from pandas import Timestamp
from tqdm import tqdm
from src.models import StreamingAgent
from src.models import TimeFrame, Symbol
from src.models import DataPoint, Tick, Candle
from src.utils import ClickHouse, Redis, b64, Config, TZ

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

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __aiter__(self): return self

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def _pull(self, src_idx: int):
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

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def __anext__(self):
        if not self._buffer: raise StopAsyncIteration
        item, src_idx = heapq.heappop(self._buffer)
        self._pull(src_idx)
        return item

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
class FileReader(DataReader):
    # Uncompressed little-endian .bin (mmap-friendly):
    #   magic[4]="LTXB" | version:u8
    #   rows packed by model.SCHEMA (time always i64 µs first)
    # Schema lives on the model class (Tick / Candle / DataPoint subclass), not in-file.
    EXT, MAGIC, VERSION = ".bin", b"LTXB", 4
    DTYPES = {"i64": (1, "q", int), "f32": (2, "f", float),
              "f64": (3, "d", float), "i32": (4, "i", int)}
    VERBOSE_EMPTY_CSV = "Empty CSV from given path: \"{0}\""
    VERBOSE_UNSUPPORTED = "Unsupported bin version {0} in given path: \"{1}\""
    VERBOSE_NO_TIME_FIELD = "Leftmost column must be \"time\", got: \"{0}\""
    VERBOSE_BAD_HEADER = "CSV columns \"{0}\" do not match expected \"{1}\""
    VERBOSE_SCHEMA_NONE = "Model \"{0}\" must define a SCHEMA class attribute"
    VERBOSE_SCHEMA_BAD = "Invalid schema on given model \"{0}\": \"{1}\""
    VERBOSE_BAD_MAGIC = "Bad magic in given path \"{0}\": \"{1!r}\""
    VERBOSE_BAD_FIELDS = "Expected {0} fields, got {1}: \"{2}\""
    VERBOSE_SCHEMA_NO_TIME = VERBOSE_SCHEMA_BAD.format("{0}",
                        f"first entry must be \"i64\" (time)")
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __init__(self, symbols: Dict[Tuple[str, str], Symbol],
                time_since: Timestamp, time_until: Timestamp,
                timeframes: Set[str], buffer: int = None):

        PATH = Config.FOLDER_DBIN
        iters = list[Iterable[Dict]]()
        self._timeframes = timeframes.copy()
        for symbol_key, symbol in symbols.items():
            venue_name, symbol_name = symbol_key
            path = PATH / venue_name / symbol_name
            if (symbol is None) and path.exists():
                iters.append(self.gen(path, DataPoint)); continue
            for tf in timeframes:
                path_tf = path + f"_{tf}.{self.EXT}"
                model = Tick if (tf == "T1") else Candle
                iters.append(self.gen(path_tf, model))
        
        super().__init__(iters, time_since, time_until, timeframes, buffer)

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
        if ((expected := list(schema)) != columns):
            raise ValueError(cls.VERBOSE_BAD_HEADER.format(columns, expected))
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
        file_from: TextIOWrapper = None
        file_to: TextIOWrapper = None

        with open(**args_csv) as file_from, open(**args_bin) as file_to:
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
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def gen(cls, path: Path, model: type):
        schema = cls._schema_of(model)
        value_cols = list(schema)[1 :]
        row_formatted = cls._row_struct(schema)
        row_size = row_formatted.size
        with open(path, "rb") as file:
            memory = mmap(file.fileno(), 0, access = MMAP_READ)
            try:
                magic = memory[: 4]
                if (magic != cls.MAGIC): raise ValueError(
                    cls.VERBOSE_BAD_MAGIC.format(path.name, magic))
                (version,) = struct.unpack_from("<B", memory, 4)
                if (version != cls.VERSION): raise ValueError(
                    cls.VERBOSE_UNSUPPORTED.format(version, path.name))
                offset = 5
                end = memory.size()
                while (offset + row_size <= end):
                    time_us, *values = row_formatted.unpack_from(memory, offset)
                    item = dict(zip(value_cols, values, strict = True))
                    item["time"] = Timestamp(time_us, unit = "us", tz = TZ)
                    if ("volume" in item): item["volume"] = int(item["volume"])
                    yield item
                    offset += row_size
                if (offset != end): raise ValueError(f"Truncated row in {path}")
            finally: memory.close()

#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
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
    @Redis.stream#█▄▄▄▄▄
    async def main(self):
        async for item in self.reader: yield item
    
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __post_init__(self):
        self.reader_mode = self.reader_mode.upper()
        self.stream_prefix = "BTX-" + b64(time.time())
        args = {"timeframes": self.timeframes, "symbols": self.symbols,
            "time_since": self.time_since, "time_until": self.time_until}
        if (self.reader_mode == "file"): self.reader = FileReader(**args)
        elif (self.reader_mode == "tsdb"): self.reader = TSDBReader(**args)
        else: raise ValueError(f"Invalid reader mode: {self.reader_mode}")
        super().__post_init__()
    
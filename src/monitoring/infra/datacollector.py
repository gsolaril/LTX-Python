#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
import asyncio
from typing import ClassVar
from collections import deque
from pandas import DataFrame, concat
from pandas import Timestamp, Timedelta
from dataclasses import dataclass, field
from src.models import TimeFrame, StreamingAgent
from src.models import Quote, Tick, Candle, Symbol
from src.utils import *

#███████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄
@dataclass#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class DataCollector(StreamingAgent):
    maxlen_local: int = field(init = False, kw_only = True, default = 500000)
    batch_size: int = field(init = False, kw_only = True, default = 100000)
    freq_write_batch: int = field(init = False, kw_only = True, default = 5)
    freq_write_report: int = field(init = False, kw_only = True, default = 60)
    freq_scan: int = field(init = False, kw_only = True, default = 60)
    tfs: str = field(init = False, kw_only = True, default = "S1 M1")
    TABLE_CONFIG: ClassVar[Postgres.Table] = Postgres.Table.MONITORING
    TS_CANDLES: ClassVar[ClickHouse.Table] = ClickHouse.Table.CANDLES
    TS_TICKS: ClassVar[ClickHouse.Table] = ClickHouse.Table.TICKS
    STREAM_PREFIX: ClassVar[str] = "DATA"
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __post_init__(self):
        super().__post_init__()
        self._xstreams = dict[str, str]()
        self._queues: dict[str, asyncio.Queue] = {
            Tick: asyncio.Queue(maxsize = self.maxlen_local),
            Candle: asyncio.Queue(maxsize = self.maxlen_local)}
        self._crons[self.scan] = Timedelta(seconds = self.freq_scan)
        self._crons[self.write_batch] = Timedelta(seconds = self.freq_write_batch)
        self._crons[self.write_report] = Timedelta(seconds = self.freq_write_report)
        self._crons[self.redis_report] = Timedelta(seconds = self.freq_redis_report)
        self._reporter = Reporter(name = "DataCollector")
        self._scan_ready = asyncio.Event()
        self._recorded: DataFrame = None
        self.config_verbose()
 
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def setup(self):
        tasks = await super().setup()
        tasks.append(asyncio.create_task(
            self.main(), name = f"{self.name}/main"))
        return tasks

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def write(self, queue: asyncio.Queue):
        copy = deque(maxlen = self.batch_size)
        while (len(copy) < self.batch_size):
            if queue.empty(): break
            copy.append(queue.get_nowait())
        while copy:
            item: Quote = copy.popleft()
            row = dict(item.__dict__["payload"])
            row["venue"] = item.symbol.venue
            row["symbol"] = item.symbol.symbol
            row["time"] = item.time
            if isinstance(item, Candle):
                row["tf"] = item.tf.name
            yield row
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    @ClickHouse.to_table(table = TS_TICKS)#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def write_ticks(self): yield from self.write(self._queues[Tick])
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    @ClickHouse.to_table(table = TS_CANDLES)#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def write_candles(self): yield from self.write(self._queues[Candle])

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def write_batch(self):
        agg = {"since": "min", "until": "max", "count": "count"}

        df = DataFrame(columns = Tick.INDEX_KEYS)
        columns, data = await self.write_ticks.flush()
        if data: df = DataFrame(data, columns = columns)
        df = df[Tick.INDEX_KEYS]
        df["tf"] = "T1"

        dfc = DataFrame(columns = Candle.INDEX_KEYS)
        columns, data = await self.write_candles.flush()
        if data: dfc = DataFrame(data, columns = columns)
        dfc = dfc[Candle.INDEX_KEYS]

        df = concat((df, dfc)).set_index(Candle.INDEX_KEYS[: -1])["time"]
        if df.empty: return Log.warning("No rows written to ClickHouse")
        df = df.groupby(df.index.names).agg([*agg.values()])
        df.columns = agg.keys()
        agg["count"] = "sum"

        if (self._recorded is not None):
            keys = Candle.INDEX_KEYS[: -1]
            df: DataFrame = concat((self._recorded, df))
            self._recorded = df.groupby(keys).agg(agg)
        else: self._recorded = df

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def write_report(self):
        if (self._recorded is None): return
        df = self._recorded.reset_index("tf")
        # "YYYY-MM-DD HH:MM:SS" => "MM/DD HH:MM:SS"
        df["since"] = df["since"].astype(str).str[5 : 19]
        df["until"] = df["until"].astype(str).str[5 : 19]
        is_small_tf = df["tf"].str.startswith("T")
        is_small_tf |= df["tf"].str.startswith("S")
        large_tf: DataFrame = df.loc[~ is_small_tf]
        # (tf >= M1) => "MM/DD HH:MM:SS" => "MM/DD HH:MM"
        df.loc[large_tf.index, "since"] = large_tf["since"].str[: -3]
        df.loc[large_tf.index, "until"] = large_tf["until"].str[: -3]
        df = df.set_index("tf", append = True).sort_index().unstack("tf")
        df = df.swaplevel(axis = "columns").sort_index(axis = "columns")
        Log.success("Wrote to ClickHouse...\n" + df.to_string(max_rows = 20))

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def scan(self):
        new = set[str]()
        self._reporter.add("*Scanning")
        pat = Redis.STREAM_PREFIX + "|*"
        self._tfs = set(self.tfs.split(" "))
        suffixes = self._tfs.union({"T1"})
        async for stream in Redis.scan(pat):
            tf = str.split(stream, "|")[-1]
            if tf not in suffixes: continue
            if stream not in self._xstreams:
                self._xstreams[stream] = Redis.StreamGet.NEW.value
                new.add(stream)
        self._scan_ready.set()
        if self._xstreams:
            await Redis.ensure_groups(list(self._xstreams))
        if not new: return
        new_str = str.join(", ", sorted(new))
        Log.info(f"New streams:\n => {new_str}")

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def redis_report(self):
        freq = self._crons[self.redis_report]
        next_at = Timestamp.now(TZ).ceil(freq)
        report = self._reporter.to_string(next_at)
        self._reporter.close_batch()
        if report: Log.info(report)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def process(self, stream: str, message_id: str, payload: dict):
        self._reporter.add(stream)
        ms, us = map(int, message_id.split("-"))
        _, _, venue, symbol, tf = stream.split("|")
        payload["symbol"] = Symbol(venue = venue, symbol = symbol)
        payload["time"] = Timestamp(ms * 1e3 + us, unit = "us", tz = "UTC")
        if tf in self._tfs:
            payload["volume"] = int(payload.pop("volume", 0))
            obj = Candle(**payload, tf = TimeFrame[tf])
            await self._queues[Candle].put(obj)
        elif tf.startswith("T"):
            obj = Tick(**payload)
            await self._queues[Tick].put(obj)
            
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def main(self):
        await self._scan_ready.wait()
        while True:
            try:
                response = await Redis.xreadgroup(self,
                    Redis.Group.MONITOR, self._xstreams)
                if not response: continue
                for stream, messages in response:
                    for message_id, payload in messages:
                        if not payload: continue
                        await self.process(stream, message_id, payload)
                        await Redis.xack(Redis.Group.MONITOR, stream, message_id)
            except asyncio.CancelledError: break
            except Exception as EXC: Log.exception(EXC); break
        
#███████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
import asyncio
from tkinter.constants import NONE
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
    batch_size: int = field(init = False, kw_only = True, default = 1000)
    freq_scan: int = field(init = False, kw_only = True, default = 60)
    tfs: str = field(init = False, kw_only = True, default = "S1 M1")
    STREAM_PREFIX: ClassVar[str] = "DATA"
    TS_TICKS: ClassVar[ClickHouse.Table] = ClickHouse.Table.TICKS
    TS_CANDLES: ClassVar[ClickHouse.Table] = ClickHouse.Table.CANDLES
    TABLE_CONFIG: ClassVar[Postgres.Table] = Postgres.Table.MONITORING
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __post_init__(self):
        super().__post_init__()
        self._xstreams = dict[str, str]()
        self._queues: dict[str, asyncio.Queue] = {
            Tick: asyncio.Queue(maxsize = self.maxlen),
            Candle: asyncio.Queue(maxsize = self.maxlen)}
        self._crons[self.scan] = Timedelta(seconds = self.freq_scan)
        self._crons[self.report] = Timedelta(seconds = self.freq_redis_report)
        self._crons[self.record] = TimeFrame.M1.value
        self._reporter = Reporter(name = "DataCollector")
        self._scan_ready = asyncio.Event()
        self._recorded = None
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
        while len(copy) < copy.maxlen:
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
    async def record(self):
        Log.info("Recording...")
        agg = {"first": "since", "last": "until", "count": "count"}

        dft = DataFrame(columns = Tick.INDEX_KEYS)
        columns, data = await self.write_ticks.flush()
        if data: dft = DataFrame(data, columns = columns)
        dft = dft[Tick.INDEX_KEYS]
        dft["tf"] = "T1"

        dfc = DataFrame(columns = Candle.INDEX_KEYS)
        columns, data = await self.write_candles.flush()
        if data: dfc = DataFrame(data, columns = columns)
        dfc = dfc[Candle.INDEX_KEYS]

        df = concat((dft, dfc)).sort_index()
        df = df.set_index(Candle.INDEX_KEYS[: -1])["time"]
        if df.empty: return Log.warning("No rows written to ClickHouse")
        df = df.groupby(df.index.names).agg([*agg.keys()])
        df = df.rename(columns = agg, errors = "ignore")

        if self._recorded is not None:
            new = df.loc[df.index.difference(self._recorded.index)]
            self._recorded = concat((self._recorded, new), axis = "index")
            self._recorded["count"] = self._recorded["count"].fillna(0)
            self._recorded["count"] = self._recorded["count"] + df["count"]
            self._recorded["since"] = self._recorded["since"].fillna(df["since"])
            self._recorded["until"] = df["until"]
        else: self._recorded = df

        df = df.reset_index("tf")
        sub_minute = df["tf"].str[0].isin({*"ST"})
        df["since"] = df["since"].dt.strftime("%m/%d %H:%M:%S")
        df["until"] = df["until"].dt.strftime("%m/%d %H:%M:%S")
        df.loc[~ is_sub_minute, "until"] = df["until"].str[: -2]
        df = df.set_index("tf", append = True).sort_index()
        df["since"] = df["since"].str[: -2]

        df = self._recorded.unstack("tf")
        df = df.swaplevel(axis = "columns")
        df = df.sort_index(axis = "columns")
        verbose = "Wrote to ClickHouse...\n"
        verbose += df.to_string(max_rows = 20)
        Log.success(verbose)

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
                self._xstreams[stream] = ">"
                new.add(stream)
        self._scan_ready.set()
        if self._xstreams:
            await Redis.ensure_groups(list(self._xstreams))
        if not new: return
        new_str = str.join(", ", sorted(new))
        Log.info(f"New streams:\n => {new_str}")

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def report(self):
        freq = self._crons[self.report]
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
                    RedisGroup.MONITOR, self._xstreams)
                if not response: continue
                for stream, messages in response:
                    for message_id, payload in messages:
                        if not payload: continue
                        await self.process(stream, message_id, payload)
                        await Redis.xack(RedisGroup.MONITOR, stream, message_id)
            except asyncio.CancelledError: break
            except Exception as EXC: Log.exception(EXC); break
        
#███████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
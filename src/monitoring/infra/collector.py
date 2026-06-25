#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
import os, sys, asyncio
from dataclasses import dataclass, field
from pandas import Series, DataFrame
from pandas import Timestamp, Timedelta
from src.models import *
from src.utils import *

#███████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class StreamingBundle(Bundle):
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    @Redis.on_stream#█▄▄▄▄▄▄▄▄▄▄▄
    def on_tick(self, tick: Tick):
        super().on_tick(tick)
        return tick
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    @Redis.on_stream#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def on_candle(self, candle: Candle):
        super().on_candle(candle)
        return candle

#▄▄▄▄▄▄▄▄▄▄▄
@dataclass#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class Collector(BaseAgent):
    freq_scan: int = field(init = False, kw_only = True, default = 60)
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __post_init__(self):
        super().__post_init__()
        self._xstreams = dict[str, str]()
        self._reporter = Reporter(name = "Collector")
        self.maxlen = max(self.maxlen, 1000000)
        self._bundle = StreamingBundle(maxlen = self.maxlen)
        self._crons[self.scan] = Timedelta(seconds = self.freq_scan)
        self._crons[self.report] = Timedelta(seconds = self.freq_report)
        self._crons[self.resample] = Timedelta(seconds = TimeFrame.MIN.value)
        self._scan_ready = asyncio.Event()

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def setup(self):
        name = f"{self.name}/main"
        tasks, logs = await super().setup()
        tasks[name] = asyncio.create_task(self.main(), name = name)
        logs.append(self.VERBOSE_TASK.format(name, tasks[name]))
        return tasks, logs

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def resample(self):
        last = Timestamp.now("UTC").floor(TimeFrame.MIN.value)
        ticks, candles = self._bundle.resample_ticks(last)
        candles.extend(self._bundle.resample_candles(last))
        await self.write_ticks(ticks)
        await self.write_candles(candles)
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def write(self, series: list[Tick | Candle]):
        __dict__ = lambda X: X.__dict__
        gen_series = map(__dict__, series)
        return DataFrame(gen_series)
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    @ClickHouse.to_series(TS_TICKS := "history_ticks")
    def write_ticks(self, series: list[Tick]):
        return self.write(series).set_index(Tick.INDEX_KEYS)
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    @ClickHouse.to_series(TS_CANDLES := "history_candles")
    def write_candles(self, series: list[Candle]):
        return self.write(series).set_index(Candle.INDEX_KEYS)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def scan(self):
        new = set[str]()
        self._reporter.add("*Scanning")
        async for stream in Redis.scan():
            if not stream in self._xstreams:
                self._reporter.add(stream)
                self._xstreams[stream] = ">"
                new.add(stream)
        self._scan_ready.set()
        if not new: return
        new_str = str.join(", ", sorted(new))
        Log.info(f"New streams:\n => {new_str}")

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def report(self):
        freq = self._crons[self.report]
        next_at = Timestamp.now("UTC").ceil(freq)
        Log.info(self._reporter.to_string(next_at))

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def process(self, stream: str, message_id: str, payload: dict):
        self._reporter.add(stream)
        ms, us = map(int, message_id.split("-"))
        _, _, venue, symbol, tfs = stream.split("|")
        payload["symbol"] = Symbol(venue = venue, symbol = symbol)
        payload["time"] = Timestamp(ms * 1e3 + us, unit = "us", tz = "UTC")
        if (tfs == "T1"):
            self._bundle.on_tick(Tick(**payload))
        elif tfs in TimeFrame:
            payload["tf"] = TimeFrame[tfs]
            self._bundle.on_candle(Candle(**payload))

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
                        self.process(stream, message_id, payload)
            except asyncio.CancelledError: break
            except Exception as EXC: Log.exception(EXC); break
        
#███████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
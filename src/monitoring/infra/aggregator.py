#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
import asyncio
from typing import ClassVar
from collections import deque
from pandas import Series, DataFrame
from pandas import Timestamp, Timedelta
from dataclasses import dataclass, field
from src.models import *
from src.utils import *

#███████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class StreamingBundle(Bundle):
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def resample_candles(self, time: Timestamp = None):
        recent: deque = super().resample_candles(time)
        candle: Candle = None
        for candle in recent:
            if (candle.tf <= TimeFrame.MIN): continue
            if not self._has_data(candle): continue
            EventLoop.create_task(self.publish_candle(candle))
            print(candle)
        return recent
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    @Redis.on_stream#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def publish_candle(self, candle: Candle):
        return candle

#▄▄▄▄▄▄▄▄▄▄▄
@dataclass#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class Aggregator(BaseAgent):
    freq_scan: int = field(init = False, kw_only = True, default = 60)
    STREAM_PREFIX: ClassVar[str] = "DATA"
    TS_TICKS: ClassVar[str] = "history_ticks"
    TS_CANDLES: ClassVar[str] = "history_candles"
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __post_init__(self):
        super().__post_init__()
        self._xstreams = dict[str, str]()
        self._reporter = Reporter(name = "Aggregator")
        self._crons[self.scan] = Timedelta(seconds = self.freq_scan)
        self._crons[self.report] = Timedelta(seconds = self.freq_report)
        self._crons[self.resample] = TimeFrame.MIN.value
        self._bundle = StreamingBundle(maxlen = self.maxlen)
        self._scan_ready = asyncio.Event()

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def setup(self):
        tasks = await super().setup()
        tasks.append(asyncio.create_task(
            self.main(), name = f"{self.name}/main"))
        return tasks

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def resample(self):
        last = Timestamp.now(TZ).floor(TimeFrame.MIN.value)
        result = self._bundle.resample_ticks(last)
        if result is None: return
        ticks, candles = result
        candles.extend(self._bundle.resample_candles(last))
        if ticks: await self.write_ticks(list(ticks))
        if candles: await self.write_candles(list(candles))
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def write(self, series: list[Tick | Candle]):
        for item in series:
            row = item.__dict__["payload"]
            row["venue"] = item.symbol.venue
            row["symbol"] = item.symbol.symbol
            row["time"] = item.time
            if isinstance(item, Candle):
                row["tf"] = item.tf.name
            yield row
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    @ClickHouse.to_series(series = TS_TICKS)
    def write_ticks(self, series: list[Tick]):
        yield from self.write(series)
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    @ClickHouse.to_series(series = TS_CANDLES)
    def write_candles(self, series: list[Candle]):
        candles = [c for c in series
            if (c.volume > 0) or (c.oa is not None)]
        yield from self.write(candles)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def scan(self):
        new = set[str]()
        self._reporter.add("*Scanning")
        async for stream in Redis.scan():
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
        self._reporter.close_batch()
        report = self._reporter.to_string(next_at)
        if report: Log.info(report)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def process(self, stream: str, message_id: str, payload: dict):
        self._reporter.add(stream)
        ms, us = map(int, message_id.split("-"))
        _, _, venue, symbol, tfs = stream.split("|")
        payload["symbol"] = Symbol(venue = venue, symbol = symbol)
        payload["time"] = Timestamp(ms * 1e3 + us, unit = "us", tz = "UTC")
        for key in ("pa", "qa", "pb", "qb", "oa", "ha", "la", "ca",
                    "ob", "hb", "lb", "cb", "volume", "dus"):
            if key in payload and payload[key] is not None:
                payload[key] = float(payload[key])
        if (tfs == "T1"):
            self._bundle.on_tick(Tick(**payload))
        elif tfs in TimeFrame:
            payload["tf"] = TimeFrame[tfs]
            payload["volume"] = int(payload.get("volume", 0) or 0)
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
                        await self.process(stream, message_id, payload)
                        await Redis.xack(RedisGroup.MONITOR, stream, message_id)
            except asyncio.CancelledError: break
            except Exception as EXC: Log.exception(EXC); break
        
#███████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
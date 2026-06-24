#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
import os, sys, asyncio
from pathlib import Path
from argparse import ArgumentParser
from typing import Any, Callable, ClassVar
from pandas import Series, DataFrame, Timestamp, Timedelta
from pandas import concat, Index, DatetimeIndex, MultiIndex

_ROOT = Path(__file__).resolve().parents[2]
_SRC = _ROOT / "src"
for _path in (_ROOT, _SRC, _SRC / "models", _SRC / "utils"):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

from src.models import *
from src.utils import *

#███████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class Collector:
    DEFAULT_MAXLEN = 100000
    DEFAULT_FREQ_SCAN = 60
    DEFAULT_FREQ_REPORT = 300
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __init__(self, maxlen: int = DEFAULT_MAXLEN,
                 freq_scan: int = DEFAULT_FREQ_SCAN,
                 freq_report: int = DEFAULT_FREQ_REPORT):

        self.maxlen = maxlen
        self.freq_scan = Timedelta(seconds = freq_scan)
        self.bundle, self.xstreams = Bundle(), dict[str, str]()
        self.reports = {"*Scanning": Report("*Scanning")}
        self._crons = {
            self.scan: Timedelta(seconds = freq_scan),
            self.report: Timedelta(seconds = freq_report)}
        self._ready = asyncio.Event()

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def start_cron(self, cron: Callable):
        now = Timestamp.now("UTC")
        freq = self._crons[cron]
        next = now.floor(freq)
        while True:
            await asyncio.sleep(0.1)
            now = Timestamp.now("UTC")
            if (now < next): continue
            next = now.ceil(freq)
            try: await cron()
            except Exception as EXC:
                Log.exception(EXC)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def scan(self):
        new = set[str]()
        async for stream in Redis.scan():
            if not stream in self.xstreams:
                self.reports[stream] = Report(stream)
                self.xstreams[stream] = ">"
                new.add(stream)
        self._ready.set()
        self.reports["*Scanning"]._incr()
        if not new: return
        new_str = str.join(", ", sorted(new))
        Log.info(f"New streams:\n => {new_str}")

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def report(self):
        next_at = Timestamp.now("UTC").ceil(self._crons[self.report])
        Log.info(Report.multi_close(self.reports.values(), next_at))

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def process(self, stream: str, message_id: str, payload: dict):
        self.reports[stream]._incr()
        ms, us = map(int, message_id.split("-"))
        prefix1, prefix2, venue, symbol, tfs = stream.split("|")
        payload["symbol"] = Symbol(venue = venue, symbol = symbol)
        payload["time"] = Timestamp(ms * 1e3 + us, unit = "us", tz = "UTC")
        if (tfs == "T1"):
            self.bundle.on_tick(Tick(**payload))
        elif tfs in TimeFrame:
            payload["tf"] = TimeFrame[tfs]
            self.bundle.on_candle(Candle(**payload))

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def main(self):
        await self._ready.wait()
        while True:
            try:
                response = await Redis.xreadgroup(self,
                    RedisGroup.MONITOR, self.xstreams)
                if not response: continue
                for stream, messages in response:
                    for message_id, payload in messages:
                        if not payload: continue
                        self.process(stream, message_id, payload)
            except asyncio.CancelledError: break
            except Exception as EXC: Log.exception(EXC); break

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def start(self):
        self._tasks = {
            "scan": asyncio.create_task(self.start_cron(self.scan)),
            "report": asyncio.create_task(self.start_cron(self.report)),
            "main": asyncio.create_task(self.main()),
        }
        tasks = await asyncio.gather(
            *self._tasks.values(),
            return_exceptions = True)
        for task in tasks:
            if (task is None): continue
            try: raise task
            except Exception as EXC:
                Log.exception(EXC)
        
#███████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
if (__name__ == "__main__"):

    parser = ArgumentParser()
    parser.add_argument("maxlen", nargs = "?", type = int, default = 10000)
    args = parser.parse_args()
    maxlen = args.maxlen
    collector = Collector(maxlen)
    EventLoop.run_until_complete(collector.start())
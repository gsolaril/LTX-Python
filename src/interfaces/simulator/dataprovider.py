#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
import asyncio, time
from dataclasses import dataclass, field
from collections import deque
from pathlib import Path
from pandas import Timestamp
from unittest import TestCase
from tempfile import TemporaryDirectory
from typing import Set, ClassVar, Tuple
from src.models import StreamingAgent, Tick, Candle
from src.models import Symbol, SymbolDict
from src.utils import Redis, b64
from src.utils import TZ, EventLoop, Log
from .datareader import FileReader, TSDBReader, TestFileReader

#███████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄
@dataclass#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class DataProvider(StreamingAgent):
    STREAM_PREFIX: ClassVar[str] = "BTX-"
    XGROUP: ClassVar[str] = Redis.Group.DATA
    id: str = field(init = True, default_factory = b64)
    symbols: SymbolDict = field(init = True)
    timeframes: Set[str] = field(init = True)
    time_since: Timestamp = field(init = True, default = None)
    time_until: Timestamp = field(init = True, default = None)
    reader_mode: str = field(init = True, default = "tsdb")
    wait_response: bool = field(init = True, default = True)
    TS_SINCE_DEF: ClassVar[Timestamp] = Timestamp.min.tz_localize(TZ)
    TS_UNTIL_DEF: ClassVar[Timestamp] = Timestamp.max.tz_localize(TZ)
    VERBOSE_ERROR_NORESP: ClassVar[str] = "No response from \"{0}\""
    VERBOSE_ERROR_UNPHASED: ClassVar[str] = "Unphased item: {0} != {1}"
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    @Redis.stream#█▄▄▄▄▄
    async def main(self):
        last_time_us: int = 0
        error = self.VERBOSE_ERROR_NORESP.format(self.id)
        xkey = Redis.join(self.stream_prefix, "CID")
        xstreams = {xkey: self.XGROUP}
        queue = deque[Tick | Candle]()
        async for item in self.reader:
            if item.INTERVAL_BASED:
                if (last_time_us < item.time_us):
                    last_time_us = item.time_us
                    while len(queue):
                        prev_item = queue.pop()
                        prev_item.rem = len(queue)
                        yield prev_item

                queue.appendleft(item)

            else: yield item
            if not self.wait_response: continue
            response = await Redis.xread(self, xstreams)
            if not response: Log.error(error); continue
            self.process_response(response, last_time_us)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def process_response(self, response: Tuple, last_time_us: int):
        check_id: str = b64(last_time_us)
        payload: dict = None
        for _, messages in response:
            for _, payload in messages:
                message_id = "NO_CID"
                if isinstance(payload, dict):
                    message_id = payload.pop("cid", message_id)
                    if (message_id == check_id): return
                Log.error(self.VERBOSE_ERROR_UNPHASED
                        .format(message_id, check_id))

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __post_init__(self):
        self.reader_mode = self.reader_mode.upper()
        self.stream_prefix = self.STREAM_PREFIX + self.id
        if (self.time_since is None): self.time_since = self.TS_SINCE_DEF
        if (self.time_until is None): self.time_until = self.TS_UNTIL_DEF
        if (self.time_since.tz is None): self.time_since = self.time_since.tz_localize(TZ)
        if (self.time_until.tz is None): self.time_until = self.time_until.tz_localize(TZ)
        args = {"timeframes": self.timeframes, "time_since": self.time_since, "symbols": self.symbols,
            "time_until": self.time_until}
        if (self.reader_mode == "FILE"): self.reader = FileReader(**args)
        elif (self.reader_mode == "TSDB"): self.reader = TSDBReader(**args)
        else: raise ValueError(f"Invalid reader mode: {self.reader_mode}")
        super().__post_init__()
        self._procs[f"DataProvider/main"] = self.main


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

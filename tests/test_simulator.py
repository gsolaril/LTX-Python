#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
import asyncio
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase

import numpy as np
from pandas import DataFrame, Timestamp

from src.interfaces.simulator.datareader import FileReader, TestFileReader
from src.interfaces.simulator.simulator import Simulator
from src.models import Account, Candle, Rules, Symbol, Tick, TimeFrame
from src.utils import ClickHouse, TZ

#███████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class TestSimulator(TestCase):
    N: int = TestFileReader.N
    N_PRINT: int = TestFileReader.N_PRINT
    VENUE: str = TestFileReader.VENUE
    SYMBOL: str = TestFileReader.SYMBOL
    T0_US: int = TestFileReader.T0_US
    TF: str = TestFileReader.TF

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def setUp(self):
        self._tmpdir = TemporaryDirectory()
        self._folder = Path(self._tmpdir.name)
        self._prev_folder = FileReader.FOLDER_DBIN
        FileReader.FOLDER_DBIN = self._folder
        (self._folder / self.VENUE).mkdir(parents = True, exist_ok = True)
        self._write_market_files(n_ticks = 25, n_candles = 5)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def tearDown(self):
        FileReader.FOLDER_DBIN = self._prev_folder
        self._tmpdir.cleanup()

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def _write_market_files(self, n_ticks: int, n_candles: int):
        tick_rows = TestFileReader._random_walk_ticks(n_ticks, seed = 7)
        candle_rows = TestFileReader._random_walk_candles(n_candles, seed = 11)
        tick_csv = self._folder / "ticks.csv"
        candle_csv = self._folder / "candles.csv"
        tick_bin = self._folder / self.VENUE / f"{self.SYMBOL}{FileReader.EXT}"
        candle_bin = self._folder / self.VENUE / f"{self.SYMBOL}_{self.TF}{FileReader.EXT}"
        TestFileReader._write_csv(tick_csv, list(Tick.SCHEMA), tick_rows)
        TestFileReader._write_csv(candle_csv, list(Candle.SCHEMA), candle_rows)
        FileReader.csv_to_bin(tick_csv, tick_bin, model = Tick)
        FileReader.csv_to_bin(candle_csv, candle_bin, model = Candle)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    @staticmethod
    def _round_sig(value: float | np.ndarray, digits: int = 8):
        values = np.asarray(value, dtype = float)
        rounded = np.empty_like(values, dtype = float)
        for idx, val in enumerate(values.flat):
            if (val == 0.0): rounded.flat[idx] = 0.0
            else:
                mag = int(np.floor(np.log10(abs(val))))
                rounded.flat[idx] = np.round(val, digits - mag - 1)
        return rounded.item() if np.asarray(value).ndim == 0 else rounded

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def _generate_sine_history(self, *, venue: str = "MYVENUE",
            symbol: str = "MYSYMBOL", p_min: float = 100.0,
            p_max: float = 120.0, p_spr: float = 0.25,
            tps: int = 5, days: int = 1, period: float = 8.64e10,
            start_time: Timestamp = None,
            timeframes: list[str] | set[str] = None,
            reader_mode: str = "FILE"):
        if (start_time is None):
            start_time = Timestamp.now(TZ).floor("D")
        if (timeframes is None):
            timeframes = ["M1", "H1", "D1"]
        start_time = start_time.tz_localize(TZ) if start_time.tz is None else start_time
        period = float(period)
        tps = int(tps)
        days = int(days)
        points = int(tps * days * 86400)
        step_us = int(1_000_000 / max(1, tps))
        start_us = int(start_time.timestamp() * 1e6)
        times_us = start_us + np.arange(points, dtype = np.int64) * step_us
        t_us = times_us.astype(np.float64)
        p_mid = (p_max + p_min) / 2.0
        p_amp = (p_max - p_min) / 2.0
        ask = self._round_sig(p_mid + p_amp * np.sin((2 * np.pi * t_us) / period))
        bid = ask - p_spr
        rows = list[dict]()
        for time_us, ask_value, bid_value in zip(times_us, ask, bid):
            rows.append({
                "time": Timestamp(time_us, unit = "us", tz = TZ),
                "venue": venue,
                "symbol": symbol,
                "pa": float(ask_value),
                "qa": 1.0,
                "pb": float(bid_value),
                "qb": 1.0,
            })

        tick_df = DataFrame(rows).set_index("time")
        history = {
            "venue": venue,
            "symbol": symbol,
            "reader_mode": reader_mode.upper(),
            "start": start_time,
            "end": Timestamp(times_us[-1], unit = "us", tz = TZ),
            "files": [],
            "clickhouse": {"tables": [], "since": start_time, "until": Timestamp(times_us[-1], unit = "us", tz = TZ)},
            "ticks": tick_df.copy(),
            "candles": {},
        }

        for tf_name in sorted(timeframes, key = lambda tf: TimeFrame[tf].value):
            tf = TimeFrame[tf_name]
            freq = f"{tf.ts}s"
            ask_ohlc = tick_df["pa"].resample(freq).ohlc()
            bid_ohlc = tick_df["pb"].resample(freq).ohlc()
            volume = tick_df["pa"].resample(freq).count().astype(int)
            candle_rows = list[dict]()
            for time_idx, row in ask_ohlc.iterrows():
                bid_row = bid_ohlc.loc[time_idx]
                candle_rows.append({
                    "time": time_idx,
                    "venue": venue,
                    "symbol": symbol,
                    "tf": tf_name,
                    "oa": float(row["open"]),
                    "ha": float(row["high"]),
                    "la": float(row["low"]),
                    "ca": float(row["close"]),
                    "ob": float(bid_row["open"]),
                    "hb": float(bid_row["high"]),
                    "lb": float(bid_row["low"]),
                    "cb": float(bid_row["close"]),
                    "volume": int(volume.loc[time_idx]),
                })
            candle_df = DataFrame(candle_rows)
            history["candles"][tf_name] = candle_df

            if (reader_mode.upper() == "FILE"):
                tick_csv = self._folder / "ticks_generated.csv"
                candle_csv = self._folder / f"candles_{tf_name}_generated.csv"
                TestFileReader._write_csv(tick_csv, list(Tick.SCHEMA), [
                    (int(row["time"].timestamp() * 1e6), row["pa"], row["qa"], row["pb"], row["qb"]) for row in rows
                ])
                TestFileReader._write_csv(candle_csv, list(Candle.SCHEMA), [
                    (int(row["time"].timestamp() * 1e6), row["oa"], row["ha"], row["la"], row["ca"], row["ob"], row["hb"], row["lb"], row["cb"], row["volume"]) for _, row in candle_df.iterrows()
                ])
                tick_bin = self._folder / venue / f"{symbol}{FileReader.EXT}"
                candle_bin = self._folder / venue / f"{symbol}_{tf_name}{FileReader.EXT}"
                tick_bin.parent.mkdir(parents = True, exist_ok = True)
                FileReader.csv_to_bin(tick_csv, tick_bin, model = Tick)
                FileReader.csv_to_bin(candle_csv, candle_bin, model = Candle)
                history["files"].extend([str(tick_csv), str(tick_bin), str(candle_csv), str(candle_bin)])

            elif (reader_mode.upper() == "TSDB"):
                tick_payloads = [{
                    "time": row["time"],
                    "venue": venue,
                    "symbol": symbol,
                    "pa": row["pa"],
                    "qa": row["qa"],
                    "pb": row["pb"],
                    "qb": row["qb"],
                } for _, row in tick_df.iterrows()]
                candle_payloads = [{
                    "time": row["time"],
                    "venue": venue,
                    "symbol": symbol,
                    "tf": tf_name,
                    "oa": row["oa"],
                    "ha": row["ha"],
                    "la": row["la"],
                    "ca": row["ca"],
                    "ob": row["ob"],
                    "hb": row["hb"],
                    "lb": row["lb"],
                    "cb": row["cb"],
                    "volume": int(row["volume"]),
                } for _, row in candle_df.iterrows()]
                ClickHouse.write(ClickHouse.Table.TICKS.value, tick_payloads)
                ClickHouse.write(ClickHouse.Table.CANDLES.value, candle_payloads)
                history["clickhouse"]["tables"].append({
                    "table": ClickHouse.Table.TICKS.value,
                    "since": history["start"],
                    "until": history["end"],
                    "venue": venue,
                    "symbol": symbol,
                })
                history["clickhouse"]["tables"].append({
                    "table": ClickHouse.Table.CANDLES.value,
                    "since": history["start"],
                    "until": history["end"],
                    "venue": venue,
                    "symbol": symbol,
                    "tf": tf_name,
                })

        return history

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def _cleanup_generated_history(self, history: dict):
        for path in history.get("files", []):
            file_path = Path(path)
            if file_path.exists(): file_path.unlink() 
        for table in history.get("clickhouse", {}).get("tables", []):
            table_name = table["table"]
            venue = table["venue"]
            symbol = table["symbol"]
            since = table.get("since")
            until = table.get("until")
            tf = table.get("tf")
            if (tf is None):
                cond = f"(venue = '{venue}') AND (symbol = '{symbol}') AND (time >= '{since}') AND (time <= '{until}')"
            else:
                cond = f"(venue = '{venue}') AND (symbol = '{symbol}') AND (tf = '{tf}') AND (time >= '{since}') AND (time <= '{until}')"
            history["clickhouse"].setdefault("delete_queries", []).append(
                f"ALTER TABLE {table_name} DELETE WHERE {cond}"
            )

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def test_simulator_initializes_default_account(self):
        symbol = Symbol(venue = self.VENUE, symbol = self.SYMBOL,
            min_price_diff = 0.01, min_order_size = 0.001)
        sim = Simulator(id = "sim-init", accounts = None,
            symbols = {(self.VENUE, self.SYMBOL): symbol},
            timeframes = {"T1", self.TF},
            reader_mode = "file",
            rules = Rules(fixed_spread = 2.0))

        self.assertIn(sim.id, sim.accounts)
        self.assertIsInstance(sim.accounts[sim.id], Account)
        self.assertEqual(sim.accounts[sim.id].balance, Simulator.DEFAULT_BALANCE)
        self.assertTrue(sim._tick_driven)
        self.assertIn(self.TF, {tf.name for tf in sim._timeframes})

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def test_on_quote_updates_quote_map_without_trade_clearing(self):
        symbol = Symbol(venue = self.VENUE, symbol = self.SYMBOL,
            min_price_diff = 0.01, min_order_size = 0.001)
        sim = Simulator(id = "sim-quote", accounts = None,
            symbols = {(self.VENUE, self.SYMBOL): symbol},
            timeframes = {"T1", self.TF},
            reader_mode = "file",
            rules = Rules(fixed_spread = 2.0))
        ts = Timestamp(self.T0_US, unit = "us", tz = TZ)
        tick = Tick(time = ts, symbol = symbol, pa = 100.0, qa = 1.0,
            pb = 100.0, qb = 1.0)

        responses = sim.on_quote(tick)

        self.assertEqual(sim.quotes[(self.VENUE, self.SYMBOL)], tick)
        self.assertEqual(tick.pb, 98.0)
        self.assertEqual(responses, [])

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def test_file_reader_drains_ticks_and_candles(self):
        n_ticks, n_candles = self.N, max(1, self.N // 60)
        expected = n_ticks + n_candles
        tick_rows = TestFileReader._random_walk_ticks(n_ticks, seed = 7)
        candle_rows = TestFileReader._random_walk_candles(n_candles, seed = 11)
        path_tick_csv = self._folder / "ticks.csv"
        path_tick_bin = self._folder / self.VENUE / f"{self.SYMBOL}{FileReader.EXT}"
        path_candle_csv = self._folder / "candles.csv"
        path_candle_bin = self._folder / self.VENUE / f"{self.SYMBOL}_{self.TF}{FileReader.EXT}"
        TestFileReader._write_csv(path_tick_csv, list(Tick.SCHEMA), tick_rows)
        TestFileReader._write_csv(path_candle_csv, list(Candle.SCHEMA), candle_rows)
        FileReader.csv_to_bin(path_tick_csv, path_tick_bin, model = Tick)
        FileReader.csv_to_bin(path_candle_csv, path_candle_bin, model = Candle)

        symbol = Symbol(venue = self.VENUE, symbol = self.SYMBOL,
            min_price_diff = 0.01, min_order_size = 0.001)
        symbols = {(self.VENUE, self.SYMBOL): symbol}
        sim = Simulator(id = "sim-file", accounts = None,
            symbols = symbols,
            timeframes = {"T1", self.TF},
            rules = Rules(fixed_spread = 2.0),
            time_since = Timestamp(self.T0_US, unit = "us", tz = TZ),
            time_until = Timestamp.max.tz_localize(TZ),
            reader_mode = "file",
            wait_response = False)

        items = list(sim.reader)
        self.assertEqual(len(items), expected)
        self.assertEqual(sum(isinstance(item, Tick) for item in items), n_ticks)
        self.assertEqual(sum(isinstance(item, Candle) for item in items), n_candles)
        self.assertTrue(all(item.symbol.venue == self.VENUE for item in items))
        self.assertTrue(all(item.symbol.symbol == self.SYMBOL for item in items))

        sim2 = Simulator(id = "sim-file-2", accounts = None,
            symbols = symbols,
            timeframes = {"T1", self.TF},
            rules = Rules(fixed_spread = 2.0),
            time_since = Timestamp(self.T0_US, unit = "us", tz = TZ),
            time_until = Timestamp.max.tz_localize(TZ),
            reader_mode = "file",
            wait_response = False)
        results = list(sim2.reader)
        self.assertEqual(len(results), expected)
        self.assertEqual(sum(isinstance(item, Tick) for item in results), n_ticks)
        self.assertEqual(sum(isinstance(item, Candle) for item in results), n_candles)

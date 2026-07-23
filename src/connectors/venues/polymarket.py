#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
import asyncio, asyncpg, json, time
from bidict import bidict
from aiohttp import ClientSession
from dataclasses import dataclass, field
from typing import Any, Dict, ClassVar
from pandas import Series, Timedelta, Timestamp
from src.connectors.base import Venue, Connector
from src.models import *
from src.utils import *

#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class Polymarket(Venue):
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    class Credentials(Venue.Credentials):
        private_key: str; wallet_address: str
        relayer_key: str; relayer_address: str
    
    ARROWS_FROM_SIGN = {+1: "↑", -1: "↓"}
    ARROWS_FROM_CHAR = {"U": "↑", "D": "↓"}
    STATUS = {"live": "OK", "matched": "OK"}
    URL_GAMMA = "https://gamma-api.polymarket.com"
    
    #▄▄▄▄▄▄▄▄▄▄▄
    @dataclass#█▄▄▄▄▄▄▄▄▄▄
    class Event(BasePoint):
        index: str = field(kw_only = True, default = "IDS")
        STREAM_KEY: ClassVar[str] = "Polymarket|GAMMA"
        MAP: ClassVar[bidict[str, str]] = bidict()
        MIN_UPD_FREQ: ClassVar[int] = 300
        #▄▄▄▄▄▄▄▄▄▄
        @property#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
        def __dict__(self): return {"stream": self.STREAM_KEY,
              "time": self.time_us, "payload": dict(self.MAP)}
        #▄▄▄▄▄▄▄▄▄▄▄▄▄
        @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
        def shift_keys(cls, shift: int = 1):
            start_at = time.time()
            new_map: bidict[str, str] = bidict()
            for old_symbol, id in cls.MAP.items():
                key, old_shift = old_symbol.split("+")
                new_shift = int(old_shift) - shift
                if (new_shift < 0): continue
                new_map[f"{key}+{new_shift}"] = id
            cls.MAP = new_map
            delay = (time.time() - start_at) * 1e6
            Log.info(f"Keys shifted by {shift}... delay: {delay:.0f} μs...")

    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def slug(cls, symbol: str, tf: TimeFrame, ts: Timestamp = None):
        mapper = {"BTC": "Bitcoin", "ETH": "Ethereum", "SOL": "Solana"}
        if (ts is None): ts = Timestamp.now("UTC")
        tf_str = TimeFrame.swap_nt(tf)
        ts = ts.floor(tf.value)
        if (tf != TimeFrame.H1):
            ts_int = int(Timestamp.timestamp(ts))
            return f"{symbol.lower()}-updown-{tf_str}-{ts_int}".lower()
        else:
            ts_str = Timestamp.strftime(ts - tf.value, "%B-%-d-%Y-%-I%p")            
            return f"{mapper[symbol]}-up-or-down-{ts_str}-et".lower()

    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def split_symbol(cls, symbol: str):
        if "↑" in symbol: symbol, tf_str = symbol.split("↑")
        elif "↓" in symbol: symbol, tf_str = symbol.split("↓")
        tf_str, shift = tf_str.split("+")
        return symbol, TimeFrame[tf_str], int(shift)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄ 
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def status_to_local(cls, response: dict):
        status = response.get("status", None)
        status = cls.STATUS.get(status, None)
        if status is not None: return status
        if ("canceled" in response):
            canceled = response["canceled"]
            if len(canceled): return "OK"
        if ("not_canceled" in response):
            status = "OK"
            not_canceled: dict = response["not_canceled"]
            if len(not_canceled):
                matched = "already canceled or matched"
                for EID, error in not_canceled.items():
                    if not str.endswith(error, matched):
                        status = "ERROR"

            return status

#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄
@dataclass#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class PolymarketGamma(Connector, Polymarket):
    freq_redis_report: int = field(kw_only = True,
        default = Polymarket.Event.MIN_UPD_FREQ)
    VENUE: ClassVar[str] = Polymarket.VENUE
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __post_init__(self):
        super().__post_init__()
        self._crons[self.reconfig] = Timedelta(
              seconds = self.freq_redis_report)
    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def _parse_ids(cls, event: Dict[str, Any]):
        markets = event.get("markets") or list()
        if not markets: return

        outcomes = markets[0].get("outcomes")
        if isinstance(outcomes, str):
            try: outcomes = json.loads(outcomes)
            except Exception: outcomes = None
        if not isinstance(outcomes, list): return

        clob_ids = markets[0].get("clobTokenIds")
        if isinstance(clob_ids, str):
            try: clob_ids = json.loads(clob_ids)
            except Exception: clob_ids = None
        if not isinstance(clob_ids, list): return

        arrows = cls.ARROWS_FROM_CHAR
        ids = dict.fromkeys(arrows.values())
        if len(outcomes) != len(clob_ids): return
        for nm, token in zip(outcomes, clob_ids):
            ids[arrows[str(nm).strip()[0]]] = token
        if (ids[arrows["U"]] is None): return
        if (ids[arrows["D"]] is None): return
        return ids

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def _find_event(self, session: ClientSession,
            symbol: str, tf: TimeFrame, shift: int = 0):
        ts = Timestamp.now("UTC") + shift * tf.value
        args = {"url": self.URL_GAMMA.rstrip("/") + "/events",
                "params": {"slug": self.slug(symbol, tf, ts)}}
        async with session.get(**args) as resp:
            if (resp.status != 200): return
            try: ids = await resp.json()
            except Exception as EXC: return Log.exception(EXC)
            if isinstance(ids, list) and len(ids): return ids[0]

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    @Redis.stream#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def update_ids(self, shifts: int = 3):
        time_event = Timestamp.now("UTC")
        verbose, pending = dict(), dict()
        for symbol_obj in self._specs.values():
            symbol: str = symbol_obj.symbol
            key = self.split_symbol(symbol)[: 2]
            for shift in range(shifts):
                pending[(*key, shift)] = None
            verbose[key] = "{}_{!r}".format(*key)

        tf: TimeFrame = None
        keys = list(pending.keys())
        Log.info(f"Getting event IDs:\n -> " + str.join(", ", verbose.values()))
        async with ClientSession() as session: events = await asyncio.gather(
            *[self._find_event(session, *item) for item in keys])

        verbose = dict()
        for key, event in zip(keys, events):
            (symbol, tf, shift) = key
            if (event is None): continue
            parsed = self._parse_ids(event)
            if (parsed is None): continue
            for arrow in self.ARROWS_FROM_CHAR.values():
                prefix, suffix = f"{symbol}{arrow}", f"{tf!r}+{shift}"
                self.Event.MAP[prefix + suffix] = (id := parsed[arrow])
                verbose[(prefix, suffix)] = id[: 4] + "…" + id[-4 :]

        delay = (time.time() - time_event.timestamp()) * 1e6
        verbose = Series(verbose).sort_index().dropna()
        verbose = verbose.rename_axis(["symbol", "tf"]).unstack("tf")
        Log.success(f"Got IDs... delay: {delay:.0f} μs...\n{verbose}")
        yield self.Event(time = time_event)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def reconfig(self):
        conn = await Postgres._client.acquire()
        await self._reconfig(conn)
        self._crons[self.reconfig] = Timedelta(
              seconds = self.freq_redis_report)
        await self.update_specs(conn, Polymarket.VENUE)
        await conn.close()

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    @Postgres.on_table(Connector.TABLE_CONFIG)
    async def _reconfig(self, conn: asyncpg.Connection):
        Polymarket.Event.shift_keys(shift = 1)
        await super().reconfig(conn, Polymarket.VENUE)
        query_id, query_exp = list[str](), list[str]()
        condition = "WHEN (symbol = '{0}') THEN '{1}'"
        line_upper = f"UPDATE {self.TABLE_SYMBOLS} SET"
        line_lower = f"WHERE (venue = '{Polymarket.VENUE}');"

        TAB = " " * 4
        [*await self.update_ids()]
        for symbol, id in Polymarket.Event.MAP.items():
            quote, tf, shift = self.split_symbol(symbol)
            query_id.append(2 * TAB + condition.format(symbol, id))
            exp = Timestamp.now("UTC").ceil(tf.value) + shift * tf.value
            exp_str = Timestamp.strftime(exp, "%Y-%m-%d %H:%M:%S+00:00")
            query_exp.append(2 * TAB + condition.format(symbol, exp_str))

        if not query_id or not query_exp: return
        query_id.insert(0, TAB + "id = CASE")
        query_id.append(TAB + "ELSE id END,")
        query_exp.insert(0, TAB + "expiration = CASE")
        query_exp.append(TAB + "ELSE expiration END")
        query_lines = [line_upper, *query_id, *query_exp, line_lower]
        n = (await conn.execute(str.join("\n", query_lines))).split(" ")[-1]
        Log.info(f"Updated {n} Polymarket IDs in \"{self.TABLE_SYMBOLS}\"")
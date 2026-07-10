#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
import asyncio, asyncpg, json
from aiohttp import ClientSession
from dataclasses import dataclass, field
from typing import Any, List, Dict, ClassVar
from pandas import Series, DataFrame, Timestamp
from src.connectors.base import Venue, DataConnector
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

    #▄▄▄▄▄▄▄▄▄▄▄
    @dataclass#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    class TokenUpdate(DataPoint):
        index: str = field(kw_only = True, default = "IDS")
        STREAM_KEY: ClassVar[str] = "Polymarket|GAMMA"
        #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
        def __post_init__(self):
            super().__post_init__()

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
        tf_str = tf_str.split("+")[0]
        return symbol, TimeFrame[tf_str]

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
@dataclass#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class PolymarketGamma(DataConnector, Polymarket):
    VENUE: ClassVar[str] = Polymarket.VENUE
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __init__(self):
        super().__post_init__()

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
      symbol: str, tf: TimeFrame, ts: Timestamp = None):
        args = {"url": self.url + "/events", "params": {
            "slug": self.slug(symbol, tf, ts)}}
        async with session.get(**args) as resp:
            if (resp.status != 200): return
            try: data = await resp.json()
            except Exception as EXC: return Log.exception(EXC)
            if isinstance(data, list) and len(data): return data[0]

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    @Redis.on_stream#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def update_ids(self, shifts: int = 2):
        verbose, pending = dict(), dict()
        for symbol_obj in self._specs.values():
            symbol: str = symbol_obj.symbol
            key = self.split_symbol(symbol)
            for shift in range(shifts):
                pending[(*key, shift)] = None
            verbose[key] = "{}_{!r}".format(*key)

        tf: TimeFrame = None
        now = Timestamp.now("UTC")
        keys = list(pending.keys())
        Log.info(f"Getting IDs:\n -> " + str.join(", ", verbose.values()))
        async with ClientSession() as session: events = await asyncio.gather(
            *[self._find_event(session, symbol, tf, now + shift * tf.value)
              for (symbol, tf, shift) in keys])

        verbose, results = dict(), dict()
        for key, event in zip(keys, events):
            if (event is None): continue
            parsed = self._parse_ids(event)
            if (parsed is None): continue
            (symbol, tf, shift) = key
            for arrow in self.ARROWS_FROM_CHAR.values():
                key = (f"{symbol}{arrow}", f"{tf!r}+{shift}")
                results[key[0] + key[1]] = (id := parsed[arrow])
                verbose[key] = id[: 4] + "..." + id[-4 :]

        verbose = Series(verbose).sort_index()
        verbose = verbose.rename_axis(["symbol", "tf"])
        verbose = verbose.unstack("tf")
        Log.success(f"Got IDs...\n{verbose}")
        yield self.TokenUpdate(data = results)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    @Postgres.on_table(DataConnector.TABLE_CONFIG)
    async def reconfig(self, conn: asyncpg.Connection):
        table, venue = self.TABLE_SYMBOLS, Polymarket.VENUE
        query_lines = [f"UPDATE {table} SET id = CASE"]
        line = "WHEN (symbol = '{}') THEN '{}'"
        update: Polymarket.TokenUpdate = None
        await super().reconfig(conn, venue)
        for update in await self.update_ids():
            for symbol, id in update.data.items():
                query_lines.append(line.format(symbol, id))
        if (len(query_lines) <= 1): return
        query_lines.append(f"ELSE id END WHERE (venue = '{venue}');")
        query = str.join("\n" + 4 * " ", query_lines)
        print(query)
        n = (await conn.execute(query)).split(" ")[-1]
        Log.info(f"Updated {n} Polymarket IDs in \"{table}\"")
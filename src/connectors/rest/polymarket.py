#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
import asyncio, asyncpg, json
from aiohttp import ClientSession
from pandas import Timestamp, Timedelta
from dataclasses import dataclass, field
from typing import Any, List, Dict, ClassVar
from src.connectors.base import Venue, Connector
from src.connectors.base import DataConnector
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
        STREAM_KEY: ClassVar[str] = "{venue}|GAMMA"
        #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
        def __post_init__(self):
            super().__post_init__()
            self.index = "IDS"

    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def slug(cls, symbol: str, ts: Timestamp = None):
        if "↑" in symbol: symbol, tf_str = symbol.split("↑")
        elif "↓" in symbol: symbol, tf_str = symbol.split("↓")
        tf: TimeFrame = TimeFrame[tf_str]
        if (ts is None): ts = Timestamp.now("UTC")
        tf_str, ts = TimeFrame.swap_nt(tf), ts.floor(tf.value)
        mapper = {"BTC": "Bitcoin", "ETH": "Ethereum", "SOL": "Solana"}
        if (tf != TimeFrame.H1):
            ts_int = int(Timestamp.timestamp(ts))
            return f"{symbol.lower()}-updown-{tf_str}-{ts_int}"
        else:
            ts_str = Timestamp.strftime(ts - tf.value, "%b-%d-%Y-%I%p")
            return f"{mapper[symbol]}-up-to-down-{ts_str}-et"

    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def split_symbol(cls, symbol: str):
        if "↑" in symbol: symbol, tf_str = symbol.split("↑")
        elif "↓" in symbol: symbol, tf_str = symbol.split("↓")
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
@dataclass#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class PolymarketGamma(Connector, Polymarket):
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __init__(self):
        super().__init__()

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

        ids = dict.fromkeys(cls.ARROWS_FROM_CHAR.values())
        if len(outcomes) != len(clob_ids): return
        for nm, token in zip(outcomes, clob_ids):
            ids[cls.ARROWS_FROM_CHAR[str(nm).strip()[0]]] = token
        if (ids[cls.ARROWS_FROM_CHAR["U"]] is None): return
        if (ids[cls.ARROWS_FROM_CHAR["D"]] is None): return
        return ids

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def _find_event(cls, session: ClientSession,
                    symbol: str, ts: Timestamp = None):
        args = {"url": session._base_url + "/events",
            "params": {"slug": cls.slug(symbol, ts)}}
        async with session.get(**args) as resp:
            if (resp.status != 200): return
            try: data = await resp.json()
            except Exception as EXC: return Log.exception(EXC)
            if isinstance(data, list) and len(data): return data[0]

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    @Redis.on_stream#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def update_ids(self, shifts: int = 2):
        verbose, tasks = list(), dict()
        for symbol in self._specs.values():
            key = self.split_symbol(symbol.symbol)
            verbose.append(key)
            for shift in range(shifts):
                tasks[(*key, shift)] = None

        tf: TimeFrame = None
        now = Timestamp.now("UTC")
        url = self.url + "/events/"
        async with ClientSession(url) as session:
            for (symbol, tf, shift) in tasks.keys():
                ts: Timestamp = now + shift * tf.value
                task = self._find_event(session, symbol, ts)
                tasks[(symbol, tf, shift)] = task
        Log.info(f"Getting IDs:\n -> " + str.join(", ", verbose))
        tasks = zip(tasks, await asyncio.gather(*tasks.values()))

        verbose, results = list(), dict()
        for (symbol, tf, shift), event in tasks:
            if (event is None): continue
            ids = self._parse_ids(event)
            if (ids is None): continue
            for arrow in self.ARROWS_FROM_CHAR.values():
                key = f"{symbol}{arrow}{tf!r}+{shift}"
                results[key] = (id := ids[arrow])
                preview = id[: 4] + "..." + id[-4 :]
                verbose.append(f"{key}: {preview}")
            
        Log.success("Got IDs...\n -> " + str.join(", ", verbose))
        yield self.TokenUpdate(data = results)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    @Postgres.on_table(Connector.TABLE_CONFIG)#█▄▄▄▄▄▄
    async def reconfig(self, conn: asyncpg.Connection):
        await super().reconfig(conn)
        query_lines = [""]
        for symbol, id in (await self.update_ids()).items(): query_lines.append(
          f"WHEN (venue = '{self.VENUE}') AND (symbol = '{symbol}') THEN '{id}'")
        query = str.join("\n    ", query_lines)
        query = f"UPDATE {self.TABLE_SYMBOLS} SET id = CASE {query} \nEND"
        await conn.execute(query)
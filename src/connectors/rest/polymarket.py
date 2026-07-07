#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
import asyncio, asyncpg, json
from aiohttp import ClientSession
from pandas import Timestamp, Timedelta
from dataclasses import dataclass, field
from typing import Any, List, Dict, ClassVar
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

    #▄▄▄▄▄▄▄▄▄▄▄
    @dataclass#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    class TokenUpdate(DataPoint):
        index: str = field(kw_only = True, default = "IDS")
        STREAM_KEY: ClassVar[str] = "{venue}|GAMMA"
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

        ids = dict.fromkeys(cls.ARROWS_FROM_CHAR.values())
        if len(outcomes) != len(clob_ids): return
        for nm, token in zip(outcomes, clob_ids):
            ids[cls.ARROWS_FROM_CHAR[str(nm).strip()[0]]] = token
        if (ids[cls.ARROWS_FROM_CHAR["U"]] is None): return
        if (ids[cls.ARROWS_FROM_CHAR["D"]] is None): return
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

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def update_ids(self, shifts: int = 2):
        verbose, pending = dict(), dict()
        for symbol_obj in self._specs.values():
            symbol: str = symbol_obj.symbol
            key = self.split_symbol(symbol)
            for shift in range(shifts):
                pending[(*key, shift)] = None
            verbose[key] = "{}_{!r}".format(*key)

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
                key = f"{symbol}{arrow}{tf!r}+{shift}"
                results[key] = (id := parsed[arrow])
                verbose[key] = id[: 4] + "." + id[-4 :]

        Log.success(f"Got IDs...\n -> {verbose}")
        yield self.TokenUpdate(data = results)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    @Postgres.on_table(Connector.TABLE_CONFIG)#█▄▄▄▄▄▄
    async def reconfig(self, conn: asyncpg.Connection):
        await super().reconfig(conn, venue := Polymarket.VENUE)
        main = f"UPDATE {self.TABLE_SYMBOLS} " "SET id = CASE\n {} \nEND"
        line = (f"WHEN venue = '{venue}' " "AND symbol = '{}' THEN '{}'")
        lines = list()
        update: Polymarket.TokenUpdate = None
        async for update in self.update_ids():
            for symbol, id in update.data.items():
                lines.append(line.format(symbol, id))
        query = main.format(str.join("\n\t", lines))
        await conn.execute(query)
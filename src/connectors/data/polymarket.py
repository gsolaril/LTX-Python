#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
import asyncpg, time
from bidict import bidict
from typing import Any, Dict, ClassVar
from pandas import Timestamp, Timedelta
from aiohttp import ClientWebSocketResponse
from src.connectors.venues import Polymarket
from src.connectors.ws import Connector, DataConnectorWS, DataChannelWS
from src.models import Tick, Candle, TimeFrame
from src.utils import Log, Postgres, Redis, TZ

#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class DataPolymarket(DataConnectorWS, Polymarket):

    URL_WS: ClassVar[str] = "wss://ws-subscriptions-clob.polymarket.com"
    DEFAULT_PAYLOAD = {"type": "market", "custom_feature_enabled": True}
    
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __init__(self): super().__init__(
        ticks = DataChannelWS(name = "ticks",
            get_subs = self.get_subs, on_message = self.on_ticks,
            on_ping = self.on_ping, url_args = self.get_url_ticks),
        )
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __post_init__(self):
        super().__post_init__()
        name = f"{self.name}/redis_updater"
        self._procs[name] = self.Event.redis_updater(self.try_resub)
        self._crons[self.shift_keys] = Timedelta(seconds = self.Event.MIN_UPD_FREQ)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def shift_keys(self):
        start_at = time.time()
        self.Event.shift_keys()
        delay = (time.time() - start_at) * 1e6
        Log.info(f"Keys shifted... delay: {delay:.0f} μs...")

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def try_resub(self, stream: str, payload: dict, mid: str):
        self._WS_to_resub.set()
        if self.debug: Log.debug(
            "About to resubscribe to:\n => "
            + str.join(", ", sorted(payload)))
    
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    @Postgres.on_table(DataConnectorWS.TABLE_CONFIG)
    async def reconfig(self, conn: asyncpg.Connection,
              venue: str = None, sources: set = None):
        await Connector.reconfig(self, conn, venue)
        sources = "({})".format(str.join("|", self.sources))
        await self.update_specs(conn, venue, sources)
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def get_subs(self, subs: set[str], is_sub: bool):
        subs_current = set(self.Event.MAP.values())
        if is_sub: subs_due = subs_current.difference(subs)
        else: subs_due = subs.difference(subs_current)
        payload = {"assets_ids": sorted(subs_due), "channels": ["book"], 
                  "operation": "SUBSCRIBE" if is_sub else "UNSUBSCRIBE"}
        if not subs:
            payload.pop("operation")
            payload.update(self.DEFAULT_PAYLOAD)
            print("PAYLOAD:", payload)
        return subs_due, [payload]

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def get_url_headers(self, path: str):
        return {"url": self.URL_WS.rstrip("/") + "/" + path.lstrip("/")}
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def get_url_ticks(self): return self.get_url_headers("ws/market")
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def on_ping(self, WS: ClientWebSocketResponse, sender: bool = False):
        if not sender or (Timestamp.now("UTC").second != 0): return
        return await WS.send_str("PING")

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    @Redis.stream#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def on_ticks(self, data: Dict):

        template = [{"price": 0.0, "size": 0.0}]
        if not isinstance(data, list): data = [data]
        for entry in data:
            if not isinstance(entry, dict): continue
            event = entry.get("event_type", None)
            if (event != "book"): continue
            id = entry.get("asset_id", None)
            tse = entry.get("timestamp", None)
            if id is None or tse is None: continue
            symbol = self.Event.MAP.get(id, None)
            if symbol is None: continue
            A = entry.get("asks", list())
            B = entry.get("bids", list())
            if not A: A = template.copy()
            if not B: B = template.copy()
            ts = Timestamp.utcfromtimestamp(int(tse) / 1e3)
            ts = Timestamp.utcnow() # = ts + self.OFFSET
            tick = Tick(venue = self.VENUE, symbol = symbol,
                    pa = A[-1]["price"], qa = A[-1]["size"],
                    pb = B[-1]["price"], qb = B[-1]["size"],
                    time = ts)
            yield tick
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
import asyncio
from typing import Any, Dict, ClassVar
from pandas import Timestamp, Timedelta
from aiohttp import ClientWebSocketResponse
from src.connectors.venues import Polymarket
from src.connectors.ws import DataConnectorWS, DataChannelWS
from src.models import *
from src.utils import *

#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class DataPolymarket(DataConnectorWS, Polymarket):

    URL_WS: ClassVar[str] = "wss://ws-subscriptions-clob.polymarket.com"
    
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __init__(self): super().__init__(
        ticks = DataChannelWS(name = "ticks",
            get_subs = self.get_subs, on_message = self.on_ticks,
            on_ping = self.on_ping, url_args = self.get_url_headers),
    )
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __post_init__(self):
        self._xstreams = dict[str, str]()
        name = f"{self.name}/update_ids"
        self._procs[name] = self.update_ids
        self._crons[self.Event.shift_keys] = Timedelta(seconds = self.Event.MIN_UPD_FREQ)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def get_url_headers(self, path: str):
        return {"url": self.URL_WS.rstrip("/") + "/" + path.lstrip("/")} 
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def get_subs(self, subs: set[str], is_sub: bool):
        subs_current = set(self.Event.MAP.values())
        if is_sub: subs_due = subs_current.difference(subs)
        else: subs_due = subs.difference(subs_current)
        payload = {"assets_ids": sorted(subs_due), "channels": ["book"], 
                  "operation": "SUBSCRIBE" if is_sub else "UNSUBSCRIBE"}
        return subs_due, [payload]

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def on_ping(self, WS: ClientWebSocketResponse, sender: bool = False):
        if not sender or (Timestamp.now("UTC").second != 0): return
        return await WS.send_str("PING")

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def update_ids(self):
        sget_key = Redis.StreamGet.NEW.value
        keys = Redis.scan("*" + self.Event.STREAM_KEY)
        self._xstreams = dict.fromkeys(keys, sget_key)
        if not self._xstreams: return
        while True:
            try:
                response = await Redis.xreadgroup(self,
                    Redis.Group.MONITOR, self._xstreams)
                if not response: continue
                for stream, messages in response:
                    for message_id, payload in messages:
                        if not isinstance(payload, dict): continue
                        self.Event.MAP.update(payload.get("payload", dict()))
                        await Redis.xack(Redis.Group.MONITOR, stream, message_id)
                        self._WS_to_resub.set()
            except asyncio.CancelledError: break
            except Exception as EXC: Log.exception(EXC); break

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
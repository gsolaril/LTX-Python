#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
import asyncio, time, re
from bidict import bidict
from typing import Any, Dict, ClassVar
from aiohttp import ClientWebSocketResponse
from pandas import Series, Timestamp, Timedelta
from src.connectors.venues import Polymarket
from src.connectors.ws import Channel, DataConnectorWS, DataChannelWS
from src.models import Tick, Candle, TimeFrame, Quote
from src.utils import Log, Postgres, Redis, TZ

#███████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class DataPolymarket(DataConnectorWS, Polymarket):

    URL_WS: ClassVar[str] = "wss://ws-subscriptions-clob.polymarket.com"
    DEFAULT_PAYLOAD = {"type": "market", "custom_feature_enabled": True}
    
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __init__(self): super().__init__(
        ticks = DataChannelWS(name = "ticks", get_sub_payloads = self.get_sub_payloads,
          on_message = self.on_ticks, on_ping = self.on_ping, url_args = self.get_url_ticks),
        )
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __post_init__(self):
        super().__post_init__()
        name = f"{self.name}/redis_updater"
        self._procs[name] = self.Event.redis_updater(self.try_resub)
        self._crons[self.update_event] = Timedelta(seconds = self.Event.MIN_UPD_FREQ)
        self._shifted_keys = asyncio.Event()

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def local_to_stream(self, sources: set[str]):
        return super().local_to_stream(self.Event.MAP)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    @Redis.profiler(log = True)
    async def update_event(self):
        self.Event.shift_keys()
        self._shifted_keys.set()

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def try_resub(self, stream: str, mid: str, payload: dict):
        payload.pop("dus2", None)
        await self._shifted_keys.wait()
        self.Event.MAP = bidict(payload)
        if not self.debug: verbose = str.join(", ", sorted(self.Event.MAP))
        else: verbose = str.join("\n => ", [f"{K}: {V}" for K, V in self.Event.MAP.items()])
        Log.debug(f"About to resubscribe to:\n => {verbose}")
        self._WS_to_resub.set()

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def get_sub_payloads(self, sources: set[str], action: Channel.Action):
        regex: re.Pattern = re.compile("({})".format(str.join("|", sources)))
        subs_due = Series(self.Event.MAP.inv).map(regex.match).dropna().index
        payload = {"channels": ["book"], "assets_ids": subs_due.tolist()}
        if (action == Channel.Action.UNSUB):
            payload["operation"] = "UNSUBSCRIBE"
        else:
            payload["operation"] = "SUBSCRIBE"
            if (action == Channel.Action.INIT):
                payload.update(self.DEFAULT_PAYLOAD)
        return set(subs_due), [payload]

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
            symbol_name = self.Event.MAP.inv.get(id, None)
            symbol = self._specs.get(symbol_name, None)
            if (symbol is None) or (tse is None): continue
            A = entry.get("asks", list())
            B = entry.get("bids", list())
            if not A: A = template.copy()
            if not B: B = template.copy()
            ts = Timestamp.utcfromtimestamp(int(tse) / 1e3)
            ts = Timestamp.utcnow() # = ts + self.OFFSET
            yield Tick(symbol = symbol, time = ts,
                pa = A[-1]["price"], qa = A[-1]["size"],
                pb = B[-1]["price"], qb = B[-1]["size"])
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
import asyncio, asyncpg, json
from typing import Any, Callable, ClassVar
from aiohttp import WSMsgType, ClientSession
from aiohttp import ClientWebSocketResponse
from src.connectors.base import Venue, Channel, Connector
from src.connectors.base import DataConnector, ExecConnector
from src.models import *
from src.utils import *

#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class ChannelWS(Channel):
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __init__(self, name: str, on_message: Callable,
          url_args: Callable, on_ping: Callable = None):

        super().__init__(name)
        self.on_ping: Callable = on_ping
        self.url_args: Callable = url_args
        self.on_message: Callable = on_message
        self._WS: ClientWebSocketResponse = None
        self._WS_conned = asyncio.Event() # WebSocket connected.
        self._WS_subbed = asyncio.Event() # WebSocket subscribed.
        self._sources = set[str]()

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def send_ping(self, on_resp: bool = False):

        if isinstance(self.on_ping, Callable):
            try: await self.on_ping(self._WS, on_resp)
            except Exception as EXC:
                role = "received" if on_resp else "sent"
                Log.exception(self.VERBOSE_NOCONN.format(
                    self.name, f"ping was {role}"), EXC)
                self._WS_subbed.clear()

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def listen(self, src: Connector):

        conn_name = f"{src.name}/{self.name}"
        async with ClientSession() as session:
            while src.active:
                try:
                    args = await self.url_args()
                    Log.info(self.VERBOSE_RECONN.format(conn_name, args["url"]))
                    async with session.ws_connect(**args, heartbeat = 30) as WS:
                        self._WS = WS ; self._WS_conned.set()
                        src._sockets[conn_name] = self._WS
                        await self._WS_subbed.wait()
                        Log.info(self.VERBOSE_CONNED.format(conn_name))
                        async for message in self._WS:
                            if (message.type == WSMsgType.TEXT):
                                try:
                                    message_json: dict = message.json()
                                    await self.on_message(message_json)
                                except json.JSONDecodeError:
                                    text = str(message.data)
                                    if (text.lower() == "pong"): pass
                                    elif (text.lower() == "ping"): await self.send_ping(False)
                                    else: Log.warning(self.VERBOSE_NOJSON.format(conn_name, text))
                            elif (message.type == WSMsgType.PING): await self._WS.pong(message.data)
                            elif (message.type == WSMsgType.ERROR): raise self._WS.exception()
                            elif (message.type in {WSMsgType.CLOSED, WSMsgType.CLOSING}):
                                Log.warning(self.VERBOSE_CLOSED.format(conn_name))
                            else:
                                Log.warning(self.VERBOSE_WDTYPE.format(conn_name, message.type))

                except Exception as EXC:
                    Log.exception(self.VERBOSE_ERROR.format(conn_name), EXC)
                    self._WS_conned.clear()
                    self._WS_subbed.clear()
                    self._WS = None
                    if src.active:
                        await asyncio.sleep(2)

#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class DataConnectorWS(DataConnector):
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __init__(self, **datafeeds):
        super().__init__()
        datafeed: DataChannelWS
        for datafeed in datafeeds.values():
            self._sources_new[datafeed.name] = set[set]()
            self._sources_old[datafeed.name] = set[set]()
            self._procs[f"{datafeed.name}/stream"] = datafeed.listen
            self._procs[f"{datafeed.name}/update"] = datafeed.update
    
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __post_init__(self):
        super().__post_init__()
        self._WS_to_resub = asyncio.Event()  # Trigger subscription methods of WebSocket objects, cron-based (Binance) or event-based (Polymarket).

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def reconfig(self, sources: set[str]):
        await super().reconfig(sources)
        self._WS_to_resub.set()

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class DataChannelWS(ChannelWS):
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __init__(self, name: str, on_message: Callable, url_args: Callable, 
              on_ping: Callable = None, get_sub_payloads: Callable = None):

        super().__init__(name, on_message, url_args, on_ping)
        self.get_sub_payloads: Callable = get_sub_payloads
    
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def update(self, src: DataConnectorWS):
        
        conn_name = f"{src.name}/{self.name}"
        Log.warning(f"WS for \"{conn_name}\" update loop started.")
        while src.active:
            await src._WS_to_resub.wait()
            await self._WS_conned.wait()
            payloads_old, payloads_new = list(), list()
            if (sources_old := src._sources_old[self.name]):
                sources_old, payloads_old = self.get_sub_payloads(sources_old, Channel.Action.UNSUB)
            if (sources_new := src._sources_new[self.name]):
                mode = Channel.Action.SUB if self._WS_subbed.is_set() else Channel.Action.INIT
                sources_new, payloads_new = self.get_sub_payloads(sources_new, mode)

            src._WS_to_resub.clear()
            if (self._WS is None) or self._WS.closed:
                self._sources.clear(); continue
            if (not sources_old) and (not sources_new): continue
            Log.info(self.verbose_subs(sources_old, sources_new))

            try:
                if payloads_old:
                    for payload in payloads_old:
                        await self._WS.send_json(payload)
                    src._sources_old[self.name].clear()
                    self._sources -= sources_old
                if payloads_new:
                    for payload in payloads_new:
                        await self._WS.send_json(payload)
                    src._sources_new[self.name].clear()
                    self._sources |= sources_new
                    self._WS_subbed.set()
            except Exception as EXC:
                Log.exception(self.VERBOSE_NOCONN.format(conn_name, "sub"), EXC)

#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class ExecChannelWS(ChannelWS):
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __init__(self, name: str, on_message: Callable, url_args: dict,
                  on_ping: Callable = None, get_subs: Callable = None):

        super().__init__(name, on_message, url_args, on_ping)
        self._WS: ClientWebSocketResponse = None
        self.get_subs: Callable = get_subs
        
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def update(self, connector: ExecConnectorWS): ...

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class ExecConnectorWS(ExecConnector):
    SOURCE_FIELD: ClassVar[str] = "platform"
    TABLE_ACCOUNTS: ClassVar[str] = "accounts"

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __init__(self, *creds):
        super().__init__()
        channel: ExecChannelWS
        cred: Venue.Credentials = None
        self._procs = dict[str, Callable]()
        self._sockets = dict[str, ClientWebSocketResponse]()
        Credentials = getattr(self.__class__, "Credentials",
            Venue.Credentials)
        for cred in creds:
            name_gen = f"{self.name}/{{channel}}/{cred.aid}"
            if not isinstance(cred, Credentials): continue
            channel = ExecChannelWS(name := name_gen.format(channel := "account"), 
                on_message = self.on_message, get_subs = self.get_subs(cred, channel),
                  on_ping = self.on_ping, url_args = self.get_url_args(cred, channel))
            self._procs[name] = channel.listen
            channel = ExecChannelWS(name := name_gen.format(channel := "exec"),
                on_message = self.on_message, get_subs = self.get_subs(cred, channel),
                  on_ping = self.on_ping, url_args = self.get_url_args(cred, channel))
            self._procs[name] = channel.listen
            self._sockets[cred.aid] = channel._WS
    
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __post_init__(self):
        super().__post_init__()
        self._WS_to_resub = asyncio.Event()
        
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def reconfig(self, sources: set[str]):
        await super().reconfig(sources)
        self._WS_to_resub.set()

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def get_url_args(self, cred: Venue.Credentials, channel: str): ...
    def get_subs(self, cred: Venue.Credentials, channel: str): ...
    async def on_ping(self, sender: bool = False): ...
    async def on_message(self, message: Any): ...
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def sender(self, aid: str, payload: dict):
        if (WS := self._sockets[aid]) is None: return
        await WS.send_json(payload)
                    
#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
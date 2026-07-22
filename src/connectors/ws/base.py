#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
import asyncio, asyncpg, json
from typing import Any, Callable, ClassVar
from pandas import Timestamp, Timedelta
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
        self._WS_connected = asyncio.Event()
        self._subs_known = asyncio.Event()
        self._subs = set[str]()

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def send_ping(self, sender: bool = False):

        if isinstance(self.on_ping, Callable):
            try: await self.on_ping(self._WS, sender)
            except Exception as EXC:
                error = self.VERBOSE_NOCONN.format(self.name, 
                    "ping sent" if sender else "ping received")
                Log.exception(error, EXC)
                self._subs_known.clear()
                self._subs.clear()

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def listen(self, src: Connector):

        conn_name = f"{src.name}/{self.name}"
        async with ClientSession() as session:
            while src.active:
                try:
                    args = await self.url_args()
                    Log.info(self.VERBOSE_RECONN.format(conn_name, args["url"]))
                    async with session.ws_connect(**args, heartbeat = 30) as WS:
                        self._WS = WS ; self._WS_connected.set()
                        src._sockets[conn_name] = self._WS
                        await self._subs_known.wait()
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
                    self._subs.clear(); self._WS = None
                    self._WS_connected.clear()
                    self._subs_known.clear()
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

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class DataChannelWS(ChannelWS):
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __init__(self, name: str, on_message: Callable, url_args: Callable, 
                       on_ping: Callable = None, get_subs: Callable = None):

        super().__init__(name, on_message, url_args, on_ping)
        self.get_subs: Callable = get_subs
    
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def update(self, src: DataConnector):
        
        conn_name = f"{src.name}/{self.name}"
        Log.warning(f"WS for \"{conn_name}\" update loop started.")
        while src.active:
            await src._WS_to_resub.wait()
            await self._WS_connected.wait()
            if self._subs: await self.send_ping(True)

            subs_new = set()
            if (subs := src._sources_new[self.name]):
                subs_new, payload_new = self.get_subs(subs, True)
            subs_old = set()
            if (subs := src._sources_old[self.name]):
                subs_old, payload_old = self.get_subs(subs, False)

            src._WS_to_resub.clear()
            if (self._WS is None) or self._WS.closed:
                self._subs.clear(); continue
            if (not subs_old) and (not subs_new): continue
            Log.info(self.verbose_subs(subs_old, subs_new))

            try:
                if subs_new:
                    for payload in payload_new:
                        await self._WS.send_json(payload)
                    self._subs = self._subs | subs_new
                    src._sources_new[self.name].clear()
                    self._subs_known.set()
                if subs_old:
                    for payload in payload_old:
                        await self._WS.send_json(payload)
                    self._subs = self._subs - subs_old
                    src._sources_old[self.name].clear()
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
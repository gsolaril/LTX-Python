#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
import asyncio, json
from numba.core.types import NoneType
from pandas import Timestamp
from dataclasses import dataclass, field
from typing import Any, Any, Callable, List
from aiohttp import WSMsgType, ClientSession
from aiohttp import ClientWebSocketResponse
from sqlalchemy.sql.lambdas import NullLambdaStatement
from src.connectors.base import DataStream
from src.connectors.base import Connector
from src.models import *
from src.utils import *

#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class DataStreamWS(DataStream):
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __init__(self, name: str, get_subs: Callable, on_message: Callable,
                       get_urlh: Callable = None, on_ping: Callable = None):

        super().__init__(name)
        self.on_ping: Callable = on_ping
        self.get_subs: Callable = get_subs
        self.get_urlh: Callable = get_urlh
        self.on_message: Callable = on_message
        self._WS: ClientWebSocketResponse = None
        self._subs = set[str]()

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def send_ping(self, sender: bool = False):

        if isinstance(self.on_ping, Callable):
            try: await self.on_ping(self._WS, sender)
            except Exception as EXC:
                error = self.VERBOSE_NOCONN.format(self.name, 
                    "ping sent" if sender else "ping received")
                Log.exception(error, EXC) ; self._subs.clear()

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def update(self, connector: Connector):
        
        Log.warning(f"WS for \"{self.name}\" channel loop started.")
        
        while connector.active:
            await asyncio.sleep(1)
            if (self._WS is None) or self._WS.closed:
                await asyncio.sleep(0.5); continue
            elif self._subs: await self.send_ping(True)

            subs_new = list()
            if (symbols := connector._symbols_new):
                subs_new, payload_new = self.get_subs(symbols, True)
            subs_old = list()
            if (symbols := connector._symbols_old):
                subs_old, payload_old = self.get_subs(symbols, False)

            if (self._WS is None) or self._WS.closed:
                self._subs.clear(); continue
            if (not subs_old) and (not subs_new): continue
            Log.info(self.verbose_subs(subs_old, subs_new))

            try:
                if subs_new:
                    for payload in payload_new:
                        await self._WS.send_json(payload)
                    self._subs = self._subs | subs_new
                    connector._symbols_new.clear()
                if subs_old:
                    for payload in payload_old:
                        await self._WS.send_json(payload)
                    self._subs = self._subs - subs_old
                    connector._symbols_old.clear()
            except Exception as EXC:
                Log.exception(self.VERBOSE_NOCONN.format(self.name, "sub"), EXC)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def stream(self, connector: Connector):

        async with ClientSession() as session:
            while connector.active:
                try:
                    args = dict(await self.get_urlh())
                    Log.info(self.VERBOSE_RECONN.format(self.name, args["url"]))
                    async with session.ws_connect(**args, heartbeat = 30) as WS:
                        self._WS = WS
                        while not self._subs: await asyncio.sleep(0.5)
                        Log.info(self.VERBOSE_CONNED.format(self.name))
                        async for message in self._WS:
                            if (message.type == WSMsgType.TEXT):
                                try: asyncio.create_task(self.on_message(message.json()))
                                except json.JSONDecodeError:
                                    text = str(message.data)
                                    if (text.lower() == "pong"): pass
                                    elif (text.lower() == "ping"): await self.send_ping(False)
                                    else: Log.warning(self.VERBOSE_NOJSON.format(self.name, text))
                            elif (message.type == WSMsgType.PING): await self._WS.pong(message.data)
                            elif (message.type == WSMsgType.ERROR): raise self._WS.exception()
                            elif (message.type in {WSMsgType.CLOSED, WSMsgType.CLOSING}):
                                Log.warning(self.VERBOSE_CLOSED.format(self.name))
                            else:
                                Log.warning(self.VERBOSE_WDTYPE.format(self.name, message.type))

                except Exception as EXC:
                    Log.exception(self.VERBOSE_ERROR.format(self.name), EXC)
                    self._subs.clear(); self._WS = None
                    if connector.active: await asyncio.sleep(2)

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
cache: Callable = DataStreamWS.cache
#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class ConnectorWS(Connector):
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __init__(self, **streams):
        super().__init__()
        stream: DataStreamWS
        for stream in streams.values():
            self._streams[f"{stream.name}/stream"] = stream.stream
            self._streams[f"{stream.name}/update"] = stream.update

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def yield_update(self, symbols: set[str]): ...
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def update_specs_req(self):
        symbol: Symbol = None
        symbols = self._symbols_new
        if not symbols: symbols = self.symbols
        query_list: List[str] = list[str]()
        for symbol in await self.yield_update(symbols):
            query_list.append(symbol.sql_values)
            self._specs[symbol.symbol] = symbol
            while (len(self._specs) >= self.maxlen):
                self._specs.popitem(last = False)
        query_str = str.join(", ", query_list)
        query_str = Symbol.sql_update(query_str)
        with DB_ORM.connect() as conn:
            conn.execute(TextClause(query_str))
            conn.commit()

#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀

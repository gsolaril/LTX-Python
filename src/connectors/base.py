#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
import asyncio, asyncpg, json
from getpass import getpass
from bidict import bidict
from dataclasses import dataclass, field
from pandas import Timestamp, Timedelta
from typing import Any, ClassVar, NamedTuple

from src.models import *
from src.utils import *

#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄
class Venue:

    VENUE: str = ...
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    class Credentials(NamedTuple): aid: str
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __init__(self, creds: Credentials = None):
        self.creds = self.auth_local(creds)
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        if (cls is Venue): return
        venue: str = cls.__name__
        for base in cls.__bases__:
            if not issubclass(base, Venue): continue
            for word in ("Data", "Exec"):
                if venue.startswith(word):
                    venue = venue.replace(word, "")
        cls.VENUE = venue
    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def auth_local(cls, creds: Credentials = None):
        vault_dict = dict[str, str]()
        creds_dict = dict[str, str]()
        if creds is not None:
            creds_dict = creds._asdict()
            aid = creds_dict.pop("aid")
        for key, value in creds_dict.items():
            if not value:
                if not vault_dict:
                    vault_dict = Vault.secrets.kv.v2.read_secret_version(
                        path = cls.VENUE, raise_on_deleted_version = True,
                        mount_point = "creds")["data"]["data"][aid]
                value = vault_dict.get(key, None)
                if (value is None): value = getpass(
                    f"\"{aid}\"; type \"{key}\": ")
            assert isinstance(value, str) and (len(value) > 0), \
                f"\"{aid}\"; invalid \"{key}\": \"{value}\""
            creds_dict[key] = value
        return cls.Credentials(aid = aid, **creds_dict)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def update_timediff(self): ...
    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def symbol_to_local(cls, symbol: str): ...
    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def symbol_to_venue(cls, symbol: str): ...
    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def status_to_local(self, response: dict): ...

#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class StreamingBundle(Bundle):
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    @Redis.stream#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def resample(self, time: Timestamp = None):
        for candle in super().resample(): yield candle

#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄
@dataclass#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class Connector(ControllableAgent):
    sources: set[str] = field(init = False, kw_only = True, default_factory = set)
    # ↑ Sources' snapshot directly from DB. Already as set since "update_config".
    TABLE_CONFIG: ClassVar[Postgres.Table] = Postgres.Table.CONNECTORS
    IGNORE_TFS: ClassVar[set[TimeFrame]] = set()
    VENUE: ClassVar[str] = ...
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __post_init__(self):
        super().__post_init__()
        self.name = self.VENUE
        self._offset = Timedelta(0)          # Clock difference between venue/server and local time.
        self._specs = dict[str, Symbol]()    # Symbol specs from DB, for given venue, for symbols in dataclass attribute.
        self._sources_new = dict[str, set]() # Any new source (symbol / account / etc.) coming from updates, to be subscribed to.
        self._sources_old = dict[str, set]() # Any old source (symbol / account / etc.) coming from updates, to be unsubscribed from.
        self._sources = set[str]()           # Sources as state variable of the connector. New/old sources are initially absent/present here.
        self._sockets = dict[str, Any]()     # WebSocket objects already linked to URLs (used by ExecConnectors to send requests/orders).
        self._WS_to_resub = asyncio.Event()  # Trigger subscription methods of WebSocket objects, cron-based (Binance) or event-based (Polymarket).
        self._bundle = StreamingBundle(
              maxlen = 60, ignore_tfs = self.IGNORE_TFS.copy())
        self._crons[self._bundle.resample] = TimeFrame.MIN.value

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def setup(self):
        tasks = await super().setup() 
        for name, process in self._procs.items():
            process_name = f"{self.name}/{name}"
            tasks.append(asyncio.create_task(
              process(self), name = process_name))
        return tasks

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def get_stream_names(self, streams: set[str]): ...
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def reconfig(self, conn: asyncpg.Connection,
              venue: str = None, sources: set = None):
        await super().reconfig(conn)
        if sources is None: sources = self.sources.copy()
        sources_old = self._sources.difference(sources)
        sources_new = sources.difference(self._sources)
        streams = self.get_stream_names(sources_new)
        if streams: await Redis.add_streams(streams, self)
        for name in self._sources_new.keys():
            self._sources_new[name].update(sources_new)
            self._sources_old[name].update(sources_old)
        
        self._sources = sources
        self.config_verbose()
    
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def update_specs(self, conn: asyncpg.Connection,
                  venue: str = None, symbols: set = None):
        table = self.TABLE_SYMBOLS
        if (venue is None): venue = self.VENUE
        if (symbols is None): symbols = set(self._specs)
        query = f"SELECT * FROM {table} WHERE (venue = '{venue}')"
        if (len(symbols) > 0): 
            symbols_str = str.join(", ", [f"'{S}'" for S in symbols])
            query = query + " AND (symbol IN ({}))".format(symbols_str)
        result = [dict(row) for row in await conn.fetch(query)]
        if not result: return Log.error(f"No specs found:\n => {query}")
        for item in result: self._specs[item["symbol"]] = Symbol(**item)

#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄
@dataclass#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class DataConnector(Connector):
    STREAM_PREFIX: ClassVar[str] = "DATA"
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def get_stream_names(self, streams: set[str]):
        kw = {"venue": self.VENUE, "symbol": None}
        stream_names = set[str]()
        for symbol in streams:
            kw["symbol"] = symbol
            stream_names.add(
                Quote.STREAM_KEY.format(**kw, tf = "T1"))
            for tf in TimeFrame: stream_names.add(
                Quote.STREAM_KEY.format(**kw, tf = tf.name))
        return stream_names
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    @Postgres.on_table(Connector.TABLE_CONFIG)#█▄▄▄▄▄
    async def reconfig(self, conn: asyncpg.Connection,
              venue: str = None, sources: set = None):
        await super().reconfig(conn, venue, sources)
        await self.update_specs(conn, venue, self._sources)

#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄
@dataclass#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class ExecConnector(Connector):
    STREAM_PREFIX: ClassVar[str] = "EXEC"
    VERBOSE_EXEC: ClassVar[str] = "Order {0} {1}: {2}"
    VERBOSE_ERROR_OVER: ClassVar[str] = "Order map too large. Dropping oldest order...\n => {0!r}"
    class Reject(Exception): pass
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __post_init__(self):
        super().__post_init__()
        self._uid_to_acc = dict[str, str]()
        self._uid_to_eid = bidict[str, str]()
        self._accounts = dict[str, Account]()
        self._crons[self.update_specs] = TimeFrame.D1
        listen_orders = self.__class__.listen_orders
        name = f"{self.name}/listen_orders"
        self._procs[name] = listen_orders
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def get_stream_names(self, streams: set[str]):
        kw = {"venue": self.VENUE, "account_id": None}
        stream_names = set[str]()
        for account in streams:
            kw["account_id"] = account
            stream_names.add(Response.STREAM_KEY.format(**kw))
            for tf in TimeFrame:
                stream_name = Balance.STREAM_KEY.format(**kw,
                    symbol = "NAV", tf = tf.name)
                stream_names.add(stream_name)
        return stream_names
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    @Postgres.on_table(Connector.TABLE_CONFIG)#█▄▄▄▄▄
    async def reconfig(self, conn: asyncpg.Connection,
              venue: str = None, sources: set = None):
        await super().reconfig(conn, venue, sources)
        await self.update_specs(conn, venue)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def listen_orders(self):
        xstreams = dict[str, str]()
        for account_id in self._sockets.keys():
            xname = self.STREAM_PREFIX + "|" + account_id
            xstreams[xname] = "0-0"

        while self.active:
            account_id = "N/A"
            try:
                response = await Redis.xread(
                  streams = xstreams, count = 1)
                if not response: continue
                for stream, messages in response:
                    account_id = str.split(stream, "|")[-1]
                    for message_id, payload in messages:
                        xstreams[account_id] = message_id
                        await self.process_order(payload)
            except asyncio.CancelledError: break
            except Exception as EXC: Log.exception(f"\"{self.name}\" "
                    f"order listener failure @ \"{account_id}\"", EXC)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    @Redis.stream#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def process_order(self, payload: dict):
        action = payload.pop("action", "create")
        if (action == "create"): response = await self.create_order(payload)
        elif (action == "delete"): response = await self.delete_order(payload)
        elif (action == "modify"): response = await self.modify_order(payload)
        if (len(self._uid_to_eid) > 10000):
            request = self._uid_to_eid.popitem(last = False)
            Log.warning(self.VERBOSE_ERROR_OVER.format(request))
            # TODO: handle this case in "src/models/order.py" similar to Tick/Candle
            # "Response.__dict__" shall get "{stream: ..., time: ..., payload: ...}"
            yield response
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def create_order(self, payload: dict): ...
    async def delete_order(self, payload: dict): ...
    async def modify_order(self, payload: dict): ...
    async def sender(self, payload: dict, **kwargs): ...

#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄
class Channel:

    BULLET = "\n\t-> "
    VERBOSE_CONNED = "\"{}\" ready for messages."
    VERBOSE_RECONN = "\"{}\" connecting to \"{}\"..."
    VERBOSE_NOCONN = "\"{}\" {} failed (will retry after reconnect)"
    VERBOSE_CLOSED = "\"{}\" closed, reconnecting..."
    VERBOSE_WDTYPE = "\"{}\" weird type: \"{}\""
    VERBOSE_NOJSON = "\"{}\" got non-JSON: \"{}\""
    VERBOSE_ERROR = "\"{}\" error"
    VERBOSE_ERROR_XADD = "\"{}\" XADD failed:" + BULLET

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __init__(self, name: str): self.name = name
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def on_message(self, message: Any): ...
    async def listen(self, connector: Connector): ...
    async def update(self, connector: Connector): ...

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def verbose_subs(self, old: set, new: set):
        verbose = f"\"{self.name}\", reviewing subs..."
        if new: verbose += "\n => New (subs to):"
        for sub in sorted(new): verbose += self.BULLET + sub
        if old: verbose += "\n => Old (unsubs from):"
        for sub in sorted(old): verbose += self.BULLET + sub
        return verbose
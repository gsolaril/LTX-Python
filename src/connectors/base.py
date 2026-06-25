#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
import asyncio, asyncpg, json
from getpass import getpass
from dataclasses import dataclass, field
from pandas import Timestamp, Timedelta
from typing import Any, ClassVar, NamedTuple
from src.models import BaseAgent, Symbol, TimeFrame
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
#▄▄▄▄▄▄▄▄▄▄▄
@dataclass#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class Connector(BaseAgent):
    VENUE: ClassVar[str] = ...
    TABLE_CONFIG: ClassVar[str] = "connectors"
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __post_init__(self):
        super().__post_init__()
        self._streams = dict[str, object]()
        self._symbols_new = set[str]()
        self._sockets = dict[str, Any]()
        self._offset = Timedelta(0)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def setup(self):
        tasks = await super().setup() 
        for name, stream in self._streams.items():
            tasks.append(asyncio.create_task(
                stream(self), name = f"{self.name}/{name}"))
        return tasks

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    @Postgres.on_table(TABLE_CONFIG)
    async def reconfig(self, conn):
        await self.update_config(conn)
        await self.update_specs(conn)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def update_config(self, conn: asyncpg.Connection):
        FIELDS, TABLE = self.FIELDS_STR, self.TABLE_CONFIG
        query = f"SELECT {FIELDS} FROM {TABLE} WHERE (name = '{self.VENUE}');"
        row = await conn.fetchrow(query)
        if row is None:
            return Log.error(f"No config found for \"{self.VENUE}\":\n => {query}")
        config = dict[str, Any](row)
        report_freq = Timedelta(seconds = config["freq_report"])
        self._crons[Redis.report] = report_freq
        self.last_updated = Timestamp.now("UTC")
        symbols = config.pop("symbols", None)
        if symbols: symbols = json.loads(symbols)
        config["symbols"] = symbols
        for key, value in config.items():
            if (key == "symbols"): continue
            if (key == "name"): continue
            setattr(self, key, value)
        return config
    
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def update_specs(self, conn: asyncpg.Connection, symbols: set = None):
        TABLE, VENUE = self.TABLE_SYMBOLS, self.VENUE
        query = f"SELECT * FROM {TABLE} WHERE (venue = '{VENUE}')"
        if (symbols is None): symbols = set(self._specs)
        if (len(symbols) > 0): 
            symbols_str = str.join(", ", [f"'{S}'" for S in symbols])
            query = query + " AND (symbol IN ({}))".format(symbols_str)
        result = [dict(row) for row in await conn.fetch(query)]
        if not result: return Log.error(f"No specs found:\n => {query}")
        for item in result: self._specs[item["symbol"]] = Symbol(**item)
        while (len(self._specs) >= self.maxlen): self._specs.popitem(last = False)

#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄
@dataclass#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class DataConnector(Connector):
    symbols: set[str] = field(init = False,
      kw_only = True, default_factory = set)
    STREAM_PREFIX: ClassVar[str] = "DATA"
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def update_config(self, conn: asyncpg.Connection):
        config: dict = await super().update_config(conn)
        return config["symbols"]
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    @Postgres.on_table(Connector.TABLE_CONFIG)
    async def reconfig(self, conn: asyncpg.Connection):
        self._symbols_new = set[str]()
        self._symbols_old = set[str]()
        symbols_config = await self.update_config(conn)
        for symbol, keep in dict.items(symbols_config):
            available = (symbol in self.symbols)
            if available and keep: continue
            elif available and not keep:
                self._symbols_old.add(symbol)
                self.symbols.remove(symbol)
            elif not available and keep:
                self._symbols_new.add(symbol)
                self.symbols.add(symbol)

        await self.update_specs(conn, self.symbols)
        verbose = f"Config for \"{self.VENUE}\" updated:"
        for field in self.__dataclass_fields__.keys():
            if field[0].isupper(): continue
            value = getattr(self, field)
            verbose += f"\n => \"{field}\": {value!r}"
        Log.info(verbose)

#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄
@dataclass#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class ExecConnector(Connector):
    STREAM_PREFIX: ClassVar[str] = "EXEC"
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __post_init__(self):
        super().__post_init__()
        self._crons[self.update_specs] = TimeFrame.D1
        listen_orders = self.__class__.listen_orders
        name = f"{self.name}/listen_orders"
        self._streams[name] = listen_orders

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
                        await self.sender(account_id, payload)
            except asyncio.CancelledError: break
            except Exception as EXC: Log.exception(f"\"{self.name}\" "
                    f"order listener failure @ \"{account_id}\"", EXC)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def sender(self, aid: str, payload: dict): ...
    # TODO: get payload from order-like request and send to exchange. Each "AccountConnector" subclass shall
    # implement its own sender method, including its order-to-payload conversion, and its socket object.

#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄
class Stream:

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
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def update(self, connector: Connector): ...
    async def stream(self, connector: Connector): ...
    async def on_message(self, message: Any): ...

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def verbose_subs(self, old: set, new: set):
        verbose = f"\"{self.name}\", reviewing subs..."
        if new: verbose += "\n => New (subs to):"
        for sub in sorted(new): verbose += self.BULLET + sub
        if old: verbose += "\n => Old (unsubs from):"
        for sub in sorted(old): verbose += self.BULLET + sub
        return verbose
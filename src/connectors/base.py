#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
import asyncio, asyncpg, json
from getpass import getpass
from dataclasses import dataclass, field
from pandas import Timestamp, Timedelta
from typing import Any, ClassVar, Callable
from typing import Any, NamedTuple
from collections import OrderedDict
from src.models import *
from src.utils import *

#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class Meta(type):
    REMOVE_WORDS = ("Data", "Exec")
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __new__(mcls: type, name: str, bases: tuple[type], namespace: dict[str, Any]):
        cls = super().__new__(mcls, name, bases, namespace)
        if any(issubclass(base, Venue) for base in bases):
            if ((venue := name) != "Venue"):
                for word in Meta.REMOVE_WORDS:
                    if not venue.startswith(word): continue
                    venue = venue.replace(word, "").strip()
                cls.VENUE = venue
        return cls

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class Venue(metaclass = Meta):

    VENUE: str = ...
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    class Credentials(NamedTuple): aid: str
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __init__(self, creds: Credentials = None):
        self.creds = self.auth_local(creds)
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
@dataclass#█▄▄▄
class Connector:
    name: str = field(init = False, kw_only = True, default = None)
    url: str = field(init = False, kw_only = True, default = None)
    maxlen: int = field(init = False, kw_only = True, default = 10000)
    active: bool = field(init = False, kw_only = True, default = False)
    last_written: Timestamp = field(init = False, kw_only = True, default = None)
    last_updated: Timestamp = field(init = False, kw_only = True, default = None)

    VENUE: ClassVar[str] = ...
    STREAM_PREFIX: ClassVar[str] = ...
    TABLE_CONFIG: ClassVar[str] = "connectors"
    TABLE_SYMBOLS: ClassVar[str] = "symbol_specs"
    VERBOSE_XADD_OK: ClassVar[str] = "[Q{}] \"{}\" XADD @ {} => {}"
    VERBOSE_XADD_ERROR: ClassVar[str] = "\"{}\" XADD failed:\n => {}"
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __post_init__(self):
        self.name = self.__class__.__name__
        self._streams = dict[str, object]()
        self._specs = OrderedDict[str, Symbol]()
        self._tasks = dict[str, asyncio.Task]()
        self._crons = dict[Callable, TimeFrame]()
        self._symbols_new = set[str]()
        self._sockets = dict[str, Any]()
        self._offset = Timedelta(0)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def start_cron(self, cron: Callable, tf: TimeFrame):
        class_name = self.__class__.__name__
        cron_name = class_name + "/cron/" + cron.__name__
        error = f"\"{cron_name}\" cron loop failed"
        next = Timestamp.now("UTC").ceil(tf.value)
        while self.active:
            if (now := Timestamp.now("UTC")) < next:
                await asyncio.sleep(0.5) ; continue
            try: next = now.ceil(tf.value) ; await cron()
            except Exception as EXC: Log.exception(error, EXC)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def start(self):
        try:
            DBListener.bind(self)
            verbose_list = list[str]()
            class_name = self.__class__.__name__

            name = f"{class_name}/Postgres/listener"
            self._tasks[name] = asyncio.create_task(DBListener(), name = name)
            verbose_list.append(f" => {name}: {self._tasks[name]!r}")
            await DBListener.wait()
            name = f"{class_name}/Redis/writer"
            self._tasks[name] = asyncio.create_task(self.writer(), name = name)
            verbose_list.append(f" => {name}: {self._tasks[name]!r}")

            for cron, tf in self._crons.items():
                name = f"{class_name}/cron/{cron.__name__}:{tf!r}"
                self._tasks[name] = asyncio.create_task(
                    self.start_cron(cron, tf), name = name)
                verbose_list.append(f" => {name}: {self._tasks[name]!r}")

            for name, stream in self._streams.items():
                name = f"{class_name}/{name}"
                self._tasks[name] = asyncio.create_task(stream(self), name = name)
                verbose_list.append(f" => {name}: {self._tasks[name]!r}")
            verbose = f"Starting {len(self._tasks)} tasks in \"{class_name}\":\n"
            Log.info(verbose + str.join("\n", verbose_list))
            results = await asyncio.gather(*self._tasks.values(), return_exceptions = True)
            for result in results:
                if (result is not None): raise result
        
        except KeyboardInterrupt: Log.success("Exiting...")
        except Exception as EXC: Log.exception(EXC)
        finally: self.active = False

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def writer(self):
        self._queue = asyncio.Queue(self.maxlen)
        while self.active:
            N: int = self._queue._queue.__len__()
            if (N == 0): await asyncio.sleep(1e-3)
            else:
                point: BasePoint = await self._queue.get()
                suffix, time_us, payload = point.as_cache
                stream = self.STREAM_PREFIX + "|" + suffix
                id = str(time_us)[: -3] + "-" + str(time_us)[-3:]
                if not await Redis.exists(stream): await Redis.xgroup_create(
                      name = stream, groupname = stream, id = "$", mkstream = True)
                try: assert (await Redis.xadd(stream, payload, id, int(self.maxlen)))
                except Exception as EXC: Log.error(
                    self.VERBOSE_XADD_ERROR.format(stream, payload), EXC)
                if Config.DEBUG_MODE: Log.debug(
                    self.VERBOSE_XADD_OK.format(N, stream, id, payload))

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    @DBListener.on_table(TABLE_CONFIG)
    async def reconfig(self, conn):
        await self.update_config(conn)
        await self.update_specs(conn)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def update_config(self, conn: asyncpg.Connection):
        TABLE = self.TABLE_CONFIG
        fields = str.join(", ", self.__dataclass_fields__.keys())
        query = f"SELECT {fields} FROM {TABLE} WHERE (name = '{self.VENUE}');"
        config = dict[str, Any](await conn.fetchrow(query))
        for key, value in config.items():
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
    STREAM_PREFIX: ClassVar[str] = "LTX|DATA"

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    @DBListener.on_table(Connector.TABLE_CONFIG)
    async def reconfig(self, conn: asyncpg.Connection):
        symbols_config = await self.update_config(conn)
        self._symbols_new = set[str]()
        self._symbols_old = set[str]()
        for symbol, keep in symbols_config.items():
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

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def update_config(self, conn: asyncpg.Connection):
        TABLE = self.TABLE_CONFIG
        query = f"SELECT * FROM {TABLE} WHERE (name = '{self.VENUE}');"
        result: dict = dict[str, Any](await conn.fetchrow(query))
        symbols_json: dict = json.loads(result.pop("symbols"))
        self.last_updated = Timestamp.now("UTC")
        for key, value in result.items():
            if (key == "name"): continue
            setattr(self, key, value)
        return symbols_json

#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄
@dataclass#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class ExecConnector(Connector):
    STREAM_PREFIX: ClassVar[str] = "LTX|EXEC"
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
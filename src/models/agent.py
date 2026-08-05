#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
import asyncio, json, datetime
from asyncpg import Connection
from collections import OrderedDict
from typing import Any, ClassVar, Callable
from dataclasses import dataclass, field
from pandas import Timestamp, Timedelta
from loguru import logger as Log
from .misc import Symbol, TimeFrame
from src.utils import Postgres, Redis, TZ

#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄
@dataclass#█▄▄▄
class BaseAgent:
    debug: bool = field(init = False, kw_only = True, default = False)
    active: bool = field(init = False, kw_only = True, default = False)
    last_written: Timestamp = field(init = False, kw_only = True, default = None)
    last_updated: Timestamp = field(init = False, kw_only = True, default = None)
    
    TABLE_SYMBOLS: ClassVar[str] = Postgres.Table.SYMBOLS.value
    VERBOSE_TASK: ClassVar[str] = "\n => {0}: \"{1}\""
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __post_init__(self):
        self.name = self.__class__.__name__
        self._tasks = dict[str, asyncio.Task]()    # All coroutines holding sync, async and client-related processes.
        self._crons = dict[Callable, Timedelta]()  # Sync/Cron processes, that run at a given frequency; every N secs/mins/hs
        self._procs = dict[str, Callable]()        # Async processes (e.g.: Channel methods) that work on the background.

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def start_cron(self, cron: Callable):
        cron_name = self.name + "/cron/" + cron.__name__
        error = f"\"{cron_name}\" cron loop failed"
        next = Timestamp.min.tz_localize("UTC")
        while self.active:
            if (now := Timestamp.now(TZ)) < next:
                await asyncio.sleep(0.5) ; continue
            next = now.ceil(self._crons[cron])
            try:
                if getattr(cron, "__self__", None) is self:
                    await cron()
                else:
                    await cron(self)
            except Exception as EXC:
                Log.exception(error, EXC)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def setup(self):
        self.active = True
        tasks = list[asyncio.Task]()
        for cron, freq in self._crons.items():
            try: tf = TimeFrame(freq).name
            except: tf = f"S{freq: int}"
            name = f"{self.name}/cron/{cron.__name__}/{tf}"
            tasks.append(asyncio.create_task(
                self.start_cron(cron), name = name))
        for name, process in self._procs.items():
            process_name = f"{self.name}/{name}"
            tasks.append(asyncio.create_task(
              process(self), name = process_name))
        return tasks

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def start(self):
        try:
            verbose = ""
            self._tasks = await self.setup()
            for task in self._tasks:
                name, coro = task.get_name(), task.get_coro().__qualname__
                verbose = verbose + self.VERBOSE_TASK.format(name, coro)
            Log.info(f"Started {len(self._tasks)} tasks in \"{self.name}\"... {verbose}")
            results = await asyncio.gather(*self._tasks, return_exceptions = True)
            for result in results:
                if (result is not None): raise result
        except KeyboardInterrupt: Log.success("Exiting...")
        except Exception as EXC: Log.exception(EXC)
        finally: self.active = False

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def config_verbose(self):
        verbose = f"Config for \"{self.name}\" set as:"
        for field in self.__dataclass_fields__.keys():
            if field[0].isupper(): continue
            value = getattr(self, field)
            if isinstance(value, datetime.datetime):
                value = value.isoformat(" ")
            verbose += f"\n => \"{field}\": {value!r}"
        Log.info(verbose)

#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄
@dataclass#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class StreamingAgent(BaseAgent):
    maxlen_redis: int = field(init = False, kw_only = True, default = 10000)
    freq_redis_report: int = field(init = False, kw_only = True, default = 600)
    STREAM_PREFIX: ClassVar[str] = ...
    XGROUP: ClassVar[str] = ...
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def setup(self):
        tasks = list[asyncio.Task]()
        tasks.append(asyncio.create_task(Redis(self),
                name = f"{self.name}/Manager/Redis"))
        await Redis.wait()
        tasks.extend(await super().setup())
        self._crons[Redis.report] = Timedelta(
            seconds = self.freq_redis_report)
        return tasks

#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄
@dataclass#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class ControllableAgent(StreamingAgent):
    TABLE_CONFIG: ClassVar[...] = ...
    FIELDS: ClassVar[list[str]] = ...
    FIELDS_STR: ClassVar[str] = ...
    FREQ_REPORT_DEFAULT: ClassVar[int] = 300
    SYM_QUERY_BY: ClassVar[str] = "ALL"
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __post_init__(self):
        super().__post_init__()
        self._conn: Connection = None
        self._specs: dict[str, Symbol] = dict()
        self.sym_query: Callable = Symbol.QUERY_BY[self.SYM_QUERY_BY]
    
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        cls.FIELDS = list()
        for field in cls.__dataclass_fields__:
            if str.islower(field[0]): cls.FIELDS.append(field)
        cls.FIELDS_STR = str.join(", ", cls.FIELDS)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def setup(self):
        tasks = list[asyncio.Task]()
        tasks.append(asyncio.create_task(Postgres(self),
                name = f"{self.name}/Manager/Postgres"))
        await Postgres.wait()
        tasks.extend(await super().setup())
        return tasks

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    @Postgres.on_table_config(TABLE_CONFIG)#█▄▄▄▄▄▄▄▄
    async def _reconfig(self, config: dict[str, Any]):
        sources: set[str] = config.pop("sources", set())
        for key in self.FIELDS:
            if not key in config: continue
            setattr(self, key, config[key])
        if ("freq_redis_report" in self.FIELDS):
            freq = self.FREQ_REPORT_DEFAULT
            freq = getattr(self, "freq_redis_report", freq)
            self._crons[Redis.report] = Timedelta(seconds = freq)
        await self.reconfig(sources)
        self.last_updated = Timestamp.now(TZ)
        if hasattr(self, "sources"):
            self.sources = sources
        self.config_verbose()

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def update_specs(self, venue: str, symbols: set[str]):
        query = f"SELECT * FROM {self.TABLE_SYMBOLS} WHERE (venue = '{venue}')"
        if (self.sym_query is not None): query = query + self.sym_query(symbols)
        if self.debug: Log.debug(f"Querying specs:\n => {query}")
        result = [dict(row) for row in await self._conn.fetch(query)]
        if not result: return Log.error(f"No specs found:\n => {query}")
        for item in result: self._specs[item["symbol"]] = Symbol(**item)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def reconfig(self, sources: set[str]): ...
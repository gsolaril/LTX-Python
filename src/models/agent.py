#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
import asyncio, asyncpg, json, datetime
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
        verbose = f"Config for \"{self.name}\" updated:"
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
    
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    @Postgres.on_table(TABLE_CONFIG)#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def reconfig(self, conn: asyncpg.Connection):
        await self.update_config(conn)
        if ("freq_redis_report" in self.FIELDS):
            freq = self.FREQ_REPORT_DEFAULT
            freq = getattr(self, "freq_redis_report", freq)
            self._crons[Redis.report] = Timedelta(seconds = freq)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def update_config(self, conn: asyncpg.Connection):
        FIELDS, TABLE = self.FIELDS_STR, self.TABLE_CONFIG
        query = f"SELECT {FIELDS} FROM {TABLE} WHERE (name = '{self.name}');"
        row = await conn.fetchrow(query)
        if row is None: return Log.error(
            f"No config found for \"{self.name}\":\n => {query}")
        config = dict[str, Any](row)
        self.last_updated = Timestamp.now(TZ)
        for key, value in config.items():
            if key.startswith("sources"):
                sources = json.loads(value).items()
                value = {S for S, V in sources if V}
            setattr(self, key, value)
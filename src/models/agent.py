#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
import asyncio
from collections import OrderedDict
from typing import Any, ClassVar, Callable
from dataclasses import dataclass, field
from pandas import Timestamp, Timedelta
from src.utils import Log, Postgres, Redis
from src.models import Symbol

#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class BaseAgentMeta(type):
    def __new__(mcls: type, name: str, bases: tuple[type], namespace: dict[str, Any]):
        cls = super().__new__(mcls, name, bases, namespace)
        cls.FIELDS = list()
        for field in getattr(cls, "__dataclass_fields__", []):
            if str.islower(field[0]): cls.FIELDS.append(field)
        cls.FIELDS_STR = str.join(", ", cls.FIELDS)
        return cls

#▄▄▄▄▄▄▄▄▄▄▄
@dataclass#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class BaseAgent(metaclass = BaseAgentMeta):
    name: str = field(init = False, kw_only = True, default = None)
    url: str = field(init = False, kw_only = True, default = None)
    maxlen: int = field(init = False, kw_only = True, default = 10000)
    active: bool = field(init = False, kw_only = True, default = False)
    freq_report: int = field(init = False, kw_only = True, default = 600)
    last_written: Timestamp = field(init = False, kw_only = True, default = None)
    last_updated: Timestamp = field(init = False, kw_only = True, default = None)
    
    STREAM_PREFIX: ClassVar[str] = ...
    TABLE_CONFIG: ClassVar[str] = ...
    TABLE_SYMBOLS: ClassVar[str] = "symbol_specs"
    VERBOSE_TASK: ClassVar[str] = " => {0}: {1!r}"
    FIELDS: ClassVar[list[str]] = ...
    FIELDS_STR: ClassVar[str] = ...
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __post_init__(self):
        if (self.name is None):
            self.name = self.__class__.__name__
        self._specs = OrderedDict[str, Symbol]()
        self._crons = dict[Callable, Timedelta]()
        self._tasks = dict[str, asyncio.Task]()
        
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def start_cron(self, cron: Callable):
        class_name = self.__class__.__name__
        cron_name = class_name + "/cron/" + cron.__name__
        error = f"\"{cron_name}\" cron loop failed"
        next = Timestamp.min.tz_localize("UTC")
        while self.active:
            if (now := Timestamp.now("UTC")) < next:
                await asyncio.sleep(0.5) ; continue
            next = now.ceil(self._crons[cron])
            try: await cron(self)
            except Exception as EXC:
                Log.exception(error, EXC)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def setup(self):
        self.active = True
        logs = list[str]()
        tasks = dict[str, asyncio.Task]()

        name = f"{self.name}/Manager/Postgres"
        tasks[name] = asyncio.create_task(
            Postgres(self), name = name)
        logs.append(self.VERBOSE_TASK.format(name, tasks[name]))
        await Postgres.wait()

        name = f"{self.name}/Manager/Redis"
        tasks[name] = asyncio.create_task(
            Redis(self), name = name)
        logs.append(self.VERBOSE_TASK.format(name, tasks[name]))
        await Redis.wait()

        self._crons[Redis.report] = Timedelta(seconds = self.freq_report)
        for cron in self._crons.keys():
            name = f"{self.name}/cron/{cron.__name__}"
            tasks[name] = asyncio.create_task(
                self.start_cron(cron), name = name)
            logs.append(self.VERBOSE_TASK.format(name, tasks[name]))

        return tasks, logs

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def start(self):
        try:
            self._tasks, logs = await self.setup()
            verbose = f"Started {len(self._tasks)} tasks in \"{self.name}\":"
            Log.info(verbose + "\n" + str.join("\n", logs))
            results = await asyncio.gather(*self._tasks.values(), return_exceptions = True)
            for result in results:
                if (result is not None): raise result
        except KeyboardInterrupt: Log.success("Exiting...")
        except Exception as EXC: Log.exception(EXC)
        finally: self.active = False
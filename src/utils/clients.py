#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
import os, sys, psutil, asyncio, json, asyncpg, functools, enum, time
from pandas import DataFrame, Timestamp, Timedelta
from typing import Any, Callable, ClassVar, Iterable
from collections import deque
from redis.exceptions import ResponseError
from redis.asyncio import Redis as RedisClient
from clickhouse_driver import Client as ClickHouseClient
from .base import Credentials, Log, TZ
from .misc import Queue, Reporter

EventLoop: asyncio.AbstractEventLoop
EventLoop = asyncio.new_event_loop()
asyncio.set_event_loop(EventLoop)

#███████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class PostgresManager:
    _QUERY_NOTIFY = """
      CREATE OR REPLACE FUNCTION notify_{0}() RETURNS trigger AS $$ BEGIN
          PERFORM pg_notify('{0}', json_build_object('operation', TG_OP,
          'table', TG_TABLE_NAME, 'time', CURRENT_TIMESTAMP, 'query_tag',
          current_setting('app.query_tag', TRUE))::text ); RETURN NULL;
      END; $$ LANGUAGE plpgsql;
      DROP TRIGGER IF EXISTS {0}_trigger ON {0};
      CREATE TRIGGER {0}_trigger
      AFTER INSERT OR UPDATE OR DELETE ON {0}
      FOR EACH STATEMENT EXECUTE FUNCTION notify_{0}();
      """
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    class Table(enum.StrEnum):
        CONNECTORS = "connectors"
        MONITORING = "monitoring"
        SYMBOLS = "symbol_specs"
    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def _create(cls, creds: Credentials = None):
        if (creds is None): creds = Credentials.get_for("postgres")
        url = "postgresql://{USERNAME}:{PASSWORD}@{IP}/{DATABASE}"
        url = url.format(**creds._asdict())
        query = "SELECT 1"
        client = await asyncpg.create_pool(url)
        async with client.acquire() as conn:
            assert await conn.fetchval(query) == 1
        return client        
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __init__(self, creds: Credentials = None):
        client = EventLoop.run_until_complete(self._create(creds))
        self._conn: asyncpg.Connection = None
        self._decorated = dict[str, dict]()
        self._client: asyncpg.Pool = client
        self._ready = asyncio.Event()
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def wait(self):
        return await self._ready.wait()
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def _queue_handler(self, _conn, _pid, _channel, payload):
        self._queue.put_nowait(payload)
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def _decorator_base(self, make_listener: Callable, table: Any):
        def decorator(func: Callable):
            owner = func.__qualname__.rsplit(".", 1)[0]
            key = table.value if isinstance(table, self.Table) else (
                table if isinstance(table, str) else None)
            self._decorated.setdefault(owner, {})[key] = make_listener(func, table)
            return func
        return decorator
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def on_table_base(self, table: Any):
        def make_listener(func: Callable, _table: Any):
            async def listener(src: Any):
                return await func(src)
            return listener
        return self._decorator_base(make_listener, table)
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def on_table_config(self, table: Any = None):
        def make_listener(func: Callable, decorated_table: Any):
            async def listener(src: Any):
                t = decorated_table if isinstance(decorated_table, self.Table) \
                    else getattr(src, "TABLE_CONFIG", None)
                if not isinstance(t, self.Table):
                    return Log.error(f"No TABLE_CONFIG for \"{type(src).__name__}\"")
                if src._conn is None:
                    return Log.error(f"No Postgres conn on \"{type(src).__name__}\"")
                query = f"SELECT * FROM {t.value} WHERE (name = '{src.name}');"
                row = await src._conn.fetchrow(query)
                if (row is None): return Log.error(
                    f"No config found for \"{src.name}\":\n => {query}")
                config = dict(row)
                if "sources" in config:
                    sources: dict = json.loads(config.pop("sources"))
                    config["sources"] = {S for S, V in sources.items() if V}
                return await func(src, config = config)
            return listener
        return self._decorator_base(make_listener, table)
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def _get_listeners(self, src: Any):
        listeners = dict[str, Callable]()
        mro = type(src).mro()
        mro_names = {C.__name__ for C in mro}
        for owner in list(self._decorated):
            if owner not in mro_names: self._decorated.pop(owner)
        for cls in mro:  # most specific first
            for table, func in self._decorated.get(cls.__name__, {}).items():
                if table is None:
                    t = getattr(src, "TABLE_CONFIG", None)
                    if not isinstance(t, self.Table): continue
                    table = t.value
                if table not in listeners: listeners[table] = func
        return listeners
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def __call__(self, src: Any):
        self._queue = Queue(maxsize = 10)
        listeners = self._get_listeners(src)
        async with self._client.acquire() as self._conn:
            async with self._client.acquire() as src._conn:
                try: [await func(src) for func in listeners.values()]
                except Exception as EXC: Log.exception(EXC); raise EXC

                try:
                    verbose = ["Postgres listeners started:"]
                    for table, func in listeners.items():
                        await self._conn.execute(self._QUERY_NOTIFY.format(table))
                        await self._conn.add_listener(table, self._queue_handler)
                        verbose.append(f"\"{table}\": \"{func.__qualname__}\"")
                    Log.success(str.join("\n => ", verbose))
                except Exception as EXC: Log.exception(EXC); raise EXC

                self._ready.set()
                while src.active:
                    try:
                        response = json.loads(await self._queue.get())
                        func = listeners.get(response["table"], None)
                        if (func is not None): await func(src)
                    except Exception as EXC: Log.exception(EXC); break

            for table in listeners:
                await self._conn.remove_listener(
                    table, self._queue_handler)
                
        self._conn = None
        src._conn = None

#███████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class RedisManager:
    VERBOSE_PERF = "Process \"{name}\" took {dus} μs, {cpu}% CPU, {ram}B RAM"
    VERBOSE_ERROR = "\"{}\" XADD failed:\n => {}"
    VERBOSE_XADD = "[Q{}] \"{}\" XADD @ {} => {}"
    VERBOSE_CP = "Warning: Queue above {0:.0%}."
    STREAM_PREFIX: ClassVar[str] = "LTX"
    VERBOSE_QUEUE = "Queue is {:.0%} full!"
    PRINT_LIMIT = 50
    CHECKPOINTS = {
        0.5: lambda value: Log.warning("Queue is {:.0%} full!".format(value)),
        0.8: lambda value: Log.warning("Queue is {:.0%} full!".upper().format(value)),
        0.95: lambda value: Log.critical("Queue is {:.0%} full!".upper().format(value)),
    }
    SEP = "|"
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    class Group(enum.StrEnum):
        DATA, MONITOR, EXEC = "$", "$", "$"
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    class StreamGet(enum.StrEnum):
        ALL, NEW, LAST = "0-0", ">", "$"
    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def _create(cls, creds: Credentials = None):
        if (creds is None): creds = Credentials.get_for("redis")
        host, port = creds.IP.split(":")
        tstr = Timestamp.now(TZ).strftime("%Y%m%d%H%M%S%f")
        args = {"decode_responses": True, "host": host, "port": int(port),
          "username": creds.USERNAME, "password": creds.PASSWORD, "db": 0}
        client = RedisClient(**args)
        query_1, query_2 = f"SET test {tstr}", f"GET test"
        assert (await client.execute_command(query_1) == "OK")
        assert (await client.execute_command(query_2) == tstr)
        return client
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __init__(self, creds: Credentials = None):
        client = EventLoop.run_until_complete(self._create(creds))
        self._client: RedisClient = client
        self._reporter = Reporter(name = "RedisManager")
        self._ready = asyncio.Event()
        self._streams = set[str]()
        self._ncp = 0.0
        self._queue = None
        self._pending = deque()
        self._proc = psutil.Process(os.getpid())

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def _enqueue(self, payload: dict):
        if self._queue is not None:
            self._queue.put_nowait(payload)
        else: self._pending.append(payload)
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def scan(self, pattern: str = STREAM_PREFIX + "|*"):
        async for K in self._client.scan_iter(pattern): yield K
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def xread(self, src: Any, streams: dict[str, str],
                    group: Group = None, *args, **kwargs):
        kwargs.setdefault("count", 100)
        kwargs.setdefault("block", 1000)
        if group is not None:
            consumer = src.__class__.__name__
            if src.name: consumer += "|" + src.name
            return await self._client.xreadgroup(
                        group.name, consumer, streams, *args, **kwargs)
        else: return await self._client.xread(streams, *args, **kwargs)
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def xrecent(self, xstreams: set[str], n: int = 1):
        for stream in xstreams:
            for mid, payload in await self._client.xrevrange(
              stream, count = n): yield stream, mid, payload
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def xack(self, stream: str, group: Group, message_id: str):
        return await self._client.xack(stream, group.name, message_id)
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def ensure_groups(self, xstreams: set[str]):
        for stream in xstreams:
            if stream in self._streams: continue
            mkstream = not await self._client.exists(stream)
            await self.xcreategroups(stream, mkstream = mkstream)
            self._streams.add(stream)
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def xcreategroups(self, stream: str, *, mkstream: bool = True):
        for group, value in self.Group.__members__.items():
            try: await self._client.xgroup_create(
                    stream, group, value, mkstream)
            except ResponseError as EXC:
                if "BUSY" not in str(EXC):
                    Log.exception(EXC); raise
            mkstream = False
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def add_streams(self, streams: list[str]):
        for suffix in streams:
            stream = self.STREAM_PREFIX + self.SEP + suffix
            if stream in self._streams: continue
            mkstream = not await self._client.exists(stream)
            await self.xcreategroups(stream, mkstream = mkstream)
            self._streams.add(stream)
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def wait(self):
        return await self._ready.wait()
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def report(self, src: Any):
        freq = Timedelta(seconds = src.freq_redis_report)
        next_at = Timestamp.now(TZ).ceil(freq)
        self._reporter.close_batch()
        report = self._reporter.to_string(next_at)
        if report: Log.info("Redis' " + report)
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def stream(self, func: Callable = None):
        def decorator(func: Callable):
            @functools.wraps(func)
            async def wrapped(src, *args, **kwargs):
                gen = func(src, *args, **kwargs)
                results = deque()
                async for obj in gen:
                    results.append(obj)
                    if (obj is None): continue
                    payload: dict = obj.__dict__
                    payload["stream"] = src.STREAM_PREFIX + self.SEP + payload["stream"]
                    self._enqueue(payload)
                return results
            return wrapped
        if func is None: return decorator
        return decorator(func)
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def profiler(self, func: Callable = None, log: bool = False):
        def decorator(func: Callable):
            @functools.wraps(func)
            async def wrapped(src, *args, **kwargs):
                payload = dict()
                s_time = int(time.time() * 1e6)
                d_cpu = self._proc.cpu_percent()
                d_ram = self._proc.memory_info().rss
                result = await func(src, *args, **kwargs)
                payload["ram"] = self._proc.memory_info().rss - d_ram
                payload["cpu"] = self._proc.cpu_percent() - d_cpu
                payload["dus"] = int(time.time() * 1e6) - s_time
                stream = "PERF" + self.SEP + (name := src.name + self.SEP + func.__name__)
                self._enqueue({"stream": stream, "time_event": s_time, "payload": payload})
                if log: Log.info(self.VERBOSE_PERF.format(name = name, **payload))
                return result
            return wrapped
        if func is None: return decorator
        return decorator(func)
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def __call__(self, src: Any):
        self._queue = Queue(maxsize = src.maxlen_redis,
            checkpoints = self.CHECKPOINTS.copy())
        while self._pending:
            self._queue.put_nowait(self._pending.popleft())
        self._ready.set()
        while src.active:
            while (N := self._queue.qsize()) == 0:
                await asyncio.sleep(1e-6); continue
            else:
                try:
                    payload: dict = await self._queue.get()
                    suffix, time_event, payload = payload.values()
                    stream = self.STREAM_PREFIX + self.SEP + suffix
                    id = str(time_event)[: -3] + "-" + str(time_event)[-3 :]
                    if suffix not in self._streams: await self.add_streams([suffix])
                    payload["qdus"] = int(time.time() * 1e6 - time_event)
                    assert (await self._client.xadd(stream, payload, id, src.maxlen_redis))
                    if src.debug: Log.debug(self.VERBOSE_XADD.format(N, stream, id, payload))
                    self._reporter.add(suffix)
                except Exception as EXC:
                    Log.exception(EXC)
    
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def consume(self, func: Callable,
      src: Any, xstreams: set, n: int = 0):

        name = f"\"{src.name}.{func.__name__}\" "
        xstr = str.join("\n", map(" => \"{}\"".format, xstreams))
        verbose = name + f"tracking X-Streams & {{0}}:\n" + xstr
        xdict = dict.fromkeys(xstreams, src.XGROUP.value)
      
        if not n:
            action = "ensuring groups"
            Log.info(verbose.format(action))
            await self.ensure_groups(xstreams)
        elif isinstance(n, int) and (n > 0):
            action = f"retrieving tail ({n})"
            Log.info(verbose.format(action))
            gen = self.xrecent(xstreams, n = n)
            async for stream, mid, payload in gen:
                await func(stream, mid, payload)

        while True:
            try:
                response = await self.xread(src, xdict)
                if not response: continue
                for stream, messages in response:
                    for mid, payload in messages:
                        if not isinstance(payload, dict): continue
                        await self.xack(stream, src.XGROUP, mid)
                        try: await func(stream, mid, payload)
                        except Exception as EXC: Log.exception(EXC)

            except asyncio.CancelledError: break
            except Exception as EXC: Log.exception(EXC); break

#███████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class ClickHouseManager:
    VERBOSE_PUSH = "Pushed {0} rows to \"{1}\":\n => {2}"
    VERBOSE_ERROR = "Failed to write to \"{0}\":"
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    class Table(enum.StrEnum):
        TICKS = "history_ticks"
        CANDLES = "history_candles"
    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def _create(cls, creds: Credentials = None):
        if (creds is None): creds = Credentials.get_for("clickhouse")
        host, port = creds.IP.split(":")
        query = "SELECT 1"
        args = {"user": creds.USERNAME, "password": creds.PASSWORD,
          "host": host, "port": int(port), "database": creds.DATABASE}
        client = ClickHouseClient(**args)
        assert client.execute(query) == [(1,)]
        return client
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __init__(self, creds: Credentials = None):
        client = EventLoop.run_until_complete(self._create(creds))
        self._client: ClickHouseClient = client
        self._ready = set[str]()
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def parse(self, row: dict, columns: list[str]):
        values = list()
        for col in columns:
            value = row.get(col)
            if isinstance(value, Timestamp):
                value = value.to_pydatetime()
            values.append(value)
        return tuple(values)
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def write(self, table: str, rows: Iterable[dict]):
        columns, data = None, list()
        for row in rows:
            if columns is None: columns = list(row.keys())
            data.append(self.parse(row, columns))
        if data:
            fields = str.join(", ", columns)
            query = f"INSERT INTO {table} ({fields}) VALUES"
            self._client.execute(query, data, types_check = True)
        else: Log.warning(f"No rows written to \"{table}\"")
        return columns, data
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def flush(self, table: str, func: Callable, instance, *args, **kwargs):
        columns, data = None, list()
        for row in func(instance, *args, **kwargs):
            if columns is None: columns = list(row.keys())
            data.append(self.parse(row, columns))
        if data:
            fields = str.join(", ", columns)
            query = f"INSERT INTO {table} ({fields}) VALUES"
            await asyncio.to_thread(self._client.execute, query, data, True)
        else: Log.warning(f"No rows written to \"{table}\"")
        return columns, data
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def to_table(self, func: Callable[Iterable[dict]] = None, *, table: Table):
        def decorator(func: Callable[Iterable[dict]]):
            return self.Writer(self, table.value, func)
        if func is None: return decorator
        return decorator(func)
    #▄▄▄▄▄▄▄▄▄▄▄
    class Writer:
        #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
        def __init__(self, manager: "ClickHouseManager", table: str, func: Callable):
            self._manager, self._table, self._func = manager, table, func
            functools.update_wrapper(self, func)
        #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
        def __call__(self, instance, *args, **kwargs):
            try: return self._manager.write(self._table,
                  self._func(instance, *args, **kwargs))
            except Exception as EXC:
                Log.exception(EXC)
                return None, list()
        #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
        def __get__(self, instance, owner = None):
            if (instance is None): return self
            bound = functools.partial(self.__call__, instance)
            functools.update_wrapper(bound, self._func)
            async def flush(*args, **kwargs):
                try: return await self._manager.flush(
                    self._table, self._func, instance, *args, **kwargs)
                except Exception as EXC:
                    Log.exception(EXC)
                    return None, list()
            bound.flush = flush
            return bound

#███████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀       
try:
    Redis: RedisManager = RedisManager()
    Postgres: PostgresManager = PostgresManager()
    ClickHouse: ClickHouseManager = ClickHouseManager()
except Exception as EXC: Log.exception(EXC); sys.exit(1)

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
if __name__ == "__main__":

    @PostgresManager.on_table_base("test_postgres_listener")
    async def on_notify_1(src):
        query = "SELECT * FROM test_postgres_listener"
        table = DataFrame(map(dict, await src._conn.fetch(query)))
        print("table:"), print(table)

    @PostgresManager.on_table_base("accounts")
    async def on_notify_2(src):
        query = "SELECT * FROM accounts"
        table = DataFrame(map(dict, await src._conn.fetch(query)))
        print("table:"), print(table)

    async def main():
        await PostgresManager()

    EventLoop.run_until_complete(main())
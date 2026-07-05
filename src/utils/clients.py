#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
import sys, asyncio, json, asyncpg, functools, enum
from pandas import DataFrame, Timestamp, Timedelta
from typing import Any, Callable, ClassVar, Iterable
from logger import Log, LokiClient
from collections import deque
from redis.exceptions import ResponseError
from redis.asyncio import Redis as RedisClient
from clickhouse_driver import Client as ClickHouseClient
from base import DOCKER, DEFAULT_HOST, STARTUP_ERRORS, TZ
from base import Config, Credentials
from misc import Queue, Reporter

EventLoop: asyncio.AbstractEventLoop
EventLoop = asyncio.new_event_loop()
asyncio.set_event_loop(EventLoop)

#███████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
Log.remove(0)

args = {"backtrace": False, "colorize": True, "serialize": False, "level": "DEBUG"}
Log.add(**args, sink = sys.stdout, format = LokiClient.LOG_FORMAT["stdout"])
Log.info(f"Logging to stdout...")

if Config.LOG_TO_FILE:
    sink = str(Config.FOLDER_ROOT) + "/logs/" + LokiClient.LOGFILE_FORMAT
    Log.add(**args, sink = sink, format = LokiClient.LOG_FORMAT["file"])
    Log.info(f"Logging to file @ \"{Config.FOLDER_ROOT / "logs"}\"")

if Config.LOG_TO_LDB and ("grafana" in DOCKER):
    _creds = Credentials.get_for("loki")
    _creds.IP = f"{DEFAULT_HOST}:{DOCKER["grafana"]["ports"][-1]}"
    sink = LokiClient(url = LokiClient.URL_FORMAT.format(IP = _creds.IP),
            timeout = 10, labels = {"application": Config.SESSION_NAME})
    Log.add(**args, sink = sink, format = LokiClient.LOG_FORMAT["gui"])
    Log.info(f"Logging to Loki @ \"{_creds.IP}\"")

for error in STARTUP_ERRORS: Log.error(error)
Log.info(f"Master config:\n => {Config!r}")

#███████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄
class Postgres:
    #▄▄▄▄▄▄▄▄▄▄▄
    @classmethod
    def create(cls):
        creds: Credentials = Credentials.get_for("postgres")
        url = "postgresql://{USERNAME}:{PASSWORD}@{IP}/{DATABASE}"
        url = url.format(**creds._asdict())
        #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
        async def test():
            query = "SELECT 1"
            client = await asyncpg.create_pool(url)
            async with client.acquire() as conn:
                assert await conn.fetchval(query) == 1
            return client
        return EventLoop.run_until_complete(test())

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class PostgresManager:
    _QUERY = """
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
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __init__(self, client: asyncpg.Pool):
        self._to_listen = dict[str, Callable]()
        self._client: asyncpg.Pool = client
        self._ready = asyncio.Event()
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def wait(self):
        return await self._ready.wait()
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def _queue_handler(self, _conn, _pid, _channel, payload):
        self._queue.put_nowait(payload)
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def __call__(self, src: Any):
        conn = await self._client.acquire()
        self._queue = Queue(maxsize = src.maxlen)
        verbose = "Postgres listeners started:"
        for func in self._to_listen.values():
            await func(src, conn)
        for table, func in self._to_listen.items():
            await conn.execute(self._QUERY.format(table))
            await conn.add_listener(table, self._queue_handler)
            verbose += f"\n => \"{table}\": \"{func.__name__}\""
        Log.success(verbose)
        self._ready.set()
        while True:
            try:
                table = json.loads(await self._queue.get())["table"]
                if (func := self._to_listen.get(table)) is None: continue
                async with self._client.acquire() as conn: await func(src, conn)
            except Exception as EXC: Log.exception(EXC); return conn.close()
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def on_table(self, table: Table):
        def noop(func: Callable): return func
        def decorator(func: Callable):
            @functools.wraps(func)
            async def wrapped(*args, **kwargs):
                return await func(*args, **kwargs)
            self._to_listen[table.value] = wrapped
            return wrapped
        if not isinstance(table, self.Table): return noop
        else: return decorator

#███████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀  
#▄▄▄▄▄▄▄▄▄▄
class Redis:
    @classmethod
    def create(cls):
        creds: Credentials = Credentials.get_for("redis")
        host, port = creds.IP.split(":")
        #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
        async def test():
            tstr = Timestamp.now(TZ).strftime("%Y%m%d%H%M%S%f")
            args = {"decode_responses": True, "host": host, "port": int(port),
              "username": creds.USERNAME, "password": creds.PASSWORD, "db": 0}
            client = RedisClient(**args)
            query_1, query_2 = f"SET test {tstr}", f"GET test"
            assert (await client.execute_command(query_1) == "OK")
            assert (await client.execute_command(query_2) == tstr)
            return client
        return EventLoop.run_until_complete(test())

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class RedisGroup(enum.StrEnum):
    MONITOR: str = "$"
    STRATEGY: str = "$"

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class RedisManager:
    PRINT_LIMIT = 50
    GROUP: list[str] = {"MONITOR": "$", "STRATEGY": "$"}
    VERBOSE_ERROR = "\"{}\" XADD failed:\n => {}"
    VERBOSE_XADD = "[Q{}] \"{}\" XADD @ {} => {}"
    VERBOSE_CP = "Warning: Queue above {0:.0%}."
    STREAM_PREFIX: ClassVar[str] = "LTX"
    VERBOSE_QUEUE = "Queue is {:.0%} full!"
    CHECKPOINTS = {
        0.5: lambda value: Log.warning("Queue is {:.0%} full!".format(value)),
        0.8: lambda value: Log.warning("Queue is {:.0%} full!".upper().format(value)),
        0.95: lambda value: Log.critical("Queue is {:.0%} full!".upper().format(value)),
    }
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __init__(self, client: RedisClient):
        self._client: RedisClient = client
        self._reporter = Reporter(name = "RedisManager")
        self._ready = asyncio.Event()
        self._streams = set[str]()
        self._ncp = 0.0
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def scan(self, pattern: str = STREAM_PREFIX + "|*"):
        async for K in self._client.scan_iter(pattern): yield K
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def xreadgroup(self, src: Any, group: RedisGroup,
                        streams: dict[str, str], *args, **kwargs):
        consumer = src.__class__.__name__
        if src.name: consumer += "|" + src.name
        kwargs.setdefault("count", 100)
        kwargs.setdefault("block", 1000)
        return await self._client.xreadgroup(group.name,
                      consumer, streams, *args, **kwargs)
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def xack(self, group: RedisGroup, stream: str, message_id: str):
        return await self._client.xack(stream, group.name, message_id)
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def ensure_groups(self, streams: list[str]):
        for stream in streams:
            if stream in self._streams: continue
            mkstream = not await self._client.exists(stream)
            await self.xcreategroups(stream, mkstream = mkstream)
            self._streams.add(stream)
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def xcreategroups(self, stream: str, *, mkstream: bool = True):
        for group, value in RedisGroup.__members__.items():
            try: await self._client.xgroup_create(
                    stream, group, value, mkstream)
            except ResponseError as EXC:
                if "BUSY" not in str(EXC):
                    Log.exception(EXC); raise
            mkstream = False
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def add_streams(self, streams: list[str], src: Any):
        for stream in streams:
            stream = str.join("|", [self.STREAM_PREFIX, src.STREAM_PREFIX, stream])
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
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def on_stream(self, func: Callable = None):
        def decorator(func: Callable):
            @functools.wraps(func)
            async def wrapped(*args, **kwargs):
                gen = func(*args, **kwargs)
                results = deque()
                async for obj in gen:
                    results.append(obj)
                    if (obj is None): continue
                    payload: dict = obj.__dict__
                    self._queue.put_nowait(payload)
                return results
            return wrapped
        if func is None: return decorator
        return decorator(func)
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def __call__(self, src: Any):
        self._queue = Queue(maxsize = src.maxlen,
            checkpoints = self.CHECKPOINTS.copy())
        self._ready.set()
        while src.active:
            while (N := self._queue.qsize()) == 0:
                await asyncio.sleep(1e-6); continue
            else:
                try:
                    payload: dict = await self._queue.get()
                    suffix, time, payload = payload.values()
                    id = str(time)[: -3] + "-" + str(time)[-3 :]
                    stream = str.join("|", [self.STREAM_PREFIX, src.STREAM_PREFIX, suffix])
                    if stream not in self._streams: await self.add_streams([suffix], src)
                    assert (await self._client.xadd(stream, payload, id, src.maxlen))
                    if src.debug: Log.debug(self.VERBOSE_XADD.format(N, stream, id, payload))
                    self._reporter.add(suffix)
                except Exception as EXC: Log.error(
                    self.VERBOSE_ERROR.format(stream, payload), EXC)

#███████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀  
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class ClickHouse:
    @classmethod
    def create(cls):
        creds: Credentials = Credentials.get_for("clickhouse")
        host, port = creds.IP.split(":")
        #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
        async def test():
            query = "SELECT 1"
            args = {"user": creds.USERNAME, "password": creds.PASSWORD,
              "host": host, "port": int(port), "database": creds.DATABASE}
            client = ClickHouseClient(**args)
            assert client.execute(query) == [(1,)]
            return client
        return EventLoop.run_until_complete(test())

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class ClickHouseManager:
    VERBOSE_PUSH = "Pushed {0} rows to \"{1}\":\n => {2}"
    VERBOSE_ERROR = "Failed to write to \"{0}\":"
    INT_COLUMNS = {"volume", "dus"}
    FLOAT_COLUMNS = {"pa", "qa", "pb", "qb",
        "oa", "ha", "la", "ca", "ob", "hb", "lb", "cb"}
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    class Table(enum.StrEnum):
        TICKS = "history_ticks"
        CANDLES = "history_candles"
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __init__(self, client: ClickHouseClient):
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

    async def _flush(self, table: str, func: Callable, instance, *args, **kwargs):
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
            return self._ToTable(self, table.value, func)
        if func is None: return decorator
        return decorator(func)

    class _ToTable:
        def __init__(self, manager: "ClickHouseManager", table: str, func: Callable):
            self._manager, self._table, self._func = manager, table, func
            functools.update_wrapper(self, func)

        def __call__(self, instance, *args, **kwargs):
            try: return self._manager.write(self._table, self._func(instance, *args, **kwargs))
            except Exception as EXC: Log.exception(EXC); return None, list()

        def __get__(self, instance, owner = None):
            if instance is None: return self
            bound = functools.partial(self.__call__, instance)
            functools.update_wrapper(bound, self._func)
            async def flush(*args, **kwargs):
                try: return await self._manager._flush(
                    self._table, self._func, instance, *args, **kwargs)
                except Exception as EXC: Log.exception(EXC); return None, list()
            bound.flush = flush
            return bound

#███████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀       
try:
    Redis: RedisManager = RedisManager(Redis.create())
    ClickHouse: ClickHouseManager = ClickHouseManager(ClickHouse.create())
    Postgres: PostgresManager = PostgresManager(Postgres.create())
except Exception as EXC: Log.exception(EXC); sys.exit(1)

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
if __name__ == "__main__":

    @PostgresManager.on_table("test_postgres_listener")
    async def on_notify_1(conn: asyncpg.Connection):
        query = "SELECT * FROM test_postgres_listener"
        table = DataFrame(map(dict, await conn.fetch(query)))
        print("table:"), print(table)

    @PostgresManager.on_table("accounts")
    async def on_notify_2(conn: asyncpg.Connection):
        query = "SELECT * FROM accounts"
        table = DataFrame(map(dict, await conn.fetch(query)))
        print("table:"), print(table)

    async def main():
        await PostgresManager()

    EventLoop.run_until_complete(main())
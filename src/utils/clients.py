#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
import sys, asyncio, json, asyncpg, functools, enum
from pandas import DataFrame, Timestamp, Timedelta
from logger import Log, LokiClient
from typing import Any, Callable, ClassVar
from redis.asyncio import Redis as RedisClient
from redis.exceptions import ResponseError
from clickhouse_driver import Client as ClickHouseClient
from base import DOCKER, DEFAULT_HOST, STARTUP_ERRORS
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
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def on_table(self, table: str):
        def decorator(func: Callable):
            @functools.wraps(func)
            async def wrapped(*args, **kwargs):
                return await func(*args, **kwargs)
            self._to_listen[table] = wrapped
            return wrapped
        return decorator

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
            query = "SET test test"
            args = {"host": host, "port": int(port),
                "username": creds.USERNAME, "db": 0,
                "password": creds.PASSWORD,
                "decode_responses": True}
            client = RedisClient(**args)
            assert await client.execute_command(query) == "OK"
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
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
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
        return await self._client.xreadgroup(group.name,
                      consumer, streams, *args, **kwargs)
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
        freq = Timedelta(seconds = src.freq_report)
        next_at = Timestamp.now("UTC").ceil(freq)
        report = self._reporter.to_string(next_at)
        if report: Log.info(report)
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def on_stream(self, func: Callable = None):
        def decorator(func: Callable):
            @functools.wraps(func)
            async def wrapped(*args, **kwargs):
                obj = await func(*args, **kwargs)
                if (obj is None): return
                payload: dict = obj.__dict__
                self._queue.put_nowait(payload)
                return obj
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
    QUERY_INSERT = "INSERT INTO {0} ({1}) VALUES"
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __init__(self, client: ClickHouseClient):
        self._client: ClickHouseClient = client
        self._ready = set[str]()

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def write(self, series: str, df: DataFrame):
        if (df := df.copy()).empty: return 0
        if (df.index.names != (None,)) and any(df.index.names): df = df.reset_index()
        if ("time" in df.columns): df["time"] = df["time"].map(Timestamp.to_pydatetime)
        rows = df.to_records(index = False).tolist()
        query = self.QUERY_INSERT.format(series, str.join(", ", df.columns))
        self._client.execute(query, rows, types_check = True)
        return len(rows)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def to_series(self, func: Callable, series: str):
        def decorator(func: Callable):
            @functools.wraps(func)
            def wrapped(*args, **kwargs):
                async def write():
                    try:
                        df: DataFrame = func(*args, **kwargs)
                        assert (n_rows := self.write(series, df)) > 0
                        df = df.groupby(df.index.names).size().reset_index(drop = True)
                        Log.info(self.VERBOSE_PUSH.format(n_rows, series, df.to_string()))
                    except AssertionError: Log.error(f"No rows written to \"{series}\"")
                    except Exception as EXC:
                        Log.exception(self.VERBOSE_ERROR.format(series), EXC)
                asyncio.create_task(write())
            return wrapped
        return decorator(func)

#███████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀       
try:
    Redis: RedisManager = RedisManager(Redis.create())
    ClickHouse: ClickHouseManager = ClickHouseManager(ClickHouse.create())
    Postgres: PostgresManager = PostgresManager(Postgres.create())
except Exception as EXC:
    Log.exception(EXC)
    sys.exit(1)

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
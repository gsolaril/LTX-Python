#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
import os, sys, asyncio, json, asyncpg, functools
from dataclasses import dataclass, field
from clickhouse_driver import Client as ClickHouseClient
from redis.asyncio import Redis as RedisClient
from pandas import DataFrame, Timestamp, Timedelta
from logger import Log, LokiClient
from typing import Any, Callable, ClassVar
from typing import Protocol, runtime_checkable
from base import AUTH, DOCKER, DEFAULT_HOST
from base import Config, Credentials, Vault
from base import STARTUP_ERRORS

EventLoop: asyncio.AbstractEventLoop
EventLoop = asyncio.new_event_loop()
asyncio.set_event_loop(EventLoop)
#▄▄▄▄▄▄▄▄▄▄▄
@dataclass#█▄▄▄
class BaseOwner:
    maxlen: int; active: bool; freq_report: int
    STREAM_PREFIX: ClassVar[str] = ...

#███████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
Log.remove(0)

args = {"backtrace": False, "colorize": True, "serialize": False,
            "level": "DEBUG" if Config.DEBUG_MODE else "INFO"}

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
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def __call__(self, src: BaseOwner):
        self._queue = asyncio.Queue()
        conn = await self._client.acquire()
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

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class RedisManager:
    #▄▄▄▄▄▄▄▄▄▄▄
    class Report:
        #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
        def __init__(self, name: str):
            self._name = name
            self._start_at = Timestamp.now("UTC")
            self._batch_at = self._entry_at = None
            self._last_count = self._mean_count = 0
            self._batches = self._total_count = 0
        #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
        def _incr(self, count: int = 1):
            if (self._batch_at is None):
                self._batch_at = Timestamp.now("UTC")
            self._last_count = self._last_count + count
            self._total_count = self._total_count + count
            self._entry_at = Timestamp.now("UTC")
        #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
        def _close_batch(self):
            self._batches, self._last_count = self._batches + 1, 0
            self._mean_count = self._total_count / self._batches
            self._batch_at = Timestamp.now("UTC")
        #▄▄▄▄▄▄▄▄▄▄
        @property#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
        def __dict__(self): return {
            "name": self._name, "start_at": self._start_at,
            "batch_at": self._batch_at, "last_count": self._last_count, 
            "entry_at": self._entry_at, "mean_count": self._mean_count,
            "batches": self._batches, "total_count": self._total_count}
            
    PRINT_LIMIT = 50
    VERBOSE_ERROR = "\"{}\" XADD failed:\n => {}"
    VERBOSE_XADD = "[Q{}] \"{}\" XADD @ {} => {}"
    VERBOSE_CP = "Warning: Queue above {0:.0%}."
    STREAM_PREFIX: ClassVar[str] = "LTX"
    QUEUE_CHECKPOINTS = {0.5: Log.debug,
      0.8: Log.warning, 0.95: Log.error}
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __init__(self, client: RedisClient):
        self._client: RedisClient = client
        self._reports = dict[str, self.Report]()
        self._start_at = Timestamp.now("UTC")
        self._ready = asyncio.Event()
        self._ncp = 0.0
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def wait(self):
        return await self._ready.wait()
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def report(self, src: BaseOwner):
        reports = list()
        for report in self._reports.values():
            reports.append(report.__dict__.copy())
            report._close_batch()
        df = DataFrame(reports)
        if df.dropna().empty: return
        df = df.set_index("name").rename_axis(None)
        freq_report = Timedelta(seconds = src.freq_report)
        ts_next = Timestamp.now("UTC").ceil(freq_report)
        header = f"[Next report @ {ts_next:%H:%M}]"
        df = df.rename_axis(header, axis = "columns")
        df["mean_count"] = df["mean_count"].astype(int)
        if (df.shape[0] <= self.PRINT_LIMIT): df = df.sort_index()
        else: df = df.sort_values("total_count", ascending = False)
        df["start_at"] = df["start_at"].dt.strftime("%Y/%m/%d %H:%M")
        df["batch_at"] = df["batch_at"].dt.strftime("%H:%M:%S")
        df["entry_at"] = df["entry_at"].dt.strftime("%H:%M:%S.%f").str[: -3]
        ts_start = self._start_at.strftime("%Y/%m/%d %H:%M:%S")
        verbose = f"Redis manager ongoing since \"{ts_start}\":\n"
        verbose += df.to_string(max_rows = self.PRINT_LIMIT)
        Log.info(verbose)
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
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
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def __call__(self, src: BaseOwner):
        self._queue = asyncio.Queue(src.maxlen)
        self._ready.set()
        while src.active:
            while (N := self._queue.qsize()) == 0:
                await asyncio.sleep(1e-6); continue
            else:
                try:
                    ncp = int(20 * N / src.maxlen + 1) / 20
                    payload: dict = await self._queue.get()
                    suffix, time, payload = payload.values()
                    id = str(time)[: -3] + "-" + str(time)[-3 :]
                    prefix = self.STREAM_PREFIX + "|" + src.STREAM_PREFIX + "|"
                    if not await self._client.exists(stream := prefix + suffix):
                        await self._client.xgroup_create(stream, stream, "$", mkstream = True)
                    assert (await self._client.xadd(stream, payload, id, src.maxlen))
                    if Config.DEBUG_MODE:
                        Log.debug(self.VERBOSE_XADD.format(N, stream, id, payload))
                    if (ncp != self._ncp):
                        if ncp in self.QUEUE_CHECKPOINTS:
                            verbose = self.VERBOSE_CP.format(ncp)
                            self.QUEUE_CHECKPOINTS[ncp](verbose)
                            self._ncp = ncp
                    if (suffix not in self._reports):
                        self._reports[suffix] = self.Report(suffix)
                    self._reports[suffix]._incr()
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
 
#███████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀       
try:
    Redis: RedisManager = RedisManager(Redis.create())
    ClickHouse: ClickHouseClient = ClickHouse.create()
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
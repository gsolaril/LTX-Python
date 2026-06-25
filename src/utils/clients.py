#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
import os, sys, asyncio, json, asyncpg, functools
from dataclasses import dataclass, field
from clickhouse_driver import Client as ClickHouseClient
from redis.asyncio import Redis as RedisClient
from pandas import DataFrame, Timestamp, Timedelta
from logger import Log, LokiClient
from enum import StrEnum
from typing import Any, Callable, ClassVar
from src.models import Queue, Reporter, BaseAgent
from base import AUTH, DOCKER, DEFAULT_HOST
from base import Config, Credentials, Vault
from base import STARTUP_ERRORS

EventLoop: asyncio.AbstractEventLoop
EventLoop = asyncio.new_event_loop()
asyncio.set_event_loop(EventLoop)

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
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def __call__(self, src: Any):
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

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class RedisGroup(StrEnum):
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
    CHECKPOINTS = {
        0.5: lambda value: Log.warning("Queue is {value:.0%} full!"),
        0.8: lambda value: Log.warning("QUEUE IS {value:.0%} FULL!"),
        0.95: lambda value: Log.critical("QUEUE IS {value:.0%} FULL!!!"),
    }
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __init__(self, client: RedisClient):
        self._client: RedisClient = client
        self._reporter = Reporter(name = "RedisManager")
        self._ready = asyncio.Event()
        self._ncp = 0.0
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def scan(self, pattern: str = STREAM_PREFIX + "|*"):
        async for K in self._client.scan_iter(pattern): yield K
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def xreadgroup(self, src: BaseAgent, group: RedisGroup,
                        streams: dict[str, str], *args, **kwargs):
        consumer = src.__class__.__name__
        name: str = getattr(src, "name", None)
        if name: consumer = consumer + "|" + name
        return await self._client.xreadgroup(group.name,
                    consumer, streams, *args, **kwargs)
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def xcreategroups(self, stream: str):
        for group in RedisGroup.__members__:
            await self._client.xgroup_create(stream, group, 
                  RedisGroup[group].value, mkstream = True)
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def wait(self):
        return await self._ready.wait()
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def report(self, src: BaseAgent):
        freq_report = getattr(src, "freq_report", 600)
        freq_report = Timedelta(seconds = freq_report)
        next_at = Timestamp.now("UTC").ceil(freq_report)
        Log.info(self._reporter.to_string(next_at))
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
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def __call__(self, src: BaseAgent):
        self._queue = Queue(maxsize = src.maxlen,
            checkpoints = self.CHECKPOINTS.copy())
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
                    stream = str.join("|", [self.STREAM_PREFIX, src.STREAM_PREFIX, suffix])
                    if not await self._client.exists(stream): await self.xcreategroups(stream)
                    assert (await self._client.xadd(stream, payload, id, src.maxlen))
                    if Config.DEBUG_MODE:
                        Log.debug(self.VERBOSE_XADD.format(N, stream, id, payload))
                    if (ncp != self._ncp):
                        if ncp in self.QUEUE_CHECKPOINTS:
                            verbose = self.VERBOSE_CP.format(ncp)
                            self.QUEUE_CHECKPOINTS[ncp](verbose)
                            self._ncp = ncp
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
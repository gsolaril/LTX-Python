#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
import os, sys, asyncio, json, asyncpg
from functools import wraps
from logger import Log, LokiClient
from typing import Callable, Awaitable, ClassVar
from clickhouse_connect import get_client as get_clickhouse_client
from clickhouse_connect.driver.client import Client as ClickHouseClient
from redis.asyncio import Redis as RedisClient
from base import AUTH, DOCKER, DEFAULT_HOST
from base import Config, Credentials, Vault
from base import STARTUP_ERRORS

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
    _DEFAULT_PORT = DOCKER["grafana"]["ports"][-1]
    _defs = {"type": "loki", "ip": f"{DEFAULT_HOST}:{_DEFAULT_PORT}"}
    _defs["username"] = _defs["database"] = _defs["type"]
    _defs["password"] = "..."

    _credentials = Credentials.from_kv(src = "Loki", defs = _defs,
                    data = AUTH.pop("DB_LOG", dict()))
    sink = LokiClient(url = LokiClient.URL_FORMAT.format(IP = _credentials.IP),
                    timeout = 10, labels = {"application": Config.SESSION_NAME})
    Log.add(**args, sink = sink, format = LokiClient.LOG_FORMAT["gui"])
    Log.info(f"Logging to Loki @ \"{_credentials.IP}\"")

for error in STARTUP_ERRORS: Log.error(error)
Log.info(f"Master config:\n => {Config!r}")

#███████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class CreateClient:
    _defs: dict = ...
    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄
    def _def_password(cls):
        kv: dict = Vault.secrets.kv.v2.read_secret_version(mount_point = "infra", 
                path = "local", raise_on_deleted_version = True)["data"]["data"]
        return kv[cls._defs["type"]]

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class CreateDBORM(CreateClient):
    _def_type: str = "postgres"
    _defs = dict.fromkeys(["type", "database", "username"], _def_type)
    _defs["ip"] = f"{DEFAULT_HOST}:{DOCKER[_def_type]["ports"][0]}"
    ERROR: ClassVar[str] = "DB_ORM (Postgres) connection test failed"
    URL_FORMAT: ClassVar[str] = "postgresql://{USERNAME}:{PASSWORD}@{IP}/{DATABASE}"
    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄
    def from_postgres(cls) -> str:
        defs = cls._defs.copy()
        defs["password"] = cls._def_password()
        _credentials = Credentials.from_kv(src = "DB_ORM",
          defs = defs, data = AUTH.get("DB_ORM", dict()))
        return cls.URL_FORMAT.format(**_credentials._asdict())

async def init_db_orm() -> asyncpg.Pool:
    global DB_ORM
    if DB_ORM is None:
        DB_ORM = await asyncpg.create_pool(DB_ORM_DSN)
        async with DB_ORM.acquire() as conn:
            assert await conn.fetchval("SELECT 1") == 1, CreateDBORM.ERROR
    return DB_ORM
        
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class CreateDBCCH(CreateClient):
    _def_type: str = "redis"
    _defs = dict.fromkeys(["type", "database", "username"], _def_type)
    _defs["ip"] = f"{DEFAULT_HOST}:{DOCKER[_def_type]["ports"][0]}"
    ERROR: ClassVar[str] = "DB_CCH (Redis) connection test failed"
    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def from_redis(cls) -> RedisClient:
        defs = cls._defs.copy()
        defs["password"] = cls._def_password()
        _credentials = Credentials.from_kv(src = "DB_CCH",
          defs = defs, data = AUTH.get("DB_CCH", dict()))
        host, port = _credentials.IP.split(":")
        args = {"host": host, "port": int(port), "db": 0, "max_connections": 128,
            "username": _credentials.USERNAME, "password": _credentials.PASSWORD}
        #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
        async def test(**args):
            client = RedisClient(**args)
            OK = await client.ping()
            assert OK, cls.ERROR
            await client.aclose()
            return RedisClient(**args)

        return asyncio.run(test(**args))

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class CreateDBTSS(CreateClient):
    _def_type: str = "clickhouse"
    _defs = dict.fromkeys(["type", "database", "username"], _def_type)
    _defs["ip"] = f"{DEFAULT_HOST}:{DOCKER[_def_type]["ports"][0]}"
    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def from_clickhouse(cls) -> ClickHouseClient:
        defs = cls._defs.copy()
        defs["password"] = cls._def_password()
        _credentials = Credentials.from_kv(src = "DB_TSS",
          defs = defs, data = AUTH.get("DB_TSS", dict()))
        host, port = _credentials.IP.split(":")
        client = get_clickhouse_client(
            username = _credentials.USERNAME, password = _credentials.PASSWORD,
            database = _credentials.DATABASE, host = host, port = int(port))
        assert client.ping(), "DB_TSS (ClickHouse) connection test failed"
        return client

try:
    DB_ORM_method = AUTH.pop("DB_ORM", {}).pop("type", CreateDBORM._def_type)
    DB_ORM_DSN: str = getattr(CreateDBORM, "from_" + DB_ORM_method)()
    DB_ORM: asyncpg.Pool | None = None
    DB_CCH_method = AUTH.pop("DB_CCH", {}).pop("type", CreateDBCCH._def_type)
    DB_CCH: RedisClient = getattr(CreateDBCCH, "from_" + DB_CCH_method)()
    DB_TSS_method = AUTH.pop("DB_TSS", {}).pop("type", CreateDBTSS._def_type)
    DB_TSS: ClickHouseClient = getattr(CreateDBTSS, "from_" + DB_TSS_method)()
except Exception as EXC:
    Log.exception(EXC)
    sys.exit(1)

#███████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
_LISTENERS: list[Callable[[], Awaitable[None]]] = list()

_QUERY_NOTIFY = """\
CREATE OR REPLACE FUNCTION notify_{channel}() RETURNS trigger AS $$
BEGIN
    PERFORM pg_notify('{channel}', json_build_object(
        'operation', TG_OP, 'table', TG_TABLE_NAME,
        'time', CURRENT_TIMESTAMP)::text);
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;"""

_QUERY_TRIGGER = """\
DROP TRIGGER IF EXISTS {channel}_trigger ON {table};
CREATE TRIGGER {channel}_trigger
    AFTER UPDATE ON {table}
    FOR EACH STATEMENT EXECUTE FUNCTION notify_{channel}();"""

async def _ensure_notify_trigger(table: str, channel: str) -> None:
    await init_db_orm()
    async with DB_ORM.acquire() as conn:
        await conn.execute(_QUERY_NOTIFY.format(channel = channel))
        await conn.execute(_QUERY_TRIGGER.format(channel = channel, table = table))

async def _run_listener(table: str, channel: str, func: Callable) -> None:
    await _ensure_notify_trigger(table, channel)
    conn = await asyncpg.connect(DB_ORM_DSN)
    queue: asyncio.Queue[str] = asyncio.Queue()

    def handler(_conn, _pid, _channel, payload):
        queue.put_nowait(payload)

    await conn.add_listener(channel, handler)
    Log.info(f"db_listen: listening on \"{channel}\" (table \"{table}\")")
    try:
        while True:
            payload = await queue.get()
            try:
                await func(json.loads(payload))
            except Exception as EXC:
                Log.exception(EXC)
    finally:
        await conn.remove_listener(channel, handler)
        await conn.close()

def db_listen(table: str, *, channel: str = None):
    """Decorator: run ``func(payload)`` as an asyncio task on Postgres UPDATE."""
    channel = channel or table

    def decorator(func: Callable):
        @wraps(func)
        async def wrapped(*args, **kwargs):
            return await func(*args, **kwargs)

        async def listener():
            await _run_listener(table, channel, wrapped)

        _LISTENERS.append(listener)
        return wrapped
    return decorator

def start_db_listeners() -> list[asyncio.Task]:
    """Spawn background tasks for every ``@db_listen``-decorated function."""
    loop = asyncio.get_running_loop()
    return [loop.create_task(listener()) for listener in _LISTENERS]

#███████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
if (__name__ == "__main__"):

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    @db_listen(table = "test_postgres_listener")
    async def test(payload: dict):
        print(payload)

    async def main():
        await init_db_orm()
        start_db_listeners()
        await asyncio.Event().wait()

    asyncio.run(main())

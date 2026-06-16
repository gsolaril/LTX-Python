#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
import os, sys, json, asyncio
from typing import Any, Callable
from logger import Log, LokiClient
from clickhouse_connect import get_client as get_clickhouse_client
from clickhouse_connect.driver.client import Client as ClickHouseClient
from sqlalchemy import create_engine, Engine, TextClause
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
    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def from_postgres(cls) -> Engine:
        defs = cls._defs.copy()
        defs["password"] = cls._def_password()
        _credentials = Credentials.from_kv(src = "DB_ORM",
          defs = defs, data = AUTH.get("DB_ORM", dict()))
        URL = "postgresql://{USERNAME}:{PASSWORD}@{IP}/{DATABASE}"
        client = create_engine(URL.format(**_credentials._asdict()),
            isolation_level = "AUTOCOMMIT")
        with client.connect() as conn:
            result = conn.execute(TextClause("SELECT 1"))
            is_scalar = hasattr(result, "scalar")
            fetched = result.scalar() if is_scalar else result.fetchone()[0]
            assert (fetched == 1), "DB_ORM (Postgres) connection test failed"
        return client

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class CreateDBCCH(CreateClient):
    _def_type: str = "redis"
    _defs = dict.fromkeys(["type", "database", "username"], _def_type)
    _defs["ip"] = f"{DEFAULT_HOST}:{DOCKER[_def_type]["ports"][0]}"
    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def from_redis(cls) -> RedisClient:
        defs = cls._defs.copy()
        defs["password"] = cls._def_password()
        _credentials = Credentials.from_kv(src = "DB_CCH",
          defs = defs, data = AUTH.get("DB_CCH", dict()))
        client = RedisClient(host = _credentials.IP.split(":")[0],
            port = int(_credentials.IP.split(":")[1]), db = 0,
            username = _credentials.USERNAME, password = _credentials.PASSWORD)
        assert asyncio.run(client.ping()), "DB_CCH (Redis) connection test failed"
        return client

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
    DB_ORM: Engine = getattr(CreateDBORM, "from_" + DB_ORM_method)()
    DB_CCH_method = AUTH.pop("DB_CCH", {}).pop("type", CreateDBCCH._def_type)
    DB_CCH: RedisClient = getattr(CreateDBCCH, "from_" + DB_CCH_method)()
    DB_TSS_method = AUTH.pop("DB_TSS", {}).pop("type", CreateDBTSS._def_type)
    DB_TSS: ClickHouseClient = getattr(CreateDBTSS, "from_" + DB_TSS_method)()
except Exception as EXC:
    Log.exception(EXC)
    sys.exit(1)

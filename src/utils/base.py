#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
import os, sys, json, subprocess
from getpass import getpass, getuser
from configparser import ConfigParser
from hvac import Client as VaultClient
from pathlib import Path
from subprocess import Popen
from typing import NamedTuple
from .logger import Log, LokiClient

#███████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
_FOLDER_UTILS = Path(__file__).parent
_FOLDER_ROOT = _FOLDER_UTILS.parent.parent
_FOLDER_LOG = _FOLDER_ROOT / "logs"
_FOLDER_SRC = _FOLDER_ROOT / "src"
STARTUP_ERRORS = list()

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class Config(NamedTuple):
    USER: str = getuser()
    SESSION_NAME: str = __name__
    LOG_TO_FILE: bool = True
    LOG_TO_LDB: bool = False
    FOLDER_ROOT: Path = _FOLDER_ROOT
    TIMEZONE: str = "UTC"
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __repr__(self): return str.join("\n => ",
        [f"{K}: {V}" for K, V in self._asdict().items()])

_PATH_CONFIG = _FOLDER_ROOT / "config.json"
try:
    with open(_PATH_CONFIG, "r") as file:
        Config = Config(**json.load(file))
except Exception as EXC:
    Config = Config()
    verbose = f"Error reading \"{_PATH_CONFIG}\":"
    verbose += f"\n => ({EXC.__class__.__name__}) {EXC}"
    verbose += f"\n => Will use default config."
    STARTUP_ERRORS.append(verbose)

AUTH = ConfigParser()
AUTH.read(_FOLDER_ROOT / "auth.ini")
AUTH = {key: dict[str, str](value)\
    for key, value in AUTH.items()}
try:
    if not os.path.exists(_FOLDER_ROOT / "auth.ini"):
        error = f"\"auth.ini\" not found in \"{_FOLDER_ROOT}\""
        raise FileNotFoundError(error)
except Exception as EXC:
    verbose = f"Error reading \"{_FOLDER_ROOT / "auth.ini"}\":"
    verbose += f"\n => ({EXC.__class__.__name__}) {EXC}"
    verbose += f"\n => Will use default auth, including Vault password."
    STARTUP_ERRORS.append(verbose)

TZ = Config.TIMEZONE

_proc = Popen(["docker", "ps", "--format", "{{json .}}"],
        stdout = subprocess.PIPE, stderr = subprocess.PIPE,
        universal_newlines = True)
_out, _err = _proc.communicate()
if (_proc.returncode != 0): raise RuntimeError(
    f"Docker snapshot failed: \"{_err.strip()}\"")

DOCKER = dict()
for line in _out.strip().split("\n"):
    if not line.strip(): continue
    container: dict = json.loads(line)
    name = container.pop("Names")
    state = container.get("State")
    if (state != "running"):
        if (name == "vault"): raise RuntimeError(f"Vault state: \"{state}\"!")
        STARTUP_ERRORS.append(f"Warning: \"{name}\" state: \"{state}\"...")
        
    DOCKER[name] = {"state": state, "ports": []}
    for port in str.split(container["Ports"], ","):
        port = port.split("->")[-1].split("/")[0]
        DOCKER[name]["ports"].append(int(port))

#███████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class CredentialsV1(NamedTuple):
    USERNAME: str; PASSWORD: str
    IP: str
    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def from_kv(cls, data: dict, defs: dict, src: str):
        return cls(*cls._args(data, defs, src))
    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def _args(cls, data: dict, defs: dict, src: str):
        verbose = "Type {src} {attr} for \"{ref}\": "
        if not (username := data.pop("username", None)):
            if not (username := defs.get("username", None)):
                username = input(verbose.format(
                    src = src, attr = "username", ref = "..."))
        if not (password := data.pop("password", None)):
            if not (password := defs.get("password", None)):
                password = getpass(verbose.format(
                    src = src, attr = "password", ref = username))
        if not (ip := data.pop("ip", None)):
            if not (ip := defs.get("ip", None)):
                ip = input(verbose.format(
                    src = src, attr = "IP", ref = username))
        return username, password, ip
        
DEFAULT_HOST, _DEFAULT_PORT = "localhost", DOCKER["vault"]["ports"][0]
_defs = {"username": Config.USER, "ip": f"{DEFAULT_HOST}:{_DEFAULT_PORT}"}
_credentials = CredentialsV1.from_kv(src = "Vault",
        data = AUTH.get("VAULT", {}), defs = _defs)

Vault = VaultClient(url = "http://" + _credentials.IP)
Vault.auth.userpass.login(username = _credentials.USERNAME,
                          password = _credentials.PASSWORD)

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class Credentials(NamedTuple):
    USERNAME: str; PASSWORD: str; IP: str; DATABASE: str
    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def _from_kv(cls, data: dict, defs: dict, src: str):
        return cls(*cls._args(data, defs, src))
    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def _args(cls, data: dict, defs: dict, src: str):
        verbose = "Type {src} {attr} for \"{ref}\": "
        username, password, ip = CredentialsV1._args(data, defs, src)
        if not (database := data.pop("database", None)):
            if not (database := defs.get("database", None)):
                database = input(verbose.format(
                    src = src, attr = "database", ref = username))
        return username, password, ip, database
    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄
    def get_for(cls, name: str):
        kv = Vault.secrets.kv.v2.read_secret_version(path = "local",
                raise_on_deleted_version = True, mount_point = "infra")
        auth_dict = AUTH.get(name.upper(), dict[str, str]())
        defaults = dict.fromkeys(["type", "database", "username"], name.lower())
        defaults["ip"] = f"{DEFAULT_HOST}:{DOCKER[name.lower()]['ports'][0]}"
        defaults["password"] = kv["data"]["data"][name.lower()]
        return Credentials._from_kv(src = name, defs = defaults, data = auth_dict)

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
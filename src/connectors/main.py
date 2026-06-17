#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
import os, sys, asyncio
from argparse import ArgumentParser
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
_SRC = _ROOT / "src"
for _path in (_ROOT, _SRC, _SRC / "models", _SRC / "utils"):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

from src.connectors.base import Connector
from src.connectors.ws import *
from src.utils import Log
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
#███████████████████████████████████████████████████████████████████████████████████████████
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
if (__name__ == "__main__"):
    parser = ArgumentParser()
    parser.add_argument("connector", nargs = "?", type = str)
    name: str = getattr(parser.parse_args(), "connector", None)
    assert isinstance(name, str), "Connector name is required"
    connectors = {
        "binanceusdm": DataBinanceUsdm,
        "binancecoin": DataBinanceCoin,
        "binancespot": DataBinanceSpot
    }
    connector: Connector = connectors.get(name.lower(), None)
    if connector is None: parser.error(f"\"{name}\" not found")
    try: asyncio.run(connector().start())
    except Exception as EXC: Log.exception(EXC)
    finally: Log.success(f"Exiting \"{name}\"...")
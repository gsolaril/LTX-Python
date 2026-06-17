#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
import os, sys, json, requests
from pathlib import Path
from pandas import Series, DataFrame
from pandas import Timestamp, Timedelta
from pandas import concat, to_datetime

_ROOT = Path(__file__).resolve().parents[1]
_SRC = _ROOT / "src"
for _path in (_ROOT, _SRC, _SRC / "utils"):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

from src.utils import *

#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀

TABLE = "symbol_specs"
query_create_symbol_specs = TextClause(f"""
    DROP TABLE IF EXISTS {TABLE};
    CREATE TABLE IF NOT EXISTS {TABLE} (
        id TEXT PRIMARY KEY,
        venue TEXT NOT NULL,
        symbol TEXT NOT NULL,
        quote TEXT NOT NULL,
        base TEXT NOT NULL,
        min_stops_diff FLOAT NOT NULL,
        min_price_diff FLOAT NOT NULL,
        min_order_size FLOAT NOT NULL,
        expiration TIMESTAMP NOT NULL
    ); """)

columns = dict(id = "id", venue = "venue", symbol = "symbol",
          baseAsset = "base", quoteAsset = "quote", filters = "filters",
      ) # category = "category")

data = concat(axis = "index", names = ["venue", "to_drop"], objs = {
    "BinanceSpot": DataFrame(requests.get(
        "https://api.binance.com/api/v3/exchangeInfo").json()["symbols"]),
    "BinanceUsdm": DataFrame(requests.get(
        "https://fapi.binance.com/fapi/v1/exchangeInfo").json()["symbols"]),
    "BinanceCoin": DataFrame(requests.get(
        "https://dapi.binance.com/dapi/v1/exchangeInfo").json()["symbols"])})

data = data.reset_index()#.sample(20)
id_format = "{0[venue]} {0[symbol]}".format
data["id"] = data.agg(id_format, axis = "columns")
# print(data.columns) # to see raw column list
data = data[[*columns]].rename(columns = columns)
data = data.set_index("id").sort_index()

keep_filters = {
    ("min_stops_diff_above", "minTrailingAboveDelta"): "TRAILING_DELTA",
    ("min_stops_diff_below", "minTrailingBelowDelta"): "TRAILING_DELTA",
    ("min_price_diff", "tickSize"): "PRICE_FILTER",
    ("min_order_size", "stepSize"): "LOT_SIZE",
}

filters = data.pop("filters").apply(Series).stack()
filters = filters.reset_index(-1, drop = True).dropna()
allowed_filters = set(keep_filters.values())
allowed_lambda = lambda x: x["filterType"] in allowed_filters
filters = filters.loc[filters.map(allowed_lambda)].apply(Series)
filters = filters.apply(Series).set_index("filterType", append = True)
filters = filters.stack().rename_axis(["id", "filter", "field"])
filters = filters.dropna().reset_index(["filter", "field"])
filters["field"] = filters["filter"] + " " + filters["field"]
filters = filters.set_index("field", append = True)[0]
filters = filters.unstack("field")

for ((key_mine, key_exch), filter) in keep_filters.items():
    data[key_mine] = filters[filter + " " + key_exch]

columns_stops = ["min_stops_diff_above", "min_stops_diff_below"]
data["min_stops_diff"] = data[columns_stops].max(axis = "columns")

data["expiration"] = None
regex = r"_([0-9][0-9][0-9][0-9]+)$"
expirable = data.loc[data["symbol"].str.contains(pat = regex, regex = True)]
expirable["expiration"] = expirable["symbol"].str.extract(regex)
expirable["expiration"] = to_datetime(expirable["expiration"], format = "%y%m%d")
data.loc[expirable.index, "expiration"] = expirable["expiration"]

with DB_ORM.connect() as conn:
    conn.execute(query_create_symbol_specs); conn.commit()
    data.to_sql(TABLE, conn, if_exists = "replace", index = True)

#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
TABLE = "conns_data_config"
query_create_conns_data_config = TextClause(f"""
    DROP TABLE IF EXISTS {TABLE};
    CREATE TABLE IF NOT EXISTS {TABLE} (
        name TEXT PRIMARY KEY,
        url_ws TEXT NOT NULL,
        url_api TEXT NOT NULL,
        active BOOLEAN NOT NULL,
        maxlen INTEGER NOT NULL,
        last_written TIMESTAMP NOT NULL,
        last_updated TIMESTAMP NOT NULL,
        symbols JSON NOT NULL
    );""")


data = DataFrame([
    {
        "name": "BinanceUsdm", "url_ws": "wss://fstream.binance.com",
        "url_api": "https://fapi.binance.com/fapi/v1", "active": True,
        "maxlen": 10000, "last_written": None, "last_updated": None,
        "symbols": {"BTCUSD": True, "ETHUSD": True, "SOLUSD": True}},
    {
        "name": "BinanceCoin", "url_ws": "wss://dstream.binance.com",
        "url_api": "https://dapi.binance.com/dapi/v1", "active": True,
        "maxlen": 10000, "last_written": None, "last_updated": None,
        "symbols": {"BTCUSD": True, "ETHUSD": True, "SOLUSD": True}},
    {
        "name": "BinanceSpot", "url_ws": "wss://stream.binance.com:9443",
        "url_api": "https://api.binance.com/api/v3", "active": True,
        "maxlen": 10000, "last_written": None, "last_updated": None,
        "symbols": {"BTCUSDT": True, "ETHUSDT": True, "SOLUSDT": True}},
]).set_index("name")

_now = Timestamp.now("UTC")
data["last_written"] = data["last_written"].fillna(_now)
data["last_updated"] = data["last_updated"].fillna(_now)
data["symbols"] = data["symbols"].map(
    lambda value: json.dumps(value) if isinstance(value, dict) else value)

with DB_ORM.connect() as conn:
    conn.execute(query_create_conns_data_config); conn.commit()
    data.to_sql(TABLE, conn, if_exists = "replace", index = True)

#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
import sys, json, requests
from pathlib import Path
from getpass import getpass
from configparser import ConfigParser
from pandas import Series, DataFrame
from pandas import Timestamp, Timedelta
from pandas import concat, to_datetime
from sqlalchemy import create_engine, TextClause
from clickhouse_connect import get_client

#███████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
if False:

    URL = "postgresql://postgres:{password}@localhost:5432/postgres"
    url = URL.format(password = getpass("Enter Postgres password... "))
    DB_ORM = create_engine(url = url, isolation_level = "AUTOCOMMIT")

    TABLE = "connectors"
    query_create_connectors = TextClause(f"""
        DROP TABLE IF EXISTS {TABLE};
        CREATE TABLE IF NOT EXISTS {TABLE} (
            name TEXT PRIMARY KEY,
            url_ws TEXT NOT NULL,
            url_api TEXT NOT NULL,
            active BOOLEAN NOT NULL,
            debug BOOLEAN NOT NULL,
            maxlen INTEGER NOT NULL,
            freq_report INTEGER NOT NULL,
            last_written TIMESTAMP NOT NULL,
            last_updated TIMESTAMP NOT NULL,
            symbols JSON NOT NULL
        );""")


    data = DataFrame([{
        "name": "BinanceUsdm", "url": "wss://fstream.binance.com",
        "active": True, "debug": False, "maxlen": 10000, "freq_report": 300, "last_written": None,
        "last_updated": None, "symbols": {"BTCUSDT": True, "ETHUSDT": True, "SOLUSDT": True}
        }, {
        "name": "BinanceCoin", "url": "wss://dstream.binance.com",
        "active": True, "debug": False, "maxlen": 10000, "freq_report": 300, "last_written": None,
        "last_updated": None, "symbols": {"BTCUSD_PERP": True, "ETHUSD_PERP": True, "SOLUSD_PERP": True}
        }, {
        "name": "BinanceSpot", "url": "wss://stream.binance.com:9443",
        "active": True, "debug": False, "maxlen": 10000, "freq_report": 300, "last_written": None,
        "last_updated": None, "symbols": {"BTCUSD": True, "ETHUSD": True, "SOLUSD": True}
        }]).set_index("name")

    _now = Timestamp.now("UTC")
    data["last_written"] = data["last_written"].fillna(_now)
    data["last_updated"] = data["last_updated"].fillna(_now)
    data["symbols"] = data["symbols"].map(
        lambda value: json.dumps(value) if isinstance(value, dict) else value)

    with DB_ORM.connect() as conn:
        conn.execute(query_create_connectors); conn.commit()
        data.to_sql(TABLE, conn, if_exists = "replace", index = True)

#███████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
if False:

    URL = "postgresql://postgres:{password}@localhost:5432/postgres"
    url = URL.format(password = getpass("Enter Postgres password... "))
    DB_ORM = create_engine(url = url, isolation_level = "AUTOCOMMIT")

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
    data = data.drop(columns = columns_stops, errors = "ignore")

    data["expiration"] = None
    regex = r"_([0-9][0-9][0-9][0-9]+)$"
    expirable = data.loc[data["symbol"].str.contains(pat = regex, regex = True)]
    expirable["expiration"] = expirable["symbol"].str.extract(regex)
    expirable["expiration"] = to_datetime(expirable["expiration"], format = "%y%m%d")
    data.loc[expirable.index, "expiration"] = expirable["expiration"]

    with DB_ORM.connect() as conn:
        conn.execute(query_create_symbol_specs); conn.commit()
        data.to_sql(TABLE, conn, if_exists = "replace", index = True)

#███████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
if False:

    query_create_history_tables = {
        "history_ticks": ("""
        CREATE TABLE IF NOT EXISTS {table} (
        venue LowCardinality(String), symbol LowCardinality(String),
        time DateTime64(6, 'UTC'), pa Float64, qa Float64, pb Float64,
        qb Float64, pma Float64, qma Float64, pmb Float64, qmb Float64,
        dus Int32) ENGINE = MergeTree() ORDER BY (time, venue, symbol)
        TTL time + INTERVAL 30 DAY SETTINGS index_granularity = 8192
        """),
        "history_candles": ("""
        CREATE TABLE IF NOT EXISTS {table} (
        tf LowCardinality(String), venue LowCardinality(String),
        symbol LowCardinality(String), time DateTime64(6, 'UTC'),
        oa Float64, ha Float64, la Float64, ca Float64,
        ob Float64, hb Float64, lb Float64, cb Float64,
        volume UInt64, dus Int32) ENGINE = MergeTree() ORDER BY (time, venue, symbol, tf)
        TTL time + INTERVAL 365 DAY SETTINGS index_granularity = 8192
        """)}

    password = getpass("Enter ClickHouse password... ")
    DB_TSS = get_client(host = "localhost", port = 8123,
            username = "clickhouse", password = password)

    print("Databases on ClickHouse server:")
    dbs = DB_TSS.query("SHOW DATABASES")
    for db in dbs.result_rows:
        print(f"=> {db[0]}")

    #DB_TSS.query("DROP DATABASE IF EXISTS clickhouse")
    DB_TSS.query("CREATE DATABASE IF NOT EXISTS clickhouse")
    DB_TSS.query("USE clickhouse")
    for table, query in query_create_history_tables.items():
        DB_TSS.query(query.format(table = table))

    print("ClickHouse Tables and Schemas:")
    tables = DB_TSS.query("SHOW TABLES")
    for table_row in tables.result_rows:
        table_name = table_row[0]
        print(f"\nTable: {table_name}")
        schema = DB_TSS.query(f"DESCRIBE TABLE {table_name}")
        for col in schema.result_rows:
            print(f"=> {col[0]}: {col[1]}")

#███████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
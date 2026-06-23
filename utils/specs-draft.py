# from former Binance DataConnectorWS, specs' writer
"""
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
async def update_specs(self):
    symbol: Symbol = None
    symbols = self._symbols_new
    if not symbols: symbols = self.symbols
    query_list: List[str] = list[str]()
    for symbol in await self.yield_update(symbols):
        query_list.append(symbol.sql_values)
        self._specs[symbol.symbol] = symbol
        while (len(self._specs) >= self.maxlen):
            self._specs.popitem(last = False)
    query_str = str.join(", ", query_list)
    query_str = Symbol.sql_update(query_str)
    async with DB_ORM.acquire() as conn:
        await conn.execute(query_str)
"""
# from former Binance DataConnectorWS, specs' request    
"""
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
async def yield_update(self, symbols: set[str]):
    symbols_new: List = list()
    symbol_dict: Dict = dict()
    args = {"url": self.url_api + "/exchangeInfo"}
    if (symbols := list(symbols)):
        args["params"] = {"symbols": symbols}
        
    async with ClientSession() as session:
        async with session.get(**args) as request:
            response: dict = await request.json()
            symbols_new = response.get("symbols", [])

    for symbol_dict in symbols_new:
        if "symbol" not in symbol_dict: continue
        symbol = symbol_dict["symbol"]
        new = {"venue": self.VENUE, "symbol": symbol,
            "base": symbol_dict.get("baseAsset", None),
            "quote": symbol_dict.get("quoteAsset", None)}
        if (exp := symbol_dict.get("deliveryDate", None)): 
            new["expiration"] = Timestamp.utcfromtimestamp(int(exp) / 1e3)
        for filter_dict in symbol_dict.get("filters", list()):
            if (filter_dict["filterType"].upper() == "PRICE_FILTER"):
                new["min_price_diff"] = float(filter_dict["tickSize"])
            elif (filter_dict["filterType"].upper() == "LOT_SIZE"):
                new["min_order_size"] = float(filter_dict["stepSize"])
                
        yield Symbol(**new)
"""

#███████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class Redis(_ClientFacade, metaclass = Meta):
    _QUERY_CHECK_CONN = "SET test test"

    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def _create(cls, creds: Credentials) -> "Redis":
        host, port = creds.IP.split(":")
        args = {"username": creds.USERNAME, "host": host, "port": int(port),
            "db": 0, "max_connections": 128, "password": creds.PASSWORD}
        #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
        async def connect(**args):
            probe = RedisClient(**args)
            assert await probe.execute_command(cls._QUERY_CHECK_CONN)
            await probe.aclose()
            return RedisClient(**args)
        return cls(asyncio.run(connect(**args)))

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class ClickHouse(_ClientFacade, metaclass = Meta):
    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def _create(cls, creds: Credentials) -> "ClickHouse":
        host, port = creds.IP.split(":")
        args = {"username": creds.USERNAME, "host": host, "port": int(port),
            "database": creds.DATABASE, "password": creds.PASSWORD}
        client = get_clickhouse_client(**args)
        assert client.ping()
        return cls(client)

# Meta.__new__ binds each name below to its singleton instance (loguru-style).
DB_ORM = Postgres
DB_CCH = Redis
DB_TSS = ClickHouse
DB_ORM_DSN = Postgres.url

async def init_db_orm() -> Postgres:
    return DB_ORM














"""


    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def _run_listener(self, func: Callable, table: str, channel: str):
        conn = await asyncpg.connect(self._url)
        queue: asyncio.Queue[str] = asyncio.Queue()
        handler = None
        Log.info(f"\"{func.__name__}\": listening on \"{channel}\" (table \"{table}\")")
        try:
            await conn.execute(self._QUERY_NOTIFY.format(channel = channel, table = table))
            def handler(_conn, _pid, _channel, payload): queue.put_nowait(payload)
            await conn.add_listener(channel, handler)
            while True:
                try: await func(json.loads(await queue.get()))
                except Exception as EXC: Log.exception(EXC)
        finally:
            if handler is not None: await conn.remove_listener(channel, handler)
            await conn.close()
    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    def listen_to(self, table: str, channel: str = None):
        if channel is None: channel = table
        def decorator(func: Callable):
            @wraps(func)
            async def wrapped(*args, **kwargs): return await func(*args, **kwargs)
            async def listener(): await self._run_listener(wrapped, table, channel)
            self._listeners[func.__name__] = listener
            return wrapped
        return decorator

Postgres = PostgresClient()
DB_ORM = Postgres
DB_ORM_DSN = Postgres.url

async def init_db_orm() -> PostgresClient:
    return DB_ORM

#███████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
if (__name__ == "__main__"):

    @Postgres.listen_to(table = "test_postgres_listener")
    async def test(payload: dict):
        Log.info(f"Received payload: {payload}")

    async def main():
        Postgres.start_listeners()
        await asyncio.Event().wait()

    asyncio.run(main())
"""
import aioredis
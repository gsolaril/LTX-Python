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
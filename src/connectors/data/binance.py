#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
from pandas import Timestamp
from typing import Any, Dict, ClassVar
from aiohttp import ClientWebSocketResponse
from src.connectors.venues import Binance, BinanceCoin, BinanceSpot, BinanceUsdm
from src.connectors.ws import DataConnectorWS, DataChannelWS
from src.models import Tick, Candle, TimeFrame
from src.utils import Config, Redis, TZ

#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class DataBinance(DataConnectorWS, Binance):

    FEED_PATH_TICK: ClassVar[str] = ...
    FEED_PATH_KLINE: ClassVar[str] = ...
    CHANNEL_KEY_TICK: ClassVar[str] = ...
    CHANNEL_KEY_KLINE: ClassVar[str] = ...
    SYMBOL_KEY_KLINE: ClassVar[str] = "ps"
    EVENT_KLINE: ClassVar[str] = ...

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __init__(self): super().__init__(
        ticks = DataChannelWS(name = "ticks",
            get_subs = self.get_subs_ticks, on_message = self.on_ticks,
            on_ping = self.on_ping, url_args = self.get_url_headers_ticks),
        klines = DataChannelWS(name = "klines",
            get_subs = self.get_subs_klines, on_message = self.on_klines,
            on_ping = self.on_ping, url_args = self.get_url_headers_klines))

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __post_init__(self):
        super().__post_init__()
        self._crons[self.try_resub] = TimeFrame.S1.value

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def try_resub(self):
        self._WS_to_resub.set()

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def get_urlh(self, path: str):
        base = self.URL_WS or type(self).URL_WS
        if not base: raise ValueError(
            f"{self.name}: websocket url not configured")
        return {"url": base.rstrip("/") + "/" + path.lstrip("/")} 
    async def get_url_headers_ticks(self): return await self.get_urlh(self.FEED_PATH_TICK)
    async def get_url_headers_klines(self): return await self.get_urlh(self.FEED_PATH_KLINE)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def get_subs(self, symbols: set, is_sub: bool, key: str):
        subs = {str.lower(symbol) + key for symbol in symbols}
        payload = {"id": 1, "params": sorted(subs), "method": None}
        payload["method"] = "SUBSCRIBE" if is_sub else "UNSUBSCRIBE"
        return subs, [payload]
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def get_subs_ticks(self, symbols: set, is_sub: bool):
        return self.get_subs(symbols, is_sub, self.CHANNEL_KEY_TICK)
    def get_subs_klines(self, symbols: set, is_sub: bool):
        return self.get_subs(symbols, is_sub, self.CHANNEL_KEY_KLINE)
    
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def on_ping(self, WS: ClientWebSocketResponse, sender: bool = False):
        if not sender: return await WS.send_str("pong")

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    @Redis.stream#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def on_ticks(self, data: Dict):
        if (data := data.get("data", None)) is None: return
        event = data.get("e", None)
        if (event is not None) and (event != "bookTicker"): return
        symbol = data.get("s", None)
        if symbol is None: return

        tse = data.get("E", None)
        if (tse is not None): ts = Timestamp.fromtimestamp(int(tse) / 1e3, TZ)
        ts = Timestamp.now(TZ)
        symbol = self._specs.get(self.symbol_to_local(symbol), None)
        if symbol is None: return
        tick = Tick(symbol = symbol, time = ts, pa = data["a"],
               qa = data["A"], pb = data["b"], qb = data["B"])
        self._bundle.on_tick(tick)
        yield tick
        
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    @Redis.stream#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def on_klines(self, data: Dict):
        if (data := data.get("data", None)) is None: return
        if (event := data.get("e", None)) is None: return
        if (event != self.EVENT_KLINE): return
        symbol = data.get(self.SYMBOL_KEY_KLINE, None)
        if symbol is None: return

        data = data.get("k", None)
        if data is None: return
        tse = data.get("t", None)
        tf_str = data.get("i", None)
        closed = data.get("x", False)
        if not tse or not tf_str or not closed: return
        ts = Timestamp.fromtimestamp(int(tse) / 1000, TZ) + self._offset
        ts = Timestamp.now(TZ)
        symbol = self._specs.get(self.symbol_to_local(symbol), None)
        if symbol is None: return
        candle = Candle(symbol = symbol, 
              time = ts, tf = TimeFrame.swap_tn(tf_str), volume = data["n"],
              oa = data["o"], ha = data["h"], la = data["l"], ca = data["c"],
              ob = data["o"], hb = data["h"], lb = data["l"], cb = data["c"])
        self._bundle.on_candle(candle)
        yield candle

#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class DataBinanceCoin(DataBinance, BinanceCoin):
    URL_WS: ClassVar[str] = "wss://dstream.binance.com" \
        if Config.TEST else "wss://dstream.binance.com"
    FEED_PATH_TICK: ClassVar[str] = "public/stream"
    FEED_PATH_KLINE: ClassVar[str] = "market/stream"
    CHANNEL_KEY_TICK: ClassVar[str] = "@bookTicker"
    CHANNEL_KEY_KLINE: ClassVar[str] = "@continuousKline_1s"
    SYMBOL_KEY_KLINE: ClassVar[str] = "ps"
    EVENT_KLINE: ClassVar[str] = "continuous_kline"

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class DataBinanceSpot(DataBinance, BinanceSpot):
    URL_WS: ClassVar[str] = "wss://stream.binance.com:9443" \
        if Config.TEST else "wss://stream.binance.com:9443"
    FEED_PATH_TICK: ClassVar[str] = "stream"
    FEED_PATH_KLINE: ClassVar[str] = "stream"
    CHANNEL_KEY_TICK: ClassVar[str] = "@bookTicker"
    CHANNEL_KEY_KLINE: ClassVar[str] = "@kline_1s"
    SYMBOL_KEY_KLINE: ClassVar[str] = "s"
    EVENT_KLINE: ClassVar[str] = "kline"

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class DataBinanceUsdm(DataBinance, BinanceUsdm):
    URL_WS: ClassVar[str] = "wss://fstream.binance.com" \
        if Config.TEST else "wss://fstream.binance.com"
    FEED_PATH_TICK: ClassVar[str] = "public/stream"
    FEED_PATH_KLINE: ClassVar[str] = "market/stream"
    CHANNEL_KEY_TICK: ClassVar[str] = "@bookTicker"
    CHANNEL_KEY_KLINE: ClassVar[str] = "_perpetual@continuousKline_1s"
    SYMBOL_KEY_KLINE: ClassVar[str] = "ps"
    EVENT_KLINE: ClassVar[str] = "continuous_kline"

#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
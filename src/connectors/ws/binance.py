#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
import asyncio, hmac, hashlib
from pandas import Timestamp
from urllib.parse import urlencode
from aiohttp import ClientSession
from aiohttp import ClientWebSocketResponse
from typing import Any, List, Dict, ClassVar
from .base import DataConnectorWS, DataStreamWS
from .base import ExecConnectorWS, ExecStreamWS
from src.connectors.base import Venue
from src.models import *
from src.utils import *

#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class Binance(Venue):

    STATUS = {"NEW": "OK", "FILLED": "OK", "CANCELED": "OK", "PARTIALLY_FILLED": "OK"}
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    class Credentials(Venue.Credentials): api_key: str; secret: str
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def signature(self, payload: Dict[str, Any]):
        hmac_key = str.encode(self.creds.secret, "utf-8")
        hmac_msg = urlencode(payload, doseq = True).encode("utf-8")
        signature = hmac.new(hmac_key, hmac_msg, hashlib.sha256)
        return signature.hexdigest()

    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def sign_payload(cls, payload: Dict[str, Any]):
        payload = payload.copy()
        ms = Timestamp.utcnow().timestamp() * 1e3
        payload.setdefault("timestamp", int(ms))
        payload.setdefault("recvWindow", 5000)
        payload["signature"] = cls.signature(payload)
        return payload

    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def to_create_payload(cls, order: Order):
        payload = {
            "symbol": cls.symbol_to_venue(order.symbol),
            "side": order.side, "type": order.type,
            "quantity": abs(order.size),
        }
        if (order.price is not None):
            payload.update({"price": order.price, "timeInForce": order.mode})
        return payload

    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def to_modify_payload(cls, order: Order, order_id: str):
        payload = {
            "symbol": cls.symbol_to_venue(order.symbol),
            "side": order.side, "orderId": order_id,
            "quantity": abs(order.size),
        }
        if (order.price is not None):
            payload["price"] = order.price
        return payload

    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def to_cancel_payload(cls, symbol: str, order_id: str):
        return {"symbol": cls.symbol_to_venue(symbol), "orderId": order_id}

    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def status_to_local(cls, response: dict, http_status: int = None):
        if (http_status is not None) and (http_status >= 400):
            return "ERROR"
        if "code" in response: return "ERROR"
        status = response.get("status", None)
        if isinstance(status, str):
            mapped = cls.STATUS.get(status.upper(), None)
            if mapped is not None: return mapped
        if "orderId" in response: return "OK"

#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class DataBinance(DataConnectorWS, Binance):

    STREAM_PATH_TICK: ClassVar[str] = ...
    STREAM_PATH_KLINE: ClassVar[str] = ...
    CHANNEL_KEY_TICK: ClassVar[str] = ...
    CHANNEL_KEY_KLINE: ClassVar[str] = ...
    SYMBOL_KEY_KLINE: ClassVar[str] = "ps"
    KLINE_EVENT: ClassVar[str] = ...

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __init__(self): super().__init__(
        ticks = DataStreamWS(name = self.__class__.__name__ + "/ticks",
            get_subs = self.get_subs_ticks, on_message = self.on_ticks,
            on_ping = self.on_ping, get_urlh = self.get_url_headers_ticks),
        klines = DataStreamWS(name = self.__class__.__name__ + "/klines",
            get_subs = self.get_subs_klines, on_message = self.on_klines,
            on_ping = self.on_ping, get_urlh = self.get_url_headers_klines))

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def get_urlh(self, path: str): return {"url": self.url + "/" + path} 
    async def get_url_headers_ticks(self): return await self.get_urlh(self.STREAM_PATH_TICK)
    async def get_url_headers_klines(self): return await self.get_urlh(self.STREAM_PATH_KLINE)

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

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def on_ticks(self, data: Dict):
        if (data := data.get("data", None)) is None: return
        event = data.get("e", None)
        if (event is not None) and (event != "bookTicker"): return
        symbol = data.get("s", None)
        if symbol is None: return

        tse = data.get("E", None)
        if (tse is not None): ts = Timestamp.utcfromtimestamp(int(tse) / 1e3)
        ts = Timestamp.utcnow()
        symbol = self._specs.get(self.symbol_to_local(symbol), None)
        if symbol is None: return None
        return Tick(symbol = symbol, time = ts, pa = data["a"],
                qa = data["A"], pb = data["b"], qb = data["B"])
            
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def on_klines(self, data: Dict):
        if (data := data.get("data", None)) is None: return
        if (event := data.get("e", None)) is None: return
        if (event != self.KLINE_EVENT): return
        symbol = data.get(self.SYMBOL_KEY_KLINE, None)
        if symbol is None: return

        data = data.get("k", None)
        if data is None: return
        tse = data.get("t", None)
        tf_str = data.get("i", None)
        closed = data.get("x", False)
        if not tse or not tf_str or not closed: return
        ts = Timestamp.utcfromtimestamp(int(tse) / 1000) + self._offset
        ts = Timestamp.utcnow()
        symbol = self._specs.get(self.symbol_to_local(symbol), None)
        if symbol is None: return None
        return Candle(symbol = symbol, 
            time = ts, tf = TimeFrame.swap_tn(tf_str), volume = data["n"],
            oa = data["o"], ha = data["h"], la = data["l"], ca = data["c"],
            ob = data["o"], hb = data["h"], lb = data["l"], cb = data["c"])

#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class ExecBinance(ExecConnectorWS, Binance):
     # TODO: implement for Binance based on Binance API docs

    STREAM_PATH_ACCOUNT: ClassVar[str] = ...
    STREAM_PATH_EXEC: ClassVar[str] = ...
    CHANNEL_KEY_ACCOUNT: ClassVar[str] = ...
    CHANNEL_KEY_EXEC: ClassVar[str] = ...

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def get_url_args(self, cred: Binance.Credentials, channel: str): ...
    def get_subs(self, cred: Binance.Credentials, channel: str): ...
    async def on_ping(self, sender: bool = False): ...
    async def on_message(self, message: Any): ...
    async def create_order(self, aid: str, order: Order): ...
    async def cancel_order(self, aid: str, order_id: str): ...
    async def modify_order(self, aid: str, order_id: str, order: Order): ...

#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class BinanceCoin(Binance):
    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def symbol_to_local(cls, symbol: str): return symbol.upper()
    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def symbol_to_venue(cls, symbol: str): return symbol.lower()

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class DataBinanceCoin(DataBinance, BinanceCoin):
    STREAM_PATH_TICK: ClassVar[str] = "public/stream"
    STREAM_PATH_KLINE: ClassVar[str] = "market/stream"
    CHANNEL_KEY_TICK: ClassVar[str] = "@bookTicker"
    CHANNEL_KEY_KLINE: ClassVar[str] = "@continuousKline_1s"
    SYMBOL_KEY_KLINE: ClassVar[str] = "ps"
    EVENT_KLINE: ClassVar[str] = "continuous_kline"

#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class BinanceSpot(Binance):
    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def symbol_to_local(cls, symbol: str): return symbol
    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def symbol_to_venue(cls, symbol: str): return symbol

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class DataBinanceSpot(DataBinance, BinanceSpot):
    STREAM_PATH_TICK: ClassVar[str] = "stream"
    STREAM_PATH_KLINE: ClassVar[str] = "stream"
    CHANNEL_KEY_TICK: ClassVar[str] = "@bookTicker"
    CHANNEL_KEY_KLINE: ClassVar[str] = "@kline_1s"
    SYMBOL_KEY_KLINE: ClassVar[str] = "s"
    EVENT_KLINE: ClassVar[str] = "kline"

#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class BinanceUsdm(Binance):
    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def symbol_to_local(cls, symbol: str): return symbol.upper()
    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def symbol_to_venue(cls, symbol: str): return symbol.lower()

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class DataBinanceUsdm(DataBinance, BinanceUsdm):
    STREAM_PATH_TICK: ClassVar[str] = "public/stream"
    STREAM_PATH_KLINE: ClassVar[str] = "market/stream"
    CHANNEL_KEY_TICK: ClassVar[str] = "@bookTicker"
    CHANNEL_KEY_KLINE: ClassVar[str] = "_perpetual@continuousKline_1s"
    SYMBOL_KEY_KLINE: ClassVar[str] = "ps"
    EVENT_KLINE: ClassVar[str] = "continuous_kline"

#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
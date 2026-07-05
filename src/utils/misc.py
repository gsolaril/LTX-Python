#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
import asyncio
from collections import defaultdict
from pandas import DataFrame, Timestamp
from typing import Any, Callable
from .base import TZ

#███████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀ 
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class Queue(asyncio.Queue):
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __init__(self, maxsize: int, checkpoints: dict[float, Callable] = None):
        if not isinstance(checkpoints, dict): checkpoints = dict[float, Callable]()
        self._checkfunctions = [None, None]
        self._checkvalues = [0.0, 1.0]
        self.last_checkpoint = 0
        self.last_checkpeek = 0
        super().__init__(maxsize)
        self.size_1pct = maxsize // 100
        if (checkpoints is not None):
            for value, func in checkpoints.items():
                if (value > 1.0): value /= maxsize
                self._checkvalues.insert(-1, value)
                self._checkfunctions.insert(-1, func)
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def get(self): item = await super().get(); return item
    def get_nowait(self): item = super().get_nowait(); return item
    async def put(self, item: Any): await super().put(item); self._recheck()
    def put_nowait(self, item: Any): super().put_nowait(item); self._recheck()
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def _recheck(self):
        self.last_checkpeek += 1 
        if (self.last_checkpeek >= self.size_1pct):
            value = self.qsize() / self.maxsize
            index_next = self.last_checkpoint
            index_lower = self.last_checkpoint
            index_upper = self.last_checkpoint + 1
            value_lower = self._checkvalues[index_lower]
            value_upper = self._checkvalues[index_upper]
            self.last_checkpeek = 0
            if (value_upper <= value): index_next += 1
            elif (value < value_lower): index_next -= 1
            if (index_next != self.last_checkpoint):
                func = self._checkfunctions[index_next]
                if (func is not None): func(value)
                self.last_checkpoint = index_next

#███████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄
class Report:
    PRINT_LIMIT = 50
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __init__(self, name: str):
        self._name = name
        self._start_at = Timestamp.now(TZ)
        self._batch_at = self._entry_at = None
        self._last_count = self._mean_count = 0
        self._batches = self._total_count = 0
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def _add(self, count: int):
        if (self._batch_at is None):
            self._batch_at = Timestamp.now(TZ)
        self._last_count = self._last_count + count
        self._total_count = self._total_count + count
        self._entry_at = Timestamp.now(TZ)
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def _close_batch(self):
        self._batches, self._last_count = self._batches + 1, 0
        self._mean_count = self._total_count / self._batches
        self._batch_at = Timestamp.now(TZ)
    #▄▄▄▄▄▄▄▄▄▄
    @property#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __dict__(self): return {
        "name": self._name, "start_at": self._start_at,
        "batch_at": self._batch_at, "last_count": self._last_count,
        "entry_at": self._entry_at, "mean_count": self._mean_count,
        "batches": self._batches, "total_count": self._total_count}

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class Reporter(defaultdict[str, Report]):
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __init__(self, *keys, name: str = "Reporter", print_limit: int = 50):
        super().__init__()
        for key in keys: self[key] = Report(key)
        self.start_at = Timestamp.now(TZ)
        self.print_limit = print_limit
        self.name = name
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def add(self, key: str, count: int = 1):
        if (key not in self): self[key] = Report(key)
        if (count > 0): self[key]._add(count)
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def close_batch(self):
        for report in self.values():
            report._close_batch()
    #▄▄▄▄▄▄▄▄
    @property
    def df(self):
        df = DataFrame([R.__dict__ for R in self.values()])
        if not df.dropna().empty: return df.set_index("name")
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __repr__(self): return self.to_string()
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def to_string(self, next_at: Timestamp = None):
        if (df := self.df) is None: return
        if df.empty: return
        df = df.rename_axis(None)
        if next_at is not None:
            header = f"[Next report @ {next_at:%H:%M}]"
            df = df.rename_axis(header, axis = "columns")
        df["mean_count"] = df["mean_count"].astype(int)
        df["start_at"] = df["start_at"].dt.strftime("%Y/%m/%d %H:%M")
        df["batch_at"] = df["batch_at"].dt.strftime("%H:%M:%S")
        df["entry_at"] = df["entry_at"].dt.strftime("%H:%M:%S.%f").str[: -3]
        if (df.shape[0] <= self.print_limit): df = df.sort_index()
        else: df = df.sort_values("total_count", ascending = False)
        start_str = self.start_at.strftime("%Y/%m/%d %H:%M")
        verbose = f"Reporter \"{self.name}\" ongoing since \"{start_str}\":"
        return verbose + "\n" + df.to_string(max_rows = self.print_limit)

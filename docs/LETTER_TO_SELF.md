# Letter to Self — LTX Systems Manual (for a C++ rebuild)

> **You have amnesia.** You are given (1) this document and (2) the `LTX-Python` repository as it exists now.  
> **Mission:** recreate the **same capabilities** in C++. Not Python idioms. Not decorators. Not metaclasses. Not pandas types.  
> **Success:** same jobs, same external ABIs (Redis grammar, LTXB bytes, CH/PG schemas), same ordering/time semantics.  
> **Also mandatory:** the author’s **personal coding style** (§0). Capabilities without that style are incomplete.  
> **Read the code together with this letter.** The letter is the map; the code is the territory. When they disagree on a stub/TODO, prefer the letter’s “status” notes. For style, prefer §0 + live examples in this repo (especially `src/models/bundle.py` → `TestBundle.test_resample`).

---

## Table of contents

0. [STRICT personal coding style (non-negotiable)](#0-strict-personal-coding-style-non-negotiable)
1. [Product in one page](#1-product-in-one-page)
2. [Architecture & dataflow](#2-architecture--dataflow)
3. [Time, timezone, ordering](#3-time-timezone-ordering)
4. [TimeFrame (static table — no metaclass)](#4-timeframe-static-table--no-metaclass)
5. [Domain objects](#5-domain-objects)
6. [Bundle & multi-timeframe](#6-bundle--multi-timeframe)
7. [Redis bus (full ABI)](#7-redis-bus-full-abi)
8. [LTXB binary format & merge engine](#8-ltxb-binary-format--merge-engine)
9. [ClickHouse](#9-clickhouse)
10. [Postgres](#10-postgres)
11. [Agent process model](#11-agent-process-model)
12. [Connectors (live venues)](#12-connectors-live-venues)
13. [Simulator / DataProvider](#13-simulator--dataprovider)
14. [Monitoring / DataCollector](#14-monitoring--datacollector)
15. [Orders & execution](#15-orders--execution)
16. [Config, credentials, runtime](#16-config-credentials-runtime)
17. [CLI, tests, packaging](#17-cli-tests-packaging)
18. [What is NOT implemented](#18-what-is-not-implemented)
19. [C++ rebuild blueprint](#19-c-rebuild-blueprint)
20. [Sufficiency checklist](#20-sufficiency-checklist)
21. [File index (open these)](#21-file-index-open-these)

---

## 0. STRICT personal coding style (non-negotiable)

These rules apply to **all** new code in a C++ rebuild (and when editing this Python tree). They are **strict**. Study the existing `src/` files; do not invent a generic “clean C++” style that fights this one.

### 0.1 Spaces around operators — ALWAYS

Wherever there is an equality or comparison (and the same idea for related operators), put **exactly one space on the left and one space on the right**.

Applies to: `=`, `==`, `!=`, `<`, `>`, `<=`, `>=`, and the same spirit for assignments / comparisons in:

- variable declarations and assignments  
- function / method default arguments  
- conditions (`if`, `while`, `for`, ternary)  
- keyword arguments / designated initializers when written with `=`  
- SQL / query strings you author  

**NEVER** tighten tokens together.

```cpp
// WRONG — never
if (a==1) { ... }
int x=2;
void f(int n=0);

// RIGHT
if (a == 1) { ... }
int x = 2;
void f(int n = 0);
```

```python
# RIGHT (this repo’s habit)
if (n_tick == 0):
    ...
print(verbose_tick.format(nt := 0), end = "")
self.bundle.on_tick(Tick(symbol = symbol, qa = 1.0, qb = 1.0,
            time = time_C, pa = price_C, pb = price_C, dus = 0))
```

Same for SQL you write: `time >= '...'` not `time>='...'`.

### 0.2 Section separators between major blocks

When two neighboring blocks deal with **different** concerns (e.g. end of one class, start of another; file regions; major subsystems in one translation unit), delimit them with the full-width banner of exactly 96-char-width (including comment starter: Python's hash "#" or C++'s double slash "//") used throughout this repo:

```cpp
//██████████████████████████████████████████████████████████████████████████████████████████████
//▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
```

Python equivalent (already ubiquitous):

```python
#███████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
```

Use these freely between classes / large regions. They are part of the visual language of the project.

### 0.3 Overlines above every class and every function

Every **class** and every **function/method** gets an overline comment immediately above its signature.

- The overline starts above the **first character** of the signature line (`c` of `class`, `d` of `def`, return type / `void` / etc. in C++).  
- It ends above the character **just before** the colon that ends a Python signature, or 2 characters before the **opening brace `{`** that opens a C++ function/class body (because braces are K&R-style — see §0.4).

Python pattern (from "LTX-Python" repo):

```python
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
def inline(self, time: Timestamp = None, type: str = None):
```

```python
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
def test_resample(self):
```

Why 2 characters before C++'s opening brace? Because between the last character of the involved code line and the brace, there is a space. See the "int myFunction" below (compare the length of the overline, with the length of the function opener)...

```cpp
//▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
int myFunction(int a = 0, int b = 1) {
    ...
}
```

```cpp
//▄▄▄▄▄▄▄▄
class Tick {
    ...
};
```

Length of the `▄` run should visually match the signature span (from first char through the non-space char that comes right before `:` / `{`). Match the density you see in `src/models/*.py` and `src/interfaces/simulator/dataprovider.py`.

### 0.4 C++ brace style — opening `{` on the same line

Always:

```cpp
int myFunction(...args...) {
    ...
}
```

Never Allman-style:

```cpp
// WRONG for this project
int myFunction(...args...)
{
    ...
}
```

Same for `class` / `struct` / `if` / `for` / `while` / `switch` when you open a block with `{`.

### 0.5 Vertical rhythm — readable blocks, not a snake

- Inside a function / method that grows past roughly **15–25 lines**, insert blank lines between logical sections (setup → loops → asserts → teardown), not one continuous mass.  
- Always leave a tidy blank line between the end of one function/class and the overline/start of the next.  
- Prefer the cadence of `TestBundle.test_resample` in `src/models/bundle.py`: grouped assignments, blank line, loop, blank line, nested work, blank line, assertions.

Example of the intended feel (Python; mirror in C++):

```python
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
def test_resample(self):
    tf: TimeFrame = None
    now = Timestamp.now(TZ)
    times_O: dict[Symbol, List] = dict.fromkeys(self.symbols)
    ...
    time_finish: Timestamp = now.ceil(self.max_tf.value)

    res_candles = dict.fromkeys(self.symbols)
    for symbol in self.symbols:
        ...

    verbose_tick = self.VERBOSE_TICK.format("{0}",
        len(self.symbols) * len(price_diff) * self.max_ratio)
    ...
```

### 0.6 Folder structure — mirror this repo, especially `src/`

When creating the C++ tree (or any sibling LTX-C++ project), **keep the same logical layout**, especially under `src/`:

```text
src/
  models/          # Tick, Candle, Bundle, Symbol, TimeFrame, Order, Agent bases
  utils/           # Config, Redis/PG/CH clients, logging, credentials hooks
  connectors/      # venues/, data/, exec/, ws
  interfaces/      # simulator/ (DataProvider, FileReader), controllers/
  monitoring/      # infra/datacollector
  framework/       # strategy engine (today empty — same place when filled)
  strategies/      # user strategies
docs/              # including this letter
dbin/              # LTXB binaries
services/          # systemd / process units
utils/             # offline tools (db bootstrap, notebooks) — distinct from src/utils
```

Do **not** flatten everything into `include/` + `src/lib/` without preserving these domain folders. Headers may live as `src/models/tick.hpp` or `include/ltx/models/tick.hpp`, but the **module boundaries and names** should remain recognizable: `models`, `utils`, `connectors`, `interfaces/simulator`, `monitoring`.

### 0.7 Style study checklist (after amnesia)

Re-read these before writing C++:

1. `src/models/bundle.py` — `TestBundle.test_resample` (spacing, blank lines, overlines)  
2. `src/models/misc.py` — section banners between Symbol / TimeFrame / Account  
3. `src/models/data.py` — overlines on every class; spaces in signatures  
4. `src/interfaces/simulator/dataprovider.py` — banners between DataReader / FileReader / tests  

If your generated C++ looks like stock clang-format Google style with `a==1` and Allman braces, **you failed §0** even if the algorithms are correct.

---

## 1. Product in one page

**LTX** is multi-venue trading infrastructure.

Long-lived processes called **agents** do one job each:

| Agent family | Job |
|--------------|-----|
| **Data connector** | Venue WS/REST → Tick/Candle → Redis `DATA` streams |
| **Exec connector** | Redis `REQ` → venue orders → Redis `ORDERS` / Balance |
| **DataCollector** | Redis `DATA` → batch insert ClickHouse history |
| **DataProvider** | Historical LTXB (or CH) → same Redis `DATA` grammar (backtest prefix) |

Shared infrastructure:

- **Redis Streams** = real-time message bus (IPC between all agents)
- **Postgres** = connector/monitor config + `symbol_specs` (+ accounts)
- **ClickHouse** = durable tick/candle history
- **`dbin/`** = packed little-endian market-data files (`LTXB`) for fast replay

Related repo (not this tree): **`/home/gaston/LTX-MT5`** — MetaTrader EAs; Redis bridge planned; stub only here.

Roadmap leftovers (empty): strategy framework, fake exchange, order router, alerting, dashboards. See §18.

---

## 2. Architecture & dataflow

```
                         ┌─────────────────── Postgres ───────────────────┐
                         │  connectors / monitoring / symbol_specs / …     │
                         └───────────────┬─────────────────────────────────┘
                                         │ config + NOTIFY hot-reload
┌─────────────┐     ┌────────────────────▼────────────────────┐
│ Venue WS/API│────▶│ DataConnector / ExecConnector (agents)  │
└─────────────┘     └────────────┬───────────────────────────┘
                                 │ XADD
                                 ▼
                    ┌────────────────────────────┐
                    │       Redis Streams        │
                    │  LTX\|DATA\|venue\|sym\|tf    │
                    │  LTX\|EXEC\|venue\|acc\|…     │
                    │  BTX-* (backtest runs)     │
                    └───────┬───────────┬────────┘
                            │           │
              ┌─────────────▼──┐   ┌────▼──────────────┐
              │ DataCollector  │   │ Strategy / others │
              │ → ClickHouse   │   │ (future)          │
              └────────────────┘   └───────────────────┘

┌──────────────┐    ┌─────────────────────┐
│ dbin/*.bin   │───▶│ DataProvider agent  │───▶ Redis BTX-*|DATA|…
│ (or CH hist) │    │ FileReader+merge    │
└──────────────┘    └─────────────────────┘
```

**Invariant:** live and backtest publish the **same payload shape** onto streams that differ mainly by **prefix** (`LTX` vs `BTX-<id>`). Downstream code can treat them uniformly if it matches on midfix + suffix.

---

## 3. Time, timezone, ordering

### 3.1 Canonical time

- Timezone: **UTC** (`Config.TIMEZONE`, imported as `TZ`).
- Preferred C++ representation: **`int64_t time_us`** = microseconds since Unix epoch UTC.
- Python uses `pandas.Timestamp` with tz; convert with `int(ts.timestamp() * 1e6)`.
- `dus` = delay in microseconds between event time and local receive/construct time. Optional on archive; computed if omitted.

### 3.2 Event time vs bar open time

| Object | Stored `time` | Ordering key `time_event` |
|--------|---------------|---------------------------|
| Tick | quote timestamp | **same** |
| Candle | **bar open** (floored to TF) | **bar close** = open + TF duration |

This matters for merge and for “when does a candle become visible”: a candle is ordered by when it **closes**.

### 3.3 Total ordering for merged market data

When comparing two quotes (heap / sort):

1. `time_event` ascending  
2. else `symbol.venue` ascending, then `symbol.symbol` ascending  
3. else **Tick before Candle** (at identical event time + symbol)  
4. else Candle vs Candle: smaller `TimeFrame` duration first (`tf < other.tf`)

Heap implementation in file replay uses int keys approximating this — see §8.4.

### 3.4 Flooring

Candle open = floor(raw_time → TF grid).  
For fixed-duration TFs (all current ones), in µs:

```text
period_us = <from table>
open_us   = (time_us / period_us) * period_us   // integer division
close_us  = open_us + period_us
```

This matches pandas `Timestamp.floor(Timedelta)` for UTC fixed durations used here.

---

## 4. TimeFrame (static table — no metaclass)

**Product rule:** minimum TF is **S1**, maximum is **D1**. Do not invent sub-second bars.

### 4.1 Complete set (30 frames)

Built from divisors of 60 (seconds/minutes) and 24 (hours):

**Seconds:** S1,S2,S3,S4,S5,S6,S10,S12,S15,S20,S30  
**Minutes:** M1,M2,M3,M4,M5,M6,M10,M12,M15,M20,M30  
**Hours/Day:** H1,H2,H3,H4,H6,H8,H12,D1  

### 4.2 Periods in microseconds (copy into C++)

```cpp
// name -> period_us
S1=1'000'000, S2=2'000'000, S3=3'000'000, S4=4'000'000, S5=5'000'000,
S6=6'000'000, S10=10'000'000, S12=12'000'000, S15=15'000'000,
S20=20'000'000, S30=30'000'000,
M1=60'000'000, M2=120'000'000, M3=180'000'000, M4=240'000'000,
M5=300'000'000, M6=360'000'000, M10=600'000'000, M12=720'000'000,
M15=900'000'000, M20=1'200'000'000, M30=1'800'000'000,
H1=3'600'000'000, H2=7'200'000'000, H3=10'800'000'000, H4=14'400'000'000,
H6=21'600'000'000, H8=28'800'000'000, H12=43'200'000'000,
D1=86'400'000'000
```

### 4.3 Pseudo-TF on the bus

- **`T1`** = tick stream suffix (not a candle TF). Used in Redis keys and DataCollector filters.

### 4.4 Divisor graph (for MTF resample)

For each TF `U`, the set of strictly smaller TFs `L` such that `period(U) % period(L) == 0` are its divisors.  
`TimeFrame.updatable(now, mtf)` yields pairs `(tf_to_update, best_lower_source)` for bars that just closed at `now` (relative to day/min grid). Used by `Bundle.resample`.

**C++:** precompute divisor lists at compile time or startup; no runtime metaclass.

### 4.5 Name helpers

- `swap_nt("M5")` style in Python: name↔“5m” swaps exist (`swap_nt` / `swap_tn`) — useful for venue APIs; optional.

---

## 5. Domain objects

Code roots: `src/models/data.py`, `misc.py`, `order.py`.

### 5.1 Symbol

| Field | Meaning |
|-------|---------|
| `venue` | e.g. `BinanceUsdm`, `Polymarket` |
| `symbol` | venue-local code e.g. `BTCUSDT` |
| `id` | often `"VENUE SYMBOL"`; PG primary key in `symbol_specs` |
| `quote` / `base` | quote currency / base asset (defaults: quote=`USD`, base=`symbol`) |
| `min_price_diff` | tick size |
| `min_order_size` | min abs size |
| `min_stops_diff` | min SL/TP distance |
| `expiration` | optional contract expiry |

Ordering / equality: string form `venue + " " + symbol` (`SEP = " "`).  
Hash/eq on that string.  
`<`: venue then symbol lexicographic.

Postgres table: **`symbol_specs`** (§10).

### 5.2 Account

| Field | Meaning |
|-------|---------|
| `id` | account id |
| `venue` | must match symbol venue when paired |
| `leverage` | default 1 |
| `balance`, `equity`, `margin` | running balances |

Derived:

```text
ppal = margin * leverage          (0 if no margin)
uPNL = equity - balance           (0 if no equity)
uPRC = uPNL / balance             (0 if no equity)
mPRC = margin / equity            (0 if no equity)
```

### 5.3 BasePoint / Quote / DataPoint

- **BasePoint:** `time`, `dus`; virtual `time_event`, `time_us`.
- **Quote:** BasePoint + `symbol`. Stream template `{venue}|{symbol}|{tf}`.
- **DataPoint:** generic indexed payload (`index`, `data` dict) for non-quote streams; subclasses must define `SCHEMA`.

### 5.4 Tick

```text
pa, qa  = ask price, ask size
pb, qb  = bid price, bid size
error   = (pa*qa==0) OR (pb*qb==0)
```

**Binary SCHEMA (LTXB):** `time:i64, pa:f32, qa:f32, pb:f32, qb:f32`  
**Redis payload keys:** `pa,qa,pb,qb,dus`  
**Stream tf:** always `T1`

### 5.5 Candle

```text
tf                  TimeFrame
oa,ha,la,ca         ask OHLC
ob,hb,lb,cb         bid OHLC
volume              int (count of updates / ticks aggregated — see on_tick)
_time_close         open + tf
_time_ltick         last contributing tick/candle time
time_event          == _time_close
truthiness          volume>0 && oa>0 && ob>0
```

**Binary SCHEMA:**  
`time:i64, oa,ha,la,ca,ob,hb,lb,cb,volume : f32`  
(volume stored as f32 on disk; cast to int in process)

**Redis payload keys:** `oa,ha,la,ca,ob,hb,lb,cb,volume,dus`  
**Stream tf:** `tf.name` e.g. `M1`

#### Candle.on_tick(tick) — algorithm

```
if tick.time < open: return
if tick.time < _time_ltick: return          # out of order
if tick.time >= close: return
if tick.symbol != symbol: return
if tick.error: return
_time_ltick = tick.time
volume += 1
# initialize ask/bid open/high/low from first valid prices
if oa is None: oa = tick.pa; same for ha,la and bid side from pb
if tick.pa: ha=max(ha,pa); la=min(la,pa); ca=pa
if tick.pb: hb=max(hb,pb); lb=min(lb,pb); cb=pb
```

#### Candle.on_candle_lower(child) — algorithm

Fold a **closed lower TF** candle into this higher TF bar (same symbol), if child fully inside `[open, close]` and `_time_ltick` advances. Sum volumes; merge OHLC; set close from child’s close.

#### Candle.on_candle_prev(cls, candle)

Factory: next empty bar starting at `candle._time_close`, carrying previous close into `ca`/`cb` seeds (oa/ob None, volume 0).

### 5.6 Balance (streaming account snapshot)

Stream template: `{venue}|{account_id}|{symbol}`  
Payload: `balance,equity,margin,uPNL,uPRC,mPRC,dus`  
Also writes balance/equity/margin onto the `Account` object.

---

## 6. Bundle & multi-timeframe

Code: `src/models/bundle.py`.

### 6.1 Structure

```text
Bundle
  _maxlen: int = 10 (connectors often use 60)
  _ignore_tfs: set of TFs to skip
  _candles: map[TimeFrame][Symbol] -> deque[Candle] (maxlen=_maxlen)
  _tick_first, _tick_last
```

### 6.2 on_tick

1. Remember first/last tick.  
2. Ensure S1 deque for symbol.  
3. If S1 ignored, return.  
4. `opened_at = floor(tick.time, S1)`.  
5. If deque empty or last candle open != opened_at → append new S1 Candle(open=opened_at).  
6. `candles[-1].on_tick(tick)`.

### 6.3 resample(now)

1. `closed_at = floor(now, S1)`.  
2. For each `(tf_upper, tf_lower)` from `TimeFrame.updatable(now)`:  
   - skip if upper == S1 or upper ignored  
   - `tf_ratio = period_upper / period_lower`  
   - `opened_at = closed_at - period_upper`  
   - for each symbol with lower candles:  
     - build empty upper candle at `opened_at`  
     - fold last `min(len, ratio)` lower candles via `on_candle_lower`  
     - if candle truthy → `on_candle` + yield  

**Test:** `TestBundle.test_resample` is the behavioral oracle — port or keep calling via bindings.

### 6.4 StreamingBundle

Subclass that publishes yielded candles through Redis stream decorator (behavior: each closed MTF candle → XADD). Cron typically every **S1**.

---

## 7. Redis bus (full ABI)

Code: `src/utils/clients.py` (`RedisManager`), `src/models/agent.py` (stream format).

### 7.1 Separator

`Redis.SEP = "|"`

### 7.2 Stream name construction

```text
stream = join("|", [stream_prefix, STREAM_MIDFIX, format(STREAM_KEY, **parts)])
```

| Context | `stream_prefix` | `STREAM_MIDFIX` |
|---------|-----------------|-----------------|
| Live data connectors | `LTX` (default) | `DATA` |
| Live exec connectors | `LTX` | `EXEC` |
| DataCollector scan | listens to prefix `DATA` in code — **verify live deploy**; class sets `STREAM_PREFIX="DATA"` which may be intentional short prefix or legacy — **read `datacollector.py` + live keys** |
| DataProvider backtest | `BTX-` + base36(time) | `DATA` |

**STREAM_KEY templates:**

| Type | Template | Example suffix |
|------|----------|----------------|
| Quote/Tick/Candle | `{venue}\|{symbol}\|{tf}` | `BinanceUsdm\|BTCUSDT\|T1` |
| Balance | `{venue}\|{account_id}\|{symbol}` | `…\|acc1\|NAV` |
| Request / Order | `{venue}\|{account_id}\|REQ` | |
| Response | `{venue}\|{account_id}\|ORDERS` | |

Full tick example:

```text
LTX|DATA|BinanceUsdm|BTCUSDT|T1
```

### 7.3 Logical message (before XADD)

```text
{
  "stream":  "<fully formatted stream name>",
  "time":    <int64 micros>,          // quotes use key "time"
  "payload": { <flat fields> }        // strings/numbers only
}
```

Profiler uses `time_event` instead of `time` — consumer of the manager queue must accept **either**.

After dequeue, manager adds:

```text
payload["qdus"] = now_us - time_event
```

### 7.4 Redis Stream entry ID

```text
id = f"{time_us // 1000}-{time_us % 1000:03d}"   // conceptually ms-us
# Python: str(time_event)[:-3] + "-" + str(time_event)[-3:]
```

Must be monotonically increasing **per stream**.

### 7.5 XADD

```text
XADD stream id MAXLEN[~] maxlen fields...
```

- `maxlen_redis` on agent: default 10000; **`0` means no MAXLEN** (unbounded).  
- Queue used for async publish also sized from `maxlen_redis` (`0` = unbounded asyncio queue).  
- On first write to a stream: create consumer groups for all `Redis.Group` members (`DATA`, `MONITOR`, `EXEC` — currently all value `"$"` as start id in code).

### 7.6 Publish pipeline (behavior)

```
domain object
  → build {stream_key_parts, time_us, payload_map}
  → format stream name
  → enqueue to in-process queue
background task:
  → dequeue
  → ensure groups
  → XADD
```

### 7.7 Consume pipeline

- `XREAD` / `XREADGROUP` with block/count.  
- DataCollector: group MONITOR; ACK after process.  
- Exec: reads REQ streams; updates last-id cursors.

### 7.8 Tick / Candle Redis payloads (exact keys)

**Tick:** `pa, qa, pb, qb, dus` (+ `qdus` after manager)  
**Candle:** `oa, ha, la, ca, ob, hb, lb, cb, volume, dus` (+ `qdus`)

DataCollector rebuilds objects by parsing stream name for `venue|symbol|tf` and injecting `Symbol` + `time` from message id.

---

## 8. LTXB binary format & merge engine

Code: `src/interfaces/simulator/dataprovider.py` — **this is the most C++-ready module.**

### 8.1 File layout

```text
offset 0: magic[4] = 'L','T','X','B'
offset 4: version u8 = 4
offset 5: row0 | row1 | ...   (no padding between rows)
```

- Little-endian.  
- **No kind byte in file** — schema selected by caller/path.  
- Truncated trailing bytes ⇒ error.

### 8.2 Dtype codes (internal)

| Name | struct fmt | C++ |
|------|------------|-----|
| i64 | `q` | int64_t |
| i32 | `i` | int32_t |
| f32 | `f` | float |
| f64 | `d` | double |

### 8.3 Paths under `Config.FOLDER_DBIN` (default `<repo>/dbin`)

```text
dbin/{venue}/{symbol}.bin              # ticks (tf == T1)
dbin/{venue}/{symbol}_{TF}.bin         # candles e.g. BTCUSDT_M1.bin
```

### 8.4 Row tuples from generators

```text
Tick:   (TickType, time_us, venue, symbol, pa, qa, pb, qb)
Candle: (CandleType, time_us, venue, symbol, tf, oa, ha, la, ca, ob, hb, lb, cb, volume)
```

`gen(..., progress=False)` by default (no tqdm on hot merge path).

### 8.5 csv_to_bin

- Header must equal SCHEMA column names in order; first column `time`.  
- Convert with schema dtypes; pack rows; write header magic+version then body.

### 8.6 DataReader merge (k-way)

**Sources:** one iterator per `(symbol × timeframe)` requested.

**Heap entry:**

```text
(event_us, venue, symbol, kind, tf_period_us, src_idx, row_tuple)
kind: 0=Tick, 1=Candle, 2=other
```

Candle `event_us = (time_us / period) * period + period`  // close time

**Algorithm:**

```
prime: pull one valid head from each source into heap
loop:
  pop min heap entry
  emit process(row)          // build Tick/Candle here
  pull next from that src_idx (skip rows outside [since_us, until_us])
if only one source: skip heap; process sequentially
```

**Window filter:** on raw `time_us` (file timestamp), not candle close.

### 8.7 process(row) → object

- Lookup `Symbol` from `(venue, symbol)` map (may be null for DataPoint path).  
- Build Timestamp from µs; construct Tick/Candle with positional fields.  
- Candle: `volume = int(volume_f32)`; TF from cached enum.

### 8.8 Performance notes (observed)

- Decode-only gen ≈ ~1e6 ticks/s (Python).  
- Full merge+construct ≈ ~8e4–9e4/s.  
- Redis XADD path ≈ ~3e3/s (I/O bound).  
C++ should smash the first two; Redis remains network-bound unless pipelined.

---

## 9. ClickHouse

### 9.1 Tables

| Table | Engine notes (from `utils/db_create.py`) |
|-------|------------------------------------------|
| `history_ticks` | MergeTree, ORDER BY (time, venue, symbol), TTL 30 days |
| `history_candles` | MergeTree, ORDER BY (time, venue, symbol, tf), TTL 365 days |

### 9.2 history_ticks columns (DDL sketch)

```text
venue LowCardinality(String)
symbol LowCardinality(String)
time DateTime64(6, 'UTC')
pa, qa, pb, qb Float64
pma, qma, pmb, qmb Float64    # present in DDL; may be unused by current Tick payload
dus Int32
```

### 9.3 history_candles columns

```text
tf LowCardinality(String)
venue, symbol LowCardinality(String)
time DateTime64(6, 'UTC')
oa,ha,la,ca,ob,hb,lb,cb Float64
volume UInt64
dus Int32
```

**Action for C++:** dump live `DESCRIBE TABLE` — DDL sketch may drift from production.

### 9.4 Writers

DataCollector batches rows from Tick/Candle queues; uses ClickHouse manager insert path (`@to_table` in Python ≡ “buffered insert helper”).

### 9.5 Readers

`TSDBReader` builds SQL:

```sql
SELECT * FROM {ticks|candles}
WHERE ({venue/symbol clauses})
  [AND (tf IN (...))]
  AND time >= ... AND time <= ...
ORDER BY time, venue, symbol[, tf] ASC
```

Status: **WIP / fragile** (symbol dict iteration bugs in `common_query_builder`). Prefer LTXB for proven replay until fixed.

---

## 10. Postgres

### 10.1 Tables (known)

**`connectors`** (sketch):

```text
name PK, url_ws, url_api, active, debug, maxlen, freq_report,
last_written, last_updated, symbols JSON
```

**`symbol_specs`:**

```text
id PK, venue, symbol, quote, base,
min_stops_diff, min_price_diff, min_order_size, expiration
```

**`monitoring`:** DataCollector config (fields mirror agent lowercase attrs).

**`accounts`:** referenced by exec/WS code.

### 10.2 Hot reload

1. Trigger on config table → `pg_notify(channel, json)`.  
2. Agent LISTENs; on notify, `SELECT` row; apply fields; call `reconfig(sources)`.  
3. Data connectors compute subscribe/unsubscribe diffs (`_sources_new` / `_sources_old`).

### 10.3 Symbol query helpers

```text
ALL:   no extra clause
REGEX: AND (symbol ~ '(a|b|…)')
ARRAY: AND (symbol IN ('a','b',…))
```

---

## 11. Agent process model

Code: `src/models/agent.py`, entry `src/main.py`.

### 11.1 Capabilities to recreate (interfaces, not inheritance porn)

```text
IRunnable
  active: bool
  setup() -> list of tasks/threads
  start()  // run until failure/cancel; set active false in finally

ICron
  map[callback -> period]
  loop: sleep until ceil(now, period); invoke; log errors; while active

IStreaming
  stream_prefix, stream_midfix
  stream_format templates per message type
  start redis publisher worker; wait until ready

IControllable
  load config from Postgres table
  listen NOTIFY; reconfig
  load symbol_specs into map
```

### 11.2 Task bags

- `_crons`: periodic jobs (resample S1, scan streams, batch write, redis report, …)  
- `_procs`: long-running loops (main consumer, listen_orders, WS readers, …)  
- StreamingAgent also runs Redis publisher as a task.  
- ControllableAgent also runs Postgres listener task.

### 11.3 CLI registration

```text
ltx <agent-name>
```

Names built as `{parent}-{folder}-{venue_lower}` for connectors, e.g.:

```text
connectors-data-binanceusdm
connectors-data-binancecoin
connectors-data-binancespot
connectors-data-polymarket
connectors-exec-polymarket
monitoring-datacollector
```

Dict merge: connectors data + exec + venues misc + monitoring.

### 11.4 systemd

`services/ltx-agent@.service` — template for running named agents under systemd.

---

## 12. Connectors (live venues)

Code: `src/connectors/base.py`, `ws.py`, `venues/*`, `data/*`, `exec/*`.

### 12.1 Venue

- `VENUE` class string (auto from class name if not hardcoded: strip `Data`/`Exec` prefixes).  
- Credentials via Vault path `creds/<VENUE>` or interactive getpass; structure venue-specific NamedTuple.

### 12.2 Connector (common)

- Extends controllable streaming agent.  
- `sources: set[str]` from DB (symbols or account ids).  
- Owns `StreamingBundle(maxlen=60)` + cron resample every S1.  
- Tracks clock offset `_offset` venue vs local (optional `update_timediff`).  
- `reconfig`: diff sources → subscribe/unsubscribe maps; `update_specs(venue, sources)`.

### 12.3 DataConnector

- Midfix `DATA`, group DATA.  
- `local_to_stream`: for each symbol, stream for `T1` and **every** TimeFrame name (pre-create groups).  
- WS loop: parse venue messages → Tick → (publish + bundle.on_tick).

### 12.4 ExecConnector

- Midfix `EXEC`, group EXEC.  
- Maps: `_uid_to_acc`, `_uid_to_eid` (bidict local UID ↔ exchange EID), `_accounts`.  
- `listen_orders`: XREAD REQ streams; `process_order`.  
- Publish Response / Balance.  
- Reject via `ExecConnector.Reject`.

### 12.5 Implemented agents (status)

| Agent | Role | Notes |
|-------|------|-------|
| DataBinanceUsdm/Coin/Spot | market data | raw Binance WS/REST JSON |
| DataPolymarket | market data | Gamma REST + market WS |
| ExecPolymarket | trading | uses Python `polymarket` / py-clob SDKs — **reimplement HTTP/signing in C++** |
| Binance exec | present in tree | **not registered** in exec `__init__.py` |

### 12.6 WebSocket helper

`src/connectors/ws.py`: reconnect, subscribe, dispatch — recreate with any C++ WS client (Boost.Beast, etc.).

---

## 13. Simulator / DataProvider

Code: `src/interfaces/simulator/dataprovider.py`.

### 13.1 DataProvider agent

Fields:

```text
reader_mode: "file" | "tsdb"
time_since, time_until
timeframes: set[str]   # includes "T1" and/or "M1", …
symbols: map[(venue,symbol) -> Symbol]
```

On init:

- `stream_prefix = "BTX-" + base36(time)`  
- Construct FileReader or TSDBReader  
- StreamingAgent post-init for formats  

`main` (behavior): async iterate reader; yield each quote to Redis publisher.

### 13.2 FileReader construction

For each symbol × each tf in `timeframes`: attach `gen(venue, symbol, tf)`.  
Then DataReader merge.

### 13.3 Tests (behavioral oracles)

```bash
python -m src.test simulator/filereader      # csv→bin→gen correctness + throughput
python -m src.test simulator/dataprovider    # merge drain; Redis XADD end-to-end
python -m src.test models/bundle             # MTF resample oracle
```

### 13.4 ExecReceiver / fake exchange

Stub only (`execreceiver.py`). Roadmap: simulated matching against replayed quotes.

---

## 14. Monitoring / DataCollector

Code: `src/monitoring/infra/datacollector.py`.

### 14.1 Config knobs (Postgres-driven)

```text
maxlen_local, batch_size, freq_write_batch, freq_write_report,
freq_scan, tfs (string e.g. "S1 M1"), maxlen_redis, debug, …
```

### 14.2 Main loop

1. Cron `scan`: SCAN Redis keys matching prefix; keep those whose suffix ∈ configured TFs ∪ `{T1}`; ensure groups; mark NEW.  
2. Wait until first scan done.  
3. Loop XREAD → `process` → ACK.  
4. Cron flush queues → ClickHouse.  
5. Cron report aggregation stats.

### 14.3 process(stream, id, payload)

```text
parse stream: prefix|mid|venue|symbol|tf
time from message id (ms-us split)
if tf in candle tfs → Candle(**payload, tf=…)
elif tf starts with T → Tick(**payload)
enqueue to local asyncio.Queue (bounded)
```

---

## 15. Orders & execution

Code: `src/models/order.py`.

### 15.1 Order create

```text
ACTION = "create"
symbol, size, price=None, comment, expiration, mode=GTC|IOC, price_sl, price_tp
side = BUY if size>=0 else SELL
type = MARKET if price is None else LIMIT
UID = base36(time_us)          # unique-ish local id
assert size >= symbol.min_order_size
expiration clipped by symbol.expiration
check_expired(now)
check_stops(price_current?)    # SL/TP side-consistent vs entry/current
```

`Type` enum values: MARKET=0, LIMIT=-1, STOP=+1 (used with price sign logic in `check_type`).

**Note:** `Side` enum literals in source look suspicious (both -1); **trust `__post_init__` size sign**, not the IntEnum names, until fixed.

### 15.2 Modify / Delete

```text
OrderModify: ACTION=modify, UID, optional price/sl/tp/expiration/mode
OrderDelete: ACTION=delete, UID
from_order helpers: TODO stubs
```

### 15.3 Response

Built from an Order + venue ack fields (`status`, `EID`, fill size/price, `time_place`).  
Stream: ORDERS template.

### 15.4 Exec loop (conceptual)

```
XREAD REQ for each account
decode action create|modify|delete
map UID ↔ EID
send to venue
publish Response (+ Balance updates)
```

---

## 16. Config, credentials, runtime

### 16.1 Config

`config.json` overlays `Config` NamedTuple:

```text
USER, TEST, SESSION_NAME, LOG_TO_FILE, LOG_TO_LDB,
FOLDER_ROOT, FOLDER_DBIN, TIMEZONE
```

Defaults safe if file missing.

### 16.2 Auth / Vault

- `auth.ini` optional local overrides.  
- Vault KV for infra passwords and venue creds (path `creds/<VENUE>`).  
- **Assume Vault/Docker already operable** in your environment; C++ needs equivalent secret injection (env/files), not necessarily Docker inspect.

### 16.3 Logging

loguru → stdout/file; optional Loki. C++: any structured logger; keep agent name in fields.

### 16.4 Event loop

Python creates a dedicated `EventLoop` at import for Redis/PG/CH clients.  
C++: one io_context / thread pool; don’t mix loops carelessly.

---

## 17. CLI, tests, packaging

```bash
ltx <agent-name>                 # src/main.py
ltx-test / python -m src.test <suite>
```

Suites registered in package `__init__` dicts (`model_tests`, `simulator_tests`).

`pyproject.toml` defines console scripts and Python deps — for C++ rebuild, deps become libraries (hiredis, libpq, clickhouse-cpp, OpenSSL, etc.).

---

## 18. What is NOT implemented

Do **not** invent these from amnesia; they are roadmap holes:

| Item | Evidence |
|------|----------|
| Strategy framework / StateStrategy | empty `src/framework/`, `src/strategies/` |
| Fake exchange / backtest exec+manager+reporting | README dates; stubs |
| Order router, alerting, dashboards | README |
| Many venues (IBKR, Rofex, …) | README |
| MetaTrader Redis controller | comment stub `interfaces/controllers/metatrader.py` |
| OrderModify/Delete.from_order | `...` TODO |
| Robust TSDBReader | broken query builder sections |
| DataProvider as registered `ltx` agent | class exists; wire into agents dict if desired |

---

## 19. C++ rebuild blueprint

**Before Phase A:** re-read §0 (style) and mirror the `src/` folder layout. Wrong style or flattened modules = reject the work.

### Phase A — Core (week-scale)

1. `time_us`, TimeFrame table, Symbol, Account  
2. Tick, Candle (+ on_tick / on_candle_lower)  
3. Bundle + resample (validate vs `TestBundle`)  
4. LTXB read/write + mmap gen  
5. DataReader k-way merge  
6. Unit tests with golden bins

### Phase B — Bus

1. Redis XADD/XREAD/XGROUP wrapper  
2. Frozen message codec (msgpack or Redis hash fields matching §7)  
3. Publisher worker + consumer helpers  
4. DataProvider CLI: replay files → Redis  

### Phase C — Persistence

1. ClickHouse batch insert (ticks/candles)  
2. DataCollector equivalent  
3. Postgres config load + NOTIFY (optional hot reload)  
4. symbol_specs cache  

### Phase D — Live data

1. Binance WS JSON → Tick → Redis (+ Bundle)  
2. Polymarket data path  
3. Clock offset handling  

### Phase E — Exec

1. Order model + validation  
2. REQ consumer  
3. Polymarket REST/signing (from venue docs, not Python SDK)  
4. Response/Balance publish  

### Phase F — Greenfield

Strategy API, fake exchange, router, alerts — design fresh.

### Suggested layout

Mirror this Python tree (names matter):

```text
ltx-cpp/   (or continue under a C++ src root)
  src/
    models/          # tick, candle, bundle, symbol, timeframe, order, agent
    utils/           # config, redis, postgres, clickhouse, log
    connectors/      # venues/, data/, exec/, ws
    interfaces/
      simulator/     # dataprovider, filereader, merge
      controllers/
    monitoring/
      infra/         # datacollector
    framework/
    strategies/
  docs/
    LETTER_TO_SELF.md
  dbin/
  services/
  tests/golden/
  tools/             # optional CLIs (ltx_replay, …) — like top-level utils/
```

Headers may be `*.hpp` beside sources or under `include/ltx/...`, but **keep the same package folders**.

Every new `.cpp` / `.hpp` must follow §0 (spaces, banners, overlines, K&R braces, vertical rhythm).
---

## 20. Sufficiency checklist

### With this letter + this repository alone

You can rebuild **~90–95% of existing capabilities** in C++:

- Domain math, Bundle/MTF, LTXB, merge, Redis bus behavior, DataProvider replay, DataCollector job, Binance-style data connectors, agent lifecycle, order validation model.

**And** you must apply §0 style + mirrored `src/` layout, or the rebuild is not acceptable even if behavior matches.

### Still fetch outside the two items

| Need | Why |
|------|-----|
| Production secrets / endpoints | Not in git |
| Live `DESCRIBE` for CH/PG | DDL sketches may drift |
| Polymarket trading API docs | SDK hides details |
| `LTX-MT5` | only if MT5 in scope |
| Golden `.bin` fixtures | optional; regenerate from tests |
| Product decisions for stubs | strategy/fake exchange undefined |

### Explicitly do **not** need

- Decorator/metaclass replication  
- pandas  
- Vault Docker discovery code (if secrets provided another way)  
- Empty framework packages  

---

## 21. File index (open these)

| Priority | Path | Why |
|----------|------|-----|
| 0 | `docs/LETTER_TO_SELF.md` | this map |
| 1 | `src/models/data.py` | Tick/Candle/Balance contracts |
| 2 | `src/models/misc.py` | Symbol, Account, TimeFrame set |
| 3 | `src/models/bundle.py` | MTF algorithms + TestBundle |
| 4 | `src/interfaces/simulator/dataprovider.py` | LTXB + merge + DataProvider + tests |
| 5 | `src/utils/clients.py` | Redis/PG/CH managers |
| 6 | `src/models/agent.py` | process model |
| 7 | `src/models/order.py` | orders |
| 8 | `src/connectors/base.py` | connector lifecycle |
| 9 | `src/connectors/ws.py` | WS loop |
| 10 | `src/connectors/data/binance.py` | live crypto data |
| 11 | `src/connectors/data/polymarket.py` | live PM data |
| 12 | `src/connectors/exec/polymarket.py` | live PM exec (SDK) |
| 13 | `src/monitoring/infra/datacollector.py` | archive path |
| 14 | `utils/db_create.py` | schema sketches |
| 15 | `src/main.py` | CLI |
| 16 | `src/utils/base.py` | Config / TZ / creds hooks |
| 17 | `README.md` | roadmap / non-goals |
| 18 | `pyproject.toml` | Python deps → C++ lib mapping |

---

## Appendix A — Redis field cheat sheet

```text
Tick XADD fields:     pa qa pb qb dus [qdus]
Candle XADD fields:   oa ha la ca ob hb lb cb volume dus [qdus]
Envelope (internal):  stream, time|time_event, payload
```

## Appendix B — LTXB cheat sheet

```text
Header: LTXB | u8=4
Tick row:   q f f f f     (24 bytes)
Candle row: q f f f f f f f f f   (40 bytes)
Paths: dbin/V/S.bin | dbin/V/S_TF.bin
```

## Appendix C — Merge heap cheat sheet

```text
key = (event_us, venue, symbol, kind, tf_us, src, row)
Tick event_us = time_us
Candle event_us = floor(time_us,tf)+tf
kind: Tick=0 < Candle=1
```

## Appendix D — Agent name cheat sheet

```text
connectors-data-binanceusdm
connectors-data-binancecoin
connectors-data-binancespot
connectors-data-polymarket
connectors-exec-polymarket
monitoring-datacollector
```

## Appendix E — Intentional Python-isms you may delete

- `@Redis.stream`, `@Redis.profiler`, `@Postgres.on_table_*`, `@ClickHouse.to_table`  
- `TimeFrameMeta`, `DBClassMeta`  
- Overriding `__dict__` for serialization (replace with explicit `to_redis()`)  
- Import-time singleton DB connects (replace with explicit `Runtime::init`)  
- `sortedcontainers` import (unused)  

## Appendix F — Style red lines (quick card)

| Rule | Never | Always |
|------|-------|--------|
| Operators | `a==1`, `x=2` | `a == 1`, `x = 2` |
| Braces (C++) | `{` on next line | `{` on same line as signature |
| Functions/classes | bare signature | `▄` overline matching span |
| Regions | wall of classes | `█` / `▀` section banners between concerns |
| Long functions | one dense snake | blank lines every ~15–25 lines of logic |
| Between functions | glued end-to-start | blank line, then overline |
| Tree layout | flatten `src/` | keep `models/`, `utils/`, `connectors/`, `interfaces/`, `monitoring/`, … |

Oracle file for tidy vertical rhythm: `src/models/bundle.py` → `TestBundle.test_resample`.

---

*End of letter. Rebuild the machine. Keep the contracts. Keep the style. Leave the dialect.*
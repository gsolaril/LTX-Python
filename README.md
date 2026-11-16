# LTX-Python — Multi-venue trading infrastructure

> **Status: internal development.** This is the Python reference implementation of the LTX trading stack. A C++ rebuild (`LTX-C++`) is in progress; see `docs/LETTER_TO_SELF.md` for the canonical design map.

**MetaTrader EA / MQL5 live in a separate repo**: [`LTX-MT5`](https://github.com/gsolaril/LTX-MT5) — not this tree.

---

## What it is

LTX is a multi-venue trading infrastructure built around long-lived, single-purpose processes called **agents**. Agents communicate over **Redis Streams**, persist history to **ClickHouse**, and keep configuration in **Postgres**. Credentials are pulled from **HashiCorp Vault**.

| Agent family | Job |
|--------------|-----|
| **Data connector** | Venue WS/REST → `Tick`/`Candle` → Redis `DATA` streams |
| **Exec connector** | Redis `REQ` → venue orders → Redis `ORDERS` / balance updates |
| **DataCollector** | Redis `DATA` → batch insert into ClickHouse history |
| **DataProvider** | Historical `LTXB` (or ClickHouse) → same Redis `DATA` grammar under a backtest prefix |

Shared infrastructure:

- **Redis Streams** — real-time message bus (IPC between all agents)
- **Postgres** — connector/monitoring config + `symbol_specs` table (+ accounts)
- **ClickHouse** — durable tick/candle history
- **`dbin/`** — packed little-endian market-data files (`LTXB`) for fast replay

## Key design decisions

- **Time**: everything is UTC. Internal representation is `int64` microseconds since Unix epoch.
- **Order → Trade**: LTX deliberately omits the MT5 `Deal` object. An `Order` is the submitted request; a `Trade` is the resulting persistent position.
- **Account modes**: `Account(is_hedging=False)` gives one aggregated `Trade` per symbol (netting); `Account(is_hedging=True)` gives one independent `Trade` per filled order (hedging). The account owns the policy for how filled orders become trades.
- **30 fixed timeframes** from S1 to D1 (seconds/minutes/hours/day), built from divisors of 60 and 24.
- **Deterministic simulator** (`Simulator`) that behaves like an MT5/MQL exchange: order create/modify/delete, SL/TP, frequency and size limits, hedging/netting, and a full state machine verified by 31 unit tests.
- **LTXB binary format** — packed little-endian tick/candle records for fast archive replay.

## Requirements

- Python **>= 3.11**
- Running services: **Redis**, **Postgres**, **ClickHouse**, **HashiCorp Vault** (all typically run in Docker)

## Installation

```bash
pip install -e .
```

## Configuration

- `auth.ini` — credentials for Vault and the four backends (Postgres, ClickHouse, Loki). Copy `auth.ini.example` and fill it in.
- `config.json` — runtime flags (`TEST`, `LOG_TO_FILE`, `LOG_TO_LDB`) and paths.

## Running

```bash
# List available agents
ltx

# Start an agent by name
ltx <agent-name>

# Run the test suite
ltx-test <test-name>
```

Example agents:

```bash
ltx connectors-binanceusdm
ltx connectors-binancespot
ltx connectors-binancecoin
ltx connectors-polymarketgamma
ltx monitoring-datacollector
```

## Systemd deployment

```bash
sudo ./services/install-systemd.sh

sudo systemctl start ltx-agent@connectors-binanceusdm
sudo systemctl enable ltx-agent@monitoring-datacollector
journalctl -u ltx-agent@connectors-binanceusdm -f
```

## Running tests

```bash
ltx-test models/bundle
ltx-test simulator/filereader
ltx-test <any test class name>
```

The `tests/` directory holds 31 test modules covering `Order`, `Trade`, `Account`, `Simulator`, `DataReader`, `Bundle`, and the full order lifecycle (create/modify/delete, rejections, SL/TP, netting/hedging).

## Project structure

```
src/
  models/          # Tick, Candle, Bundle, Symbol, TimeFrame, Order, Trade, Account
  utils/           # Config, Vault, Redis/PG/CH clients, logging (loguru + Loki)
  connectors/      # venues/, data/ (Binance, Polymarket), exec/
  interfaces/      # simulator/ (Simulator, DataReader), controllers/
  monitoring/      # infra/datacollector
  framework/       # strategy engine (Strategy base class, StrategyController)
docs/
  LETTER_TO_SELF.md        # canonical design map for the C++ rebuild
  LTX_Python_MT5_Model_Review_and_Redesign_Plan.md
  simulator_tests.md
```

## Roadmap

- [x] Order & Trade model review
- [x] Backtester / fake exchange — exec
- [x] Backtester / fake exchange — manager
- [x] Strategy base class
- [x] StateStrategy class
- [x] Backtester / fake exchange — reporting
- [x] MetaTrader connector revision
- [x] Polymarket exec connector (REST)
- [x] Order manager & routing
- [ ] Other connectors (Crypto, IBKR, Rofex, IQOption, DeFi)
- [ ] Alerting
- [ ] LTX-C++ (in progress)

## Documentation

- `docs/LETTER_TO_SELF.md` — read this first. It is the map; the code is the territory.
- `docs/LTX_Python_MT5_Model_Review_and_Redesign_Plan.md` — MT5-like model review and redesign.
- `docs/simulator_tests.md` — automated testing strategy and test catalogue.

## License

Internal use only.

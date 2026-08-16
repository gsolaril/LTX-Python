**To be written as proper readme later... right now I am just using this to take notes.**

**Why? Just because I want to**

---

MetaTrader EA / MQL5 live in a **separate repo**: `/home/gaston/LTX-MT5` (not this tree).

---

**New plan**

Next steps:
* (08/13) Backtester / fake exchange - exec
* (08/15) Backtester / fake exchange - manager
* (08/16) Backtester / fake exchange - reporting
* (08/23) "framework/strategy.py" - autonomous Strategy class
* (08/23) "framework/strmodels.py" - state-based StateStrategy class
* (08/30) MetaTrader exec connector (REDIS)
* (09/06) Polymarket exec connector (REST)
* (09/13) Order router
* (09/20) alerting
* (10/xx) other connectors (Crypto, IBKR, Rofex, IQOption, DeFi?)
* (12/xx) LTX-C++ (already started)

---

**Old plan**

Season 1: Connectors
  ● Episode 1: Data (for 01/16)
        - Part 4: The exchange Simulator (Tue 06)
        - Part 5: Other Binance contracts (Fri 09)
        - Part 6: Other crypto exchanges (Sun 11)
        - Part 7: MetaTrader (Mon 13)
        - Part 8: BYMA (Tue 14)
        - Part 9: ALT datasource model (Wed 15)
  ● Episode 2: Trading (for 01/20)
  ● Episode 3: Account (for 01/27)

Season 2: Framework
  ● Episode 1: Objects C++ (for 01/09)
  ● Episode 2: PyBind 11 (for 01/12)
  ● Episode 3: The engine (for 01/19)
  ● Episode 4: A strategy (for 01/24)
  ● Episode 5: Backtesting (for 01/31)
        - Part 1: E-S engagement, data
        - Part 2: E-S engagement, trades

Season 3: Management
  ● Episode 1: Dashboards
  ● Episode 2: Slack alerting
  ● Episode 3: Risk management
  ● Episode 4: Backtesting GUI
### **Automated testing strategy, part 1: Order-related & Account-related entities + Simulator.**

#### **Introduction**

 First of all, some references for instructions below.
- **"[C]"**: Conditions and context behind the test.
- **"[S]"**: situation being tested
- **"[D]"**: desired result
- **"[N]"**: notes... comments, parameter values, etc.

Please document all tests with docstrings using the C/S/D/N pattern above, and with simply worded short comments for every relevant operation/action/code line.

Also from now onwards: **ALL OF THE TESTS** should go in `.py` files in that new (and right now empty) folder called `./tests/`. The name of the file should include the prefix "`test_`" and also the name of the class being tested: "`test_order_create.py`", "`test_order_modify.py`", "`test_order.py`", "`test_account.py`", "`test_simulator.py`".

The idea is for us to include a CI/CD pipeline based on Github Actions later, so the test suit to be developed, needs to be arranged and engineered correctly. Then, we will soon no longer use "`src/test.py`" anymore for the unit tests. And of course, for those tests already added to the current version of the codebase, we will move them to "`./tests/`" as well, within their own files (e.g.: "`test_bundle.py`", "`test_data_reader.py`", etc.).

#### **The main rule of all of this**

The logic of the objects and functions involved within this testing strategy, is finished. At least laid out, but is supposed to be indeed laid out completely. However, **always use your best criteria** specially with regards to the main subject: trying to make the whole system be logically compatible with a trading engine, and specially the Simulator and Order-related methods to be analogous to a real trading venue in logic at least.

So the fundamental rule should be: the tests should assure that the system behaves as consistent as a trading ecosystem as possible. BUT it is very probable that some logic errors appear, so the testing strategy may also be seized and used to get evidence on how to improve the core codebase and logic as well. What's actually very important is that this whole report is a good way of understanding not only how to test this system, but also a good way of understanding what we should pretend from the orders, accounts and simulators to do well and correct.

In other words: the examples below (specially the "desired" results signalled with **[D]**) are to be taken as what we want to see in the ecosystem and codebase. So that if there's any need to change code and logic from the involved objects being tested, we identify what and where are the errors, and we solve them afterwards.

#### **Preliminary tests**

These tests are intended to be small and right to the point in terms of the functionality we are testing. For example: see if `OrderCreate`, `OrderModify` and `OrderDelete` entries (as well as `Reject` situations born from them); are correctly built, interpreted and handled based on Redis' messages.

You will probably need to instantiate `Tick`s, `Candle`s for this, as well as `Symbol`s and `Account`s to use as order-related arguments (for the latter 2, one single and simple instance for each is enough).

For all of them, just (**[D]**) check and assert values on instance attributes (e.g.: upon negative size, side should be sell)

Let's see...
##### `OrderCreate` basic tests
- **[S]** no (limit) `price`, no `SL`/`TP`/`execution`, negative size
- **[S]** no (limit) with valid non-zero `price`, no `SL`/`TP`/`execution`
- **[S]** no (limit) with valid non-zero `SL`/`TP`
- **[S]** no (limit) with valid non-zero `execution`
- **[S]** any test(s) you consider relevant for testing edge cases and/or reject situations. **[D]** Assert rejection content if necessary (specially the "`reason`" attribute). The more `Reject` instances coming from here being tested, the better.

##### `Order` tests
- **[S]** basically take a few `Order` instances coming from valid `OrderCreate` cases above, and test their conversion towards `Order`
- **[S]** for a generic order, and with a few `Tick`s/`Candle`s with price values wisely chosen, test the "`Order.check_filled`" method.

##### `Trade` tests
- **[S]** basically take a few `Trade` instances coming from valid `Order` cases above, and test their conversion towards `Trade`
- **[S]** for a generic trade, with a few wisely chosen `Order` instances, together with a few `Tick`s/`Candle`s with price values wisely chosen, test the different contextual "`Trade`" methods: different check-hedged situations, different check-closed, etc. This test should be actually repeated several times with different trade configurations and hedging orders, to achieve the following 4 "**possible hedge cases**"
   1. Trade size gets increased by order (e.g: trade sold 2, order sells 1)
   2. Trade size gets decreased by order (e.g: trade sold 2, order buys 1)
   3. Trade gets completely closed by order (e.g.: trade sold 2, order buys 2)
   4. Trade gets inverted by order (e.g.: trade sold 2, order buys 3)

##### `OrderModify` basic tests
- **[S]** one test per parameter change, then one test with parameters all changed using `OrderModify`.
- **[S]** instantiate an "`Order`" with some dummy parameters, then do one test per parameter change but getting the instance through using "`Order.modify`" instead of `OrderModify`.
- **[S]** use `Order.on_modify` with the previous `OrderModify` instance and assert new "`Order`" attributes
- **[S]** instantiate a "`Trade`" with some dummy parameters, then do one test per parameter change but getting the instance through using "`Trade.modify`" instead of `OrderModify`.
- **[S]** use `Trade.on_modify` with the previous `OrderModify` instance and assert new "`Trade`" attributes
- **[S]** also consider some reject-raising cases (e.g.: no parameters included, non-matching UID, etc.)

##### `OrderDelete` basic tests
Same as `OrderModify` above but with order-delete-related commands ("`OrderDelete` / `Order.delete` / "`Trade.delete`")

##### `Account` tests, order-centered

**[C]** Create a new dummy symbol similar to the ones used above. Start with a brand new account, `balance = 10k`, `leverage = 100`. Order/dicts should have an empty subdict for the given dummy symbol-key (venue-symbol string tuple). Oh and also initialize default `Rules`.
Then do the following tests:
- **[S]** Generate an `OrderCreate` that would imply a simple and immediately placeable order (no limit price, and mainly with size not big enough so as not to get margin-related rejects). Then:
- **[N]** Generate a `Tick`/`Candle` with an adequately chosen price that will be needed for the following tests.
- Test the "`check_`" functions. Of course **[D]** no reject should be returned.
- Then test the "`on_order_create`" function. Verify that **[D]** the order is successfully placed into `orders_active` and that `order_count` increases by 1
  - For the accepted `Order` instance returned by `on_order_create`, assert that the attribute values match the `OrderCreate` instance values when applicable (for those attributes present in both: `price_tp`, `price_sl`, `UID`, and others.)
- Then test the "`on_order_filled`" function. Verify that **[D]** the order ends in `orders_closed` and also generates a trade within `trades_active`, decreasing `order_count` / increasing `trade_count` by 1
  - For the produced `Trade` instance returned by `on_order_filled`, assert that the attribute values match the `Order` instance values when applicable (for those attributes present in both: `price`, `asset_value`, `UID`, and others.)
- Check the `account.time` attribute during the latter 2 tests and verify that at each moment, it turns equal to the `quote.time_event` of the most recent quote.

Now we will test edge-cases and situations that cause order-based rejects.

**[C]** Create another account, same default parameters as mentioned before, but with adequately chosen pre-loaded orders and trades. Let's say; 2 orders and 4 trades. And also this time use more restrictive `Rules` such as `rules.max_freq_us = 1000` (1 ms) and "`rules.max_orders = 4`"" so that rejects are almost triggered. Also restrict the frequency parameter to one second.
- In this first block of upcoming tests, use pending orders (non-zero limit `price`) so that they don't get filled and remain on the `orders_active` dict during the course of the tests.
  - Once again **[N]** Generate a `Tick`/`Candle` with an adequately chosen price to be needed for the following tests.
  - **[S]** As we've got 2 orders left in this situation: for the first of these new tests, let's use an `OrderCreate` with a size that's too large, and **[D]** see how calculated margin is too high and triggers a margin-related `Reject`.
  - **[S]** Now use another `OrderCreate` with a smaller, acceptable size. **[D]** Verify that it is accepted and enters into the `orders_active` attribute. Also should return its corresponding `Order` instance.
  - **[S]** Now use another `OrderCreate`. For this one let the `time` argument/attribute be explicitly provided as 1 microsecond after the previous order. This should **[D]** trigger a frequency-related `Reject`
  - **[S]** Next test; send another acceptable order. Then **[D]** expect for the `Reject` to be triggered by max amount of orders.
- In these shorter second block of upcoming tests, we will focus on `Trade`s. So use immediately placeable orders (limit `price` being None to ensure immediate execution) so that they get filled as soon as possible. We will limit the `rules.max_trades = 4` as well.
  - Once again **[N]** Generate a `Tick`/`Candle` with an adequately chosen price to be needed for the following tests.
  - **[S]** Use an `OrderCreate` so that the corresponding order is immediately filled. But **[D]** it should be rejected due to max trades being reached.

Before continuing:
- **[S]** generate an `OrderModify` instance with an invalid `UID` (e.g.: "AAAAAAAA") and use `check_order_exists` to **[D]** get a `Reject`
- **[S]** generate an `OrderModify` instance with a valid `UID` (equal to one of the orders pre-loaded on the previous batches of tests) and use `check_order_exists` **[D]** which should not return a `Reject`.

Now we will test `Order.modify` and its provided `OrderModify` instances.
**[C]** Create another account, same default parameters as mentioned before, this time fresh new and with no pre-loaded trades. No restrictive `Rules` here; use default rules. Use a first `OrderCreate`, immediately executable (`price` being None or not specified) and assert the returned `Order`. Keep note of the `UID`.
- **[S]** Use the `Order.modify` to generate an `OrderModify`. Then use it within `Account.on_order_modify` which should return the modified `Order`. **[D]** Verify that the parameters in `Order` changed according to the content on the `OrderModify`.
- **[S]** Also after this one, use `OrderModify` to instantiate one directly, and provide a bad `UID` that doesn't match the original order. **[D]** This should return a `Reject` coming from the outcome of `check_order_exists`.

Now let's test `Order.delete` and its provided `OrderDelete` instances.
Basically **[S]** repeat the two cases mentioned for `OrderModify` previously, but change the command, of course. The important thing here is to **[D]** verify whether the order/trade gets removed or not from `orders_active`/`trades_active` respectively.

##### `Account` tests, trade-centered

For the final tests, we will focus on `Trade`s and what happens in the "`account.trades_active`" book-like dict. Also we need to verify the functionality of the more complex mechanisms within `Account.on_order_filled` and `Account.on_trade_closed`. Oh and except in case it's literally mentioned, just use standard default `Rules` for all upcoming tests within this section.

**[C]** Now the `Account` should have a couple of preloaded trades within "`trades_active`". Well, actually we will run 2 small test suites:
- First with "`account.is_netting = False`"; include 4 or 5 trades for each of 2 different dummy symbols.
- After with "`account.is_netting = True`"; include 1 trade for each of 2 different dummy symbols

The reason why just one trade for each symbol within "`account.is_netting = True`"? Of course: the whole point of the "`NETTING`" account is for the trades to aggregate when sharing symbol... as the logic of "`Account.on_order_filled`", "`Account.on_trade_closed`" and "`Trade.check_hedged`". So when analyzing any of these **[D]** we need to ensure that the number of active trades per symbol within a netting account, is always ONE.

Also for all tests that provoke a change in the "`trades_active`" book dict, remember to measure the account "state" variables (mostly balance, uPNL, rPNL, GAV, NAV, `order_count` and `trade_count`)...
- First predicting the future values before the changing events,
- Then comparing the resulting values post-events with those predictions
(**[D]** of course; they should match)

Also mind that for each one of these upcoming tests, we need to provide the functions we will test ("`account.on_order_filled`" and "`account.on_trade_closed`") with quotes; `Tick`s and `Candle`s. Actually we need to do 2 batches of tests; ones with ticks and the other ones with `Candles` (**[D]** to assert that the "`[Tick|Candle].mkt_price`" works OK as well).

So now below, the actual tests will be described.
- **[S]** No SL/TP in the active trades. For 1 symbol, use "`OrderCreate`" to place an immediate execution order without TP/SL, and verify that after "`account.on_order_filled`" it accurately turns into a `Trade` within "`account.trades_active`". Assert values of account state variables (specially rPNL, GAV and NAV)
- **[S]** Iterate over the `Tick`/`Candle` test array. Cycle over the `trades_active` dict and update the state of each trade with its corresponding quote (same symbol, of course) and using `Account.on_trade_closed`. **[D]** Assert account state variables and trade count after each price. (although no trade should close by themselves in this step because none of them has SL/TP)
- **[S]** At the end of the previous test, use an `OrderDelete` for one of the trades (in case of netting account, of course, the only one) and apply it to "`account.on_order_delete`". **[D]** Should close the trade, update the account state variables towards the predicted values and the trade should be moved from `account.trades_active` to `account.trades_closed`. Please assert all of this.

- **[S]** Now let's do something: get one of the leftover trades in "`trades_active`". Use `Trade.modify` to generate an `OrderModify` instance changing the SL (`price_sl`). Apply such `OrderModify` to `account.on_order_modify` and **[D]** assert values to make sure that the trade's parameters actually changed as intended.
- **[S]** Now use a few `Tick`s/`Candle`s, choosing the price sequence wisely. It shall force the trade to close in loss due to touching SL. Make sure that **[D]** the status changes to "`Trade.Status.SL`".
- **[S]** Within this context, use "`account.on_trade_closed`" so that the SL-closed trade gets processed. **[D]** should have been moved from "`trades_active`" to "`trades_closed`", and the account state variables should have been updated.
- **[S]** After finishing with the aforementioned SL-related tests, do the exact same tests but with TP involved. Of course use a wise sequence of `Tick`s/`Candle`s to make the trade to close in TP. **[D]** The results should be the same, or at least quite analogous; albeit in profit context.

- **[S]** Finally, we will test the impact of hedging orders/trades to active trades, in the same 4 "**possible hedge cases**" (spoken about during the `Trade` class tests previously). I'll repeat such cases here for the sake of clarity: 
  1. Size of active trade increased, 
  2. Size of active trade decreased to non-zero, 
  3. Size of active trade decreased to zero (trade closed by hedging)
  4. Size of active trade inverted (size of opposing hedger trade larger than the one of active trade)
We will use very short `Tick`/`Candle` sequences, as well as `OrderCreate` instances of immediate execution (limit `price` being None) within `on_order_filled` to create hedger trades. Then we use "`on_trade_closed`" for the resulting hedger trade.

**[N]** One very important thing for these hedging tests, is to make sure that the originally active trade keeps its own `UID` as per the `Trade.check_hedged` logic. Notice the "`FIXME: WHY?`" string within the "`Trade.decrease_pos`" method, when UID/EIDs are reassigned. I may have made a mistake. But **[D]** what needs to happen, is:
  1. When size of active trade increased, active trade needs to keep its UID/EID.
  2. When size of active trade decreased and non-zero, active trade keeps its UID/EID as well.
  3. When size of active trade decreases to zero (closed-hedged), active trade keeps its UID/EID as well.
  4. When active trade is inverted, active trade needs to now have the hedging trade's UID & EID because the trade that determines the new (inverted) position is the one with the different side.

#### **Situational tests**

**NOTE: FOR NOW, don't pay attention to these tests, they are still in development and planning.** 

Create different scenarios testing different things of the Simulator. Basically 20-30 situations with 2-4 different configs each... which would in total, return 50-100 unit tests / subtests. From simpler to more complex. We need to be very conceptually consistent with the inner workings of an exchange/broker/trading platform and test stuff. Some preliminary general conditions:
- Make the `Simulator` use the `FakeReader` (`simulator.reader_mode = FAKE`)
- Use only `FakeSymbolLinear` instances as `symbols`. Being predictable enough, it should be very easy to predict the correct test results and assert/compare with the outcomes.
- For simple tests that require one symbol, only instantiate one. For more complex tests that are explicitly stated to require multiple symbols, use two. But never more than two.
- Same for the set of the `timeframes` set-typed attribute: let's keep it simple and only use T1, S1 and M1.

Also something **very important**: let the test take the additional role of being the strategy-like actor in here. In case the test involves an order-based message (`OrderCreate`, `OrderModify`, etc.) to be sent to the `Simulator` instance:
- **let the test itself send the order-based message through "`Redis.xsend`" or whatever command is needed**.
- After a few milliseconds after the message was sent, let the test also use `Redis.xread` after the message was sent, to collect the response that the `Simulator` provided, through the correct x-stream channel.
- The `Simulator` is intended to always return messages containing the payloads of `Order`, `Trade` or `Reject` types. So once the test reads the response, it should be able to very well instantiate the according object using the received payload as arguments.
- Notice that in the following tests, modify or delete operations are to be done after successful `Order`s/`Trade`s having been created. So the test should always be able to use "`Order.modify` / `Order.delete` / `Trade.modify` / `Trade.delete`" methods over the aforementioned instantiated objects.

##### **Situation #0**
- **[C]** Very short-lived test (generate a couple of minutes), single account with default starting conditions for state variables (balance, leverage, etc) and default rules.
- **[S]** `Simulator` doesn't receive any message
- **[D]** All order books empty

##### **Situation #1**
- **[C]** Very short-lived test (generate a couple of minutes), single account with default starting conditions for state variables (balance, leverage, etc) and default rules.
- **[S]** `Simulator` receives an `OrderCreate` with immediate placement (price = 0) with size 1, no `SL`/`TP`/`expiration`. Some "`n_ticks`" ticks later, it receives an `OrderDelete` to close that trade. 
- **[D]** The order shall execute immediately as trade and be moved to `trades_active`. Afterwards, the delete request shall close the trade and move it to `trades_closed`. Price difference and PNL shall be equal to the one implied by the price movement. Verify correctness and validity in the change(s) within the `Account`'s state variable values in balance, equity, GAV, NAV, margin and margin level at each moment (before order creation, after order placement, before reception of delete request, and after confirmation of delete request)
- **[N]** Price movement over such number of ticks needs to be kept as record so that PNL can later be calculated and compared to trade's result.

##### **Situation #2**
- **[C]** Very short-lived test... (same as ones before)
- **[S]** Simulator receives an `OrderCreate` with immediate placement (price = 0) with size 1, and `SL` below. Nothing else
- **[D]** The order shall execute immediately, then close due to `SL`. Should be a loss: compare with price movement and verify that PNL is negative and on the right value. Also of course verify that the trade has been correctly closed and moved to `trades_closed`.
- **[N]** Let `SL` be a small amount. Let's say 1% of "`p_amp`" (price sine amplitude), in the direction of the price (if the price is going up, sell).

##### **Situation #3**
...same as Situation #2 but with `TP` instead of `SL`.

##### **Situation #4 and #5**
...same as situation #2 and #3 but let an `OrderModify` land a few minutes later, doubling the `SL`/`TP` distance.
- **[D]** outcome should be the same as before, but also verify that the `SL`/`TP` has correctly been modified.

##### **Situation #6**
- **[C]** Very short-lived test... (same as ones before)
- **[S]** Simulator receives an `OrderCreate` with a desired execution price 1% of amplitude on the direction of the current price. Order should be a limit order. No `SL`/`TP`. Right after execution (order turns into trade), Simulator should receive an `OrderDelete` to close the trade.
- **[D]** Order creation should happen: order should be added to account's `orders_active` and to simulator's own orders' book.
- **[N]** For order to be limit order: if price is ascending, order should sell (negative `size`). If price is descending, order should buy (positive `size`).

##### **Situation #7**
- **[C]** 
- **[S]** 
- **[D]** 
- **[N]**

#### **Execution agreements and additional testing guidance**

The following agreements apply to the implementation of this testing strategy:

- **[C]** The repository currently contains simulator-related tests in `src/interfaces/simulator/tests.py`, while the intended test-suite root is `./tests/`.
  - **[D]** Move the existing simulator tests into `./tests/test_simulator.py`. The `src/interfaces/simulator/` folder should contain implementation code, not the maintained unit-test suite.
- **[C]** Several test modules will need the same symbols, accounts, timestamps, quotes, and small order/trade construction helpers.
  - **[D]** Add shared fixtures or helper functions where they improve readability and reduce duplication. Keep helpers small, explicit, and located in a suitable shared test module rather than hiding test intent behind excessive abstraction.
- **[C]** The project already uses Python's built-in `unittest` library, and it is the preferred testing interface for this repository.
  - **[D]** Use `unittest.TestCase` and its assertion methods for all tests. The suite should be runnable through unittest discovery, for example with `python -m unittest discover -s tests -p "test_*.py"`.
- **[C]** Every test should explain its context, situation, desired result, and relevant notes using the vocabulary established at the beginning of this document.
  - **[D]** Add a concise C/S/D/N docstring to each test. Use short comments only for non-obvious setup, calculations, or transitions; comments on every individual line are not required.
- **[C]** Trading behavior must be tested according to coherent exchange or broker semantics rather than only according to the current implementation.
  - **[D]** Give priority to exact semantic assertions: bid/ask selection, market and limit order behavior, limit fill boundaries, stop-loss and take-profit touching behavior, PNL sign and amount, account state transitions, order/trade counts, and UID/EID preservation during hedging.
- **[C]** The first implementation phase concerns order, trade, and account entities and their direct methods. The situational simulator tests are explicitly still under development.
  - **[D]** Disregard the entire `Situational tests` section for now. Do not add simulator integration scenarios, Redis message round trips, `FakeReader` cases, or the unfinished situations until that section is specified further.
- **[C]** Initial test execution may expose defects in the production logic, and those failures are useful evidence rather than reasons to weaken the assertions.
  - **[D]** Keep desired trading behavior expressed in the tests. When a test reveals an implementation defect, identify and fix the production root cause in a separate, focused change, then rerun the relevant unittest module.

##### **Production behavior to verify while implementing the tests**

- **[C]** `AccountState` defines the margin percentage as margin divided by equity, with danger increasing as the value approaches `1`.
  - **[D]** Review `Account.check_margin` so its future percentage uses equity as the denominator and rejects only when the resulting value exceeds the configured limit. Add tests that cover both an accepted margin and a margin rejection.
- **[C]** The maximum-order rule should reject a new order when the account has already reached its configured maximum.
  - **[D]** Review `Account.check_num_orders` and verify the boundary condition carefully. Add tests for the count immediately below, exactly at, and above `rules.max_orders`.
- **[C]** A trade may define only an SL, only a TP, both, or neither.
  - **[D]** Review `Trade.on_close` so an absent SL does not disable TP evaluation, and an absent TP does not disable SL evaluation. Add separate SL-only, TP-only, both, and neither tests.
- **[C]** SL, TP, and expiration are removable optional order parameters, while other modification values may have their own valid non-`None` domain.
  - **[D]** Update modification handling so SL, TP, and expiration can explicitly be set to `None`. Add tests for changing each value, removing each value, and rejecting a modification with no effective changes.
- **[C]** Hedging can increase, reduce, close, or invert a position.
  - **[D]** Add separate tests for all four cases and assert the active trade's UID/EID rules described above, including the inversion case where the opposing trade becomes the position-defining trade.

##### **Expected test module layout**

The preliminary suite should be organized into the following unittest modules under `./tests/`:

- `test_order_create.py`
- `test_order.py`
- `test_trade.py`
- `test_order_modify.py`
- `test_order_delete.py`
- `test_account.py`
- `test_simulator.py` (existing file-backed simulator tests only; situational tests remain excluded)

#### **Decisions from follow-up discussion**

The following decisions refine the execution agreements above and should be treated as part of the testing contract:

- **[C]** A value that reaches an imposed limit has crossed the relevant boundary for this trading context.
  - **[D]** Boundary comparisons for SL, TP, limit-price, and trade-size rules must be inclusive. Use `<=` or `>=` as appropriate so equality with the configured limit triggers the expected result.
- **[C]** `Tick.mkt_price` now exposes the corrected market-price behavior, but opening and closing a position use opposite sides of the market.
  - **[D]** A buy order executes at ask. Closing that buy trade executes by selling at bid. A sell order executes at bid. Closing that sell trade executes by buying back at ask. Tests must assert these conventions independently for ticks and candles where applicable.
- **[C]** An order whose timestamp is earlier than the account timestamp represents an old order that never entered the account.
  - **[D]** Such an order must be invalidated immediately and must not be added to active orders, closed orders, or any simulator/account bookkeeping. Add a dedicated test that verifies complete discard and confirms that no account counters or state values change.
- **[C]** Direct model behavior and account lifecycle behavior have different responsibilities and should be diagnosable independently.
  - **[D]** Keep construction, conversion, validation, fill, stop, PNL, and hedging tests in the order/trade modules. Keep order-book movement, account counters, margin, balance, PNL, GAV, NAV, and lifecycle reconciliation tests in the account module. Use the simulator module only for the already-existing file-backed tests during this phase.
- **[C]** Shared test infrastructure will be used by multiple unittest modules.
  - **[D]** Helper modules may be added under `./tests/` for deterministic symbols, accounts, timestamps, ticks, candles, and case-generating contexts. Helpers should make setup consistent without concealing the behavior each test is asserting.
- **[C]** A failing test is evidence about the implementation and should provide enough context to locate the cause.
  - **[D]** Preserve intended behavior in assertions and include concise diagnostic output through existing Loguru logging or targeted test messages when useful. Do not weaken an assertion merely to match an existing defect.
- **[C]** An immediately executable order has no explicit entry price, so its effective price is determined by the most recent quote.
  - **[D]** Before margin validation and trade creation, use the current `Tick` or `Candle` quote to derive the effective execution price: ask for a buy and bid for a sell. The resulting price must drive the order's margin contribution, trade entry price, and subsequent PNL calculations.
- **[C]** The test suite should be implemented incrementally while monitoring the production behavior exposed by each focused test module.
  - **[D]** Start with direct order creation and validation, then order conversion/fills, trade behavior, modification/deletion, and finally account lifecycle tests. Run the relevant unittest module after each implementation slice and record confirmed production fixes separately from test-only changes.
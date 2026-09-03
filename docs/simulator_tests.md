#### **Notes**

 First of all, some references for instructions below.
- **"[C]"**: Conditions and context behind the test.
- **"[S]"**: situation being tested
- **"[D]"**: desired result
- **"[N]"**: notes... comments, parameter values, etc.

Please document all tests with docstrings using the C/S/D/N pattern above, and with simply worded short comments for every relevant operation/action/code line.

Also from now onwards: **ALL OF THE TESTS** should go in `.py` files in that new (and right now empty) folder called `./tests/`. The name of the file should include the prefix "`test_`" and also the name of the class being tested: "`test_order_create.py`", "`test_order_modify.py`", "`test_order.py`", "`test_account.py`", "`test_simulator.py`".

The idea is for us to include a CI/CD pipeline based on Github Actions later, so the test suit to be developed, needs to be arranged and engineered correctly. Then, we will soon no longer use "`src/test.py`" anymore for the unit tests. And of course, for those tests already added to the current version of the codebase, we will move them to "`./tests/`" as well, within their own files (e.g.: "`test_bundle.py`", "`test_data_reader.py`", etc.).

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
- **[S]** for a generic trade, with a few wisely chosen `Order` instances, together with a few `Tick`s/`Candle`s with price values wisely chosen, test the different contextual "`Trade`" methods: different check-hedged situations, different check-closed, etc. This test should be actually repeated several times with different trade configurations and hedging orders:
   - Trade size gets increased by order (e.g: trade sold 2, order sells 1)
   - Trade size gets decreased by order (e.g: trade sold 2, order buys 1)
   - Trade gets completely closed by order (e.g.: trade sold 2, order buys 2)
   - Trade gets inverted by order (e.g.: trade sold 2, order buys 3)

##### `OrderModify` basic tests
- **[S]** one test per parameter change, then one test with parameters all changed using `OrderModify`.
- **[S]** instantiate an "`Order`" with some dummy parameters, then do one test per parameter change but getting the instance through using "`Order.modify`" instead of `OrderModify`.
- **[S]** use `Order.on_modify` with the previous `OrderModify` instance and assert new "`Order`" attributes
- **[S]** instantiate a "`Trade`" with some dummy parameters, then do one test per parameter change but getting the instance through using "`Trade.modify`" instead of `OrderModify`.
- **[S]** use `Trade.on_modify` with the previous `OrderModify` instance and assert new "`Trade`" attributes
- **[S]** also consider some reject-raising cases (e.g.: no parameters included, non-matching UID, etc.)

##### `OrderDelete` basic tests
Same as `OrderModify` above but with order-delete-related commands ("`OrderDelete` / `Order.delete` / "`Trade.delete`")

##### `Account` tests
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

For the final tests, we will focus on verify the more complex mechanisms within `Account.on_order_filled` and `Account.on_trade_closed`.

#TODO: COMPLETE THIS

#### **Situational tests**

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
- **[C]** Very short-lived test (generate a couple of minutes), single account with default starting conditions (balance, leverage, etc) and default rules.
- **[S]** `Simulator` doesn't receive any message
- **[D]** All order books empty

##### **Situation #1**
- **[C]** Very short-lived test (generate a couple of minutes), single account with default starting conditions (balance, leverage, etc) and default rules.
- **[S]** `Simulator` receives an `OrderCreate` with immediate placement (price = 0) with size 1, no `SL`/`TP`/`expiration`. Some "`n_ticks`" ticks later, it receives an `OrderDelete` to close that trade. 
- **[D]** The order shall execute immediately as trade and be moved to `trades_active`. Afterwards, the delete request shall close the trade and move it to `trades_closed`. Price difference and PNL shall be equal to the one implied by the price movement. Verify correctness and validity in the change(s) within the `Account`'s values in balance, equity, GAV, NAV, margin and margin level at each moment (before order creation, after order placement, before reception of delete request, and after confirmation of delete request)
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
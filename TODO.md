3- For candle processing:
3-1- Only receive 1m candles and construct higher candles from them. Store candles in Postgres database or TimescaleDB.
3-2- For backtesting, make sure old candles from two years ago are available and if not, download and save the candles.
3-3- In live mode, store new candles in db too.
3-4- Assume Later I will work with other exchanges too, so make it flexible.
3-5- Assume Later I will store both higher candles from exchanges and higher constructed candles from 1m candles. So make it flexible too.
7- My strategy is a combination of technical indicators, candlestick patterns, and price action.
7-1- Implement the feature for defining the backtesting on historical data and output strategy performance metrics (if data is not available, the bot must download and store data).
7-2- Implement running strategy in live mode which decides on buy or sell using current data from exchange and also historical data then sends notification on telegram
7-3- Implement a running strategy so that besides generating signals, it also submits real orders to exchange. If candles for two years age is not available or partially available, the bot must first download the data since the strategy using new and old data together for making decisions. 
7-4 store each action of the strategy (order submission, etc) in the database.
8- store exchange order books of various symbols in the database. 
9- Also store ticks.
10- Implement full order management lifecycle, trade management lifecycle, market management lifecycle.
11- Implement journaling of all events.
12- Completely store the state of each order submitted, for example how much limit order is filled.
13- For now I use trading in the spot market but later I will go for derivatives and futures trading. So make the bot flexible. 
14- Implement graceful shutdown
15- Implement the bot such that in case of power failure, it restarts in a safe state.
16- If you use websocket, take care of disconnection, missing candles and missing orderbook L2 data.
17- You can also use Redis if you want.
Add anything extra which is needed to make this a robust trading bot.


Websocket State Management
If websocket endpoints are available, implement:
Connection management (connect, disconnect, reconnect)
Subscription management (subscribe/unsubscribe to channels)
Message parsing and dispatch
Heartbeat/ping-pong
Buffering and replay of missed data after reconnect


Order management and trade lifecycle (for live trading).
More indicators, patterns, and advanced strategies.
Journaling and event logging improvements.
Robust state recovery and graceful shutdown.


Adding order status tracking and recovery on restart.
Improving notification coverage.
Adding a verbose flag for live mode.
Handling of partial candle downloads:
If the DB is missing only part of the requested range, the code currently fetches the entire range from Wallex. It could be more efficient by only fetching missing segments.
Strategy warm-up period:
Some strategies (e.g., moving averages) need a warm-up period. The code should skip performance evaluation until enough candles are available.
Position sizing based on risk:
The code currently uses a fixed order size. For more realistic backtests, position size could be calculated based on risk per trade and account equity.
Multiple symbol/interval backtesting:
The code appears to support only one symbol/interval at a time.
Verbose output and logging:
Optionally, add a verbose flag to control the amount of output.
Error handling/reporting:
Ensure all errors (DB, API, file) are logged and reported.
Final open position handling:
If a position is open at the end of the backtest, it should be closed at the last candle price.
Configurable output file names:
Allow the user to specify output file names for trade logs and equity curves.
Persistence of all actions/events:
Ensure every order, signal, and error is logged to the DB (not just some).
Order status tracking:
Track open orders, check for fills/cancellations, and update state accordingly.
Handling exchange downtime and API rate limits:
Add logic to pause/retry gracefully if the exchange is down or rate-limited.
Resilience and recovery:
On restart, reload open positions and pending orders from persistent storage.
Comprehensive notification system:
Notify on all critical events: order filled, order failed, stop loss hit, daily loss limit, etc.
Position sizing based on account balance/risk:
Calculate order size dynamically if desired.
Health checks and monitoring:
Optionally, add periodic health/status notifications.
Verbose/quiet mode for logging.
(Optional) Advanced order types:
Support for OCO, partial fills, etc.
Comprehensive event logging:
Not all actions (e.g., every signal, every order submission, every fill/cancel, every state change) are logged to the DB.
Order status tracking:
No polling for open orders to check for fills/cancels, or recovery after restart.
Resilience and recovery:
On restart, open positions and pending orders should be reloaded and handled.
Notification coverage:
Not all critical events (e.g., order filled, order failed, position opened/closed, daily loss hit) are notified.
Verbose/quiet mode for logging.
Health checks:
No periodic status/health notifications.
Graceful handling of exchange downtime/rate limits:
There is retry logic, but no exponential backoff or alerting if the exchange is down for a long time.
Position sizing based on account balance/risk:
Currently, order size is fixed.
Further refinements (e.g., dynamic warm-up based on strategy parameters), or want this in all live trading variants (with ingestion, etc.)


Write tests for other models in postgres.go
Write tests for remaining functions of candle/



proper save state -> shutdown -> restart -> restore last state -> warmup (get new state) -> run -><- change state
phase 1: load everything or load old (ok), start (ingest single or batch, transactional) -> ok, finish or crash utc or local time for wallex
generate readme
channel unsubscribe
// UTC *********
// TX on CTX
// rerun tests specially for strategy rsi
// It is not guaranteed that order fills
// location of subscription
// position must decide on ticks too not only candles
// front
// opened positions, unfilled positions, failing orders, tick data using websocket, multitimeframe channel
// test position with real money
// stop loss or take profit on tick, signal on candle
// read candles from db instead of channel.
// Dont store ticks in db, store in cache, CSP for last tick

bitcoin price correlation on doge coin

read websocket.go logic
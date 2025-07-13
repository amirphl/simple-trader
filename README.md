# Simple Trader

A robust, extensible crypto algorithmic trading bot written in Go, designed for both backtesting and live trading.

## Features

- **Exchange Integration**
  - Modular exchange abstraction (currently supports Wallex)
  - Easily extendable to support additional exchanges
  - Websocket integration for real-time market data

- **Advanced Candle Management**
  - Real-time candle ingestion from exchange data
  - Automatic candle aggregation (1m → 5m → 15m → 30m → 1h → 4h → 1d)
  - Historical data backfill (up to 2 years)
  - Efficient storage in PostgreSQL/TimescaleDB
  - Optimized for high-frequency updates and queries

- **Technical Analysis**
  - Comprehensive technical indicators:
    - Moving Averages (SMA, EMA)
    - Oscillators (RSI, Stochastic)
    - Trend indicators (MACD, Bollinger Bands, ATR)
  - Candlestick pattern detection (Doji, Engulfing, Hammer, Morning/Evening Star)
  - Price action analysis

- **Strategy Engine**
  - Modular strategy implementation
  - Composite strategies (combining multiple indicators)
  - Configurable risk management parameters
  - Strategy-specific timeframe selection

- **Position Management**
  - Sophisticated position tracking and management
  - Trade statistics and performance metrics
  - Automatic position sizing based on risk parameters
  - Support for both long and short positions
  - Trailing stop implementation with configurable parameters

- **Order Management**
  - Market and limit orders
  - Stop-loss and take-profit functionality
  - Trailing stops
  - Order tracking and management

- **Risk Management**
  - Per-trade risk calculation
  - Stop-loss automation
  - Maximum daily loss limits
  - Position sizing based on risk percentage
  - Strategy-specific risk parameters

- **Backtesting**
  - Historical performance analysis
  - Realistic simulation with slippage and commission
  - Equity curve generation
  - Detailed trade logs
  - Performance metrics (win rate, profit factor, Sharpe ratio, expectancy)

- **Operational Features**
  - Telegram notifications for trades and system events
  - Event journaling and state persistence
  - Graceful shutdown and recovery
  - Comprehensive error handling and retry mechanisms

## Architecture

```
cmd/
  main.go                  # Main entry point with CLI options
internal/
  candle/                  # Candle aggregation, storage, retrieval
    candle.go              # Core candle types and interfaces
    ingestion.go           # Real-time candle ingestion system
  config/
    config.go              # Configuration management
  db/
    db.go                  # Database interface
    postgres.go            # PostgreSQL/TimescaleDB implementation
  exchange/
    exchange.go            # Exchange interface
    wallex.go              # Wallex exchange implementation
    websocket.go           # Websocket client for real-time data
  indicator/
    indicator.go           # Base indicator interface
    sma.go, ema.go         # Moving average implementations
    rsi.go                 # Relative Strength Index
    macd.go                # Moving Average Convergence Divergence
    bollinger.go           # Bollinger Bands
    atr.go                 # Average True Range
    stochastic.go          # Stochastic Oscillator
  pattern/
    pattern.go             # Pattern detection interface
    doji.go                # Doji pattern detection
    engulfing.go           # Bullish/Bearish Engulfing patterns
    hammer.go              # Hammer/Hanging Man patterns
    morning_evening_star.go # Morning/Evening Star patterns
  position/
    position.go            # Position management system
  strategy/
    strategy.go            # Strategy interface
    sma.go                 # Simple Moving Average strategy
    ema.go                 # Exponential Moving Average strategy
    rsi.go                 # RSI strategy
    rsi_obos.go            # RSI Overbought/Oversold strategy
    macd.go                # MACD strategy
    composite.go           # Composite strategy
  order/
    order.go               # Order types and management
  market/
    market.go              # Market data structures (orderbook, ticks)
  notifier/
    notifier.go            # Notification interface
    telegram.go            # Telegram implementation
  journal/
    journal.go             # Event journaling
  state/
    state.go               # State persistence
  utils/
    log.go                 # Logging utilities
scripts/
  schema.sql               # Database schema
wallex-go/                 # Wallex API client
```

## Getting Started

### Prerequisites

- Go 1.24 or higher
- PostgreSQL with TimescaleDB extension
- Wallex API credentials (for live trading)
- Telegram bot token (optional, for notifications)

### Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/amirphl/simple-trader.git
   cd simple-trader
   ```

2. Install dependencies:
   ```bash
   go mod download
   ```

3. Set up the database:
   ```bash
   psql -U your_user -d your_database -f scripts/schema.sql
   ```

4. Build the application:
   ```bash
   go build -o simple-trader ./cmd
   ```

### Configuration

Simple Trader can be configured via command-line flags or a YAML configuration file:

```yaml
wallex_api_key: "your-api-key"
db_conn_str: "postgres://user:password@localhost:5432/simple_trader?sslmode=disable"
db_max_open: 10
db_max_idle: 5
mode: "live"  # or "backtest"
symbols: ["BTCIRT", "ETHIRT"]
strategies: ["rsi", "macd"]
risk_percent: 1.0
stop_loss_percent: 2.0
trailing_stop_percent: 0.5
take_profit_percent: 3.0
max_daily_loss: 100.0
limit_spread: 0.1
order_type: "market"  # or "limit", "stop-limit", "oco"
notification_retries: 3
notification_delay: 5s
risk_map:
  rsi: { risk_percent: 1.0, stop_loss_percent: 2.0, trailing_stop_percent: 0.5 }
  macd: { risk_percent: 0.5, stop_loss_percent: 1.5, trailing_stop_percent: 0.2 }
slippage_percent: 0.05
commission_percent: 0.1
telegram_token: "your-telegram-bot-token"
telegram_chat_id: "your-chat-id"
```

### Usage

#### Live Trading

```bash
./simple-trader -mode live -symbols btc-usdt,eth-usdt -strategies rsi,macd -risk-percent 1.0 -stop-loss-percent 2.0
```

Or with a config file:

```bash
./simple-trader -config config.yaml
```

#### Backtesting

```bash
./simple-trader -mode backtest -symbols btc-usdt -strategies rsi -from 2023-01-01 -to 2023-12-31
```

### Position Management

The system features a sophisticated position management system:

- **Risk-Based Sizing**: Automatically calculates position size based on risk parameters
- **Stop-Loss Management**: Implements fixed and trailing stops
- **Take-Profit Targets**: Configurable take-profit levels
- **Performance Tracking**: Calculates key metrics like win rate, profit factor, and Sharpe ratio
- **Strategy-Specific Parameters**: Different risk parameters for each strategy
- **Daily Loss Limits**: Automatically disables trading when daily loss threshold is reached

### Technical Indicators

The system implements a comprehensive set of technical indicators:

- **Moving Averages**: Simple Moving Average (SMA), Exponential Moving Average (EMA)
- **Oscillators**: Relative Strength Index (RSI), Stochastic Oscillator
- **Trend Indicators**: Moving Average Convergence Divergence (MACD), Bollinger Bands, Average True Range (ATR)

### Pattern Detection

The system includes detection for common candlestick patterns:

- **Doji**: Identifies indecision in the market
- **Engulfing**: Detects bullish and bearish engulfing patterns
- **Hammer/Hanging Man**: Identifies potential reversals
- **Morning/Evening Star**: Detects complex reversal patterns

### Equity Curve & Trade Log Reporting

- **Backtest mode:**
  - Prints the equity curve (cumulative PnL after each trade) as a list.
  - Prints a trade log with entry/exit price, timestamps, and PnL for each trade.
- **Live mode:**
  - Prints the live equity curve and trade log after each trade close.

This helps you visualize performance and audit every trade.

## Candle Ingestion System

The system features a robust candle ingestion and aggregation system:

- **Real-time Ingestion**: Fetches 1m candles from exchanges
- **Automatic Aggregation**: Aggregates to higher timeframes (5m, 15m, 30m, 1h, 4h, 1d)
- **Gap Detection**: Identifies and fills missing candle data
- **Efficient Storage**: Optimized for time-series data with TimescaleDB

For detailed information, see [CANDLE_INGESTION.md](CANDLE_INGESTION.md).

## Constructed Candles

The system can construct candles from raw market data:

- **Aggregation**: Builds higher timeframe candles from 1m data
- **Validation**: Ensures data integrity and correctness
- **Storage**: Efficiently stores constructed candles

For detailed information, see [CONSTRUCTED_CANDLES.md](CONSTRUCTED_CANDLES.md).

## 1M Candle Storage

The system implements efficient storage for high-frequency 1-minute candles:

- **Optimized Schema**: Designed for time-series data
- **Indexing**: Fast retrieval by symbol, timeframe, and timestamp
- **Compression**: Efficient storage with TimescaleDB

For detailed information, see [1M_CANDLE_STORAGE.md](1M_CANDLE_STORAGE.md).

## Roadmap

- [x] Project scaffolding
- [x] Exchange abstraction
- [x] Candle aggregation/storage
- [x] Indicator/pattern engine
- [x] Strategy engine
- [x] Position management
- [x] Order/trade/market management
- [x] Event journaling/state persistence
- [x] Notification integration
- [ ] Web interface for monitoring
- [ ] Machine learning integration
- [ ] Portfolio management
- [ ] Multi-exchange arbitrage

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- [TimescaleDB](https://www.timescale.com/) for time-series database optimization
- [Wallex Exchange](https://wallex.ir/) for API access 
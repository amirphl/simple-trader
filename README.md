# Simple Trader

A robust, extensible crypto algorithmic trading bot written in Go, designed for both backtesting and live trading with comprehensive database persistence and advanced market data processing.

## Features

- **Exchange Integration**
  - Modular exchange abstraction (currently supports Wallex)
  - Mock exchange implementation for testing and simulation
  - Easily extendable to support additional exchanges
  - Real-time websocket integration for market data
  - Optimized tick processing (stores last tick instead of overwhelming subscribers)

- **Database Persistence**
  - Full PostgreSQL/TimescaleDB integration with comprehensive schema
  - Position management with unique ID-based tracking for historical records
  - Automatic position saving on creation and updating on closure
  - JSONB storage for complex data structures (risk parameters, order specs, trade logs)
  - Transactional operations with rollback support

- **Advanced Market Data Processing**
  - Real-time orderbook fetching and processing from exchange APIs
  - Optimized websocket implementation with last-tick storage pattern
  - Market statistics and tick data retrieval
  - Efficient market depth analysis for optimal order placement

- **Advanced Candle Management**
  - Real-time candle ingestion from exchange data
  - Automatic candle aggregation (1m → 5m → 15m → 30m → 1h → 4h → 1d)
  - Historical data backfill (up to 2 years)
  - Efficient storage in PostgreSQL/TimescaleDB
  - Optimized for high-frequency updates and queries
  - Clear distinction between raw and constructed candles
  - Comprehensive source tracking and validation

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

- **Enhanced Position Management**
  - Database-backed position persistence with unique ID tracking
  - Automatic position saving and updating lifecycle
  - Sophisticated position tracking and management
  - Trade statistics and performance metrics
  - Automatic position sizing based on risk parameters
  - Support for both long and short positions
  - Trailing stop implementation with configurable parameters
  - Historical position tracking for performance analysis

- **Order Management**
  - Market and limit orders with mock exchange support for testing
  - Stop-loss and take-profit functionality
  - Trailing stops
  - Order tracking and management
  - Optimal order placement using real-time orderbook data

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
  - Thread-safe operations with mutex protection

## Architecture

```
cmd/
  main.go                  # Main entry point with CLI options
internal/
  candle/                  # Candle aggregation, storage, retrieval
    candle.go              # Core candle types and interfaces
    ingestion.go           # Real-time candle ingestion system
    heiken_ashi.go         # Heikin Ashi candle construction
  config/
    config.go              # Configuration management
  db/                      # Database layer with full persistence
    db.go                  # Database interfaces and DTOs
    postgres.go            # PostgreSQL/TimescaleDB implementation
    conf/                  # Database configuration
  exchange/                # Exchange abstraction layer
    exchange.go            # Exchange interface
    wallex.go              # Wallex exchange implementation
    mock_wallex.go         # Mock exchange for testing/simulation
    websocket.go           # Optimized websocket client for real-time data
    adapter.go             # Exchange adapters
  indicator/               # Technical analysis indicators
    indicator.go           # Base indicator interface
    sma.go, ema.go         # Moving average implementations
    rsi.go                 # Relative Strength Index
    macd.go                # Moving Average Convergence Divergence
    bollinger.go           # Bollinger Bands
    atr.go                 # Average True Range
    stochastic.go          # Stochastic Oscillator
  pattern/                 # Candlestick pattern detection
    pattern.go             # Pattern detection interface
    doji.go                # Doji pattern detection
    engulfing.go           # Bullish/Bearish Engulfing patterns
    hammer.go              # Hammer/Hanging Man patterns
    morning_evening_star.go # Morning/Evening Star patterns
  position/                # Enhanced position management
    position.go            # Database-backed position management system
  strategy/                # Trading strategies
    strategy.go            # Strategy interface
    sma.go                 # Simple Moving Average strategy
    ema.go                 # Exponential Moving Average strategy
    rsi.go                 # RSI strategy
    rsi_obos.go            # RSI Overbought/Oversold strategy
    macd.go                # MACD strategy
    composite.go           # Composite strategy
    stochastic_heikin_ashi.go # Stochastic with Heikin Ashi
    engulfing_heikin_ashi.go  # Engulfing pattern with Heikin Ashi
  order/
    order.go               # Order types and management
  market/
    market.go              # Market data structures (orderbook, ticks)
  notifier/
    notifier.go            # Notification interface
    telegram.go            # Telegram implementation
  journal/                 # Event journaling
  livetrading/            # Live trading coordination
  backtest/               # Backtesting engine
  state/
    state.go               # State persistence
  utils/
    log.go                 # Logging utilities
  tfutils/                # Timeframe utilities
scripts/
  schema.sql               # Complete database schema with positions table
```

## Database Schema

The system uses a comprehensive PostgreSQL/TimescaleDB schema:

- **Positions Table**: ID-based position tracking with full persistence
  - Unique BIGSERIAL primary key for historical tracking
  - JSONB storage for complex structures (risk params, trade logs, equity curves)
  - Support for multiple active/inactive positions per strategy/symbol
  - Complete trade history and performance metrics

- **Time-Series Tables**: Optimized for high-frequency market data
  - Candles, Orderbooks, Ticks with TimescaleDB hypertables
  - Efficient indexing for symbol/timestamp queries

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

#### Testing with Mock Exchange

```bash
./simple-trader -mode live -use-mock-exchange -symbols btc-usdt -strategies rsi
```

#### Backtesting

```bash
./simple-trader -mode backtest -symbols btc-usdt -strategies rsi -from 2023-01-01 -to 2023-12-31
```

### Enhanced Position Management

The system features a sophisticated database-backed position management system:

- **ID-Based Tracking**: Each position gets a unique BIGSERIAL ID for historical tracking
- **Automatic Persistence**: Positions are automatically saved on creation and updated on closure
- **Risk-Based Sizing**: Automatically calculates position size based on risk parameters
- **Stop-Loss Management**: Implements fixed and trailing stops
- **Take-Profit Targets**: Configurable take-profit levels
- **Performance Tracking**: Calculates key metrics like win rate, profit factor, and Sharpe ratio
- **Strategy-Specific Parameters**: Different risk parameters for each strategy
- **Daily Loss Limits**: Automatically disables trading when daily loss threshold is reached
- **Historical Analysis**: Complete trade history with performance metrics stored in database

### Market Data Features

- **Real-time Orderbook**: Fetches and processes live orderbook data from Wallex API
- **Optimized Websockets**: Uses last-tick storage pattern to prevent subscriber overwhelm
- **Market Statistics**: Comprehensive market data including bid/ask, volume, price changes
- **Optimal Order Placement**: Uses real-time market depth for optimal entry/exit pricing

### Mock Exchange for Testing

The system includes a comprehensive mock exchange implementation:

- **Hybrid Approach**: Proxies read-only operations to real exchange while mocking order operations
- **Limit Order Support**: Successfully simulates limit order execution
- **Error Simulation**: Proper error handling for unsupported order types
- **Testing Safety**: Allows strategy testing without real money at risk

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
  - All data persisted to database for historical analysis.

This helps you visualize performance and audit every trade.

## Candle Ingestion and Storage System

The system features a comprehensive candle ingestion and storage system:

- **Raw 1m Candle Storage**: Efficient storage of raw 1-minute candles from exchanges
- **Intelligent Aggregation**: Real-time aggregation to higher timeframes (5m, 15m, 30m, 1h, 4h, 1d)
- **Constructed Candle Management**: Clear distinction between raw exchange data and constructed (aggregated) candles
- **Source Tracking**: Support for multiple data sources with proper source attribution
- **Gap Detection**: Identifies and fills missing candle data
- **Efficient Storage**: Optimized for time-series data with TimescaleDB
- **Performance Optimizations**: Batch processing, caching, and optimized queries

For detailed information, see [CANDLE_INGESTION_AND_STORAGE.md](CANDLE_INGESTION_AND_STORAGE.md).

## Multi-Symbol Backtesting

The system now supports running backtests across multiple top-performing symbols from Binance automatically.

### Configuration

Set the following parameters in `conf.yml`:

```yaml
mode: "backtest"
multi_symbol_backtest: true
top_symbols_count: 100  # Test top 100 symbols by 24h volume
strategies: ["engulfing_heikin_ashi"]
```

Or use command line flags:

```bash
go run cmd/main.go \
  --mode=backtest \
  --multi-symbol-backtest=true \
  --top-symbols-count=20 \
  --strategies=engulfing_heikin_ashi \
  --from=2023-01-01 \
  --to=2024-01-01
```

### Features

- **Automatic Symbol Selection**: Fetches top N USDT pairs by 24h volume from Binance
- **Parallel Processing**: Runs backtests for each symbol sequentially with progress tracking
- **Comprehensive HTML Report**: Generates `multi_symbol_backtest_report.html` with:
  - Overall performance metrics
  - Individual charts for each symbol with Heikin Ashi candles
  - Interactive controls for each chart
  - Top/bottom performing symbols
  - Aggregate statistics
- **Robust API Calls**: Exponential backoff retry logic for reliable data fetching behind proxies

### API Retry Configuration

For environments with proxy or network connectivity issues, configure retry behavior:

```yaml
# API retry configuration (useful for proxy environments)
api_retry_max_attempts: 5     # Maximum retry attempts for API calls
api_retry_base_delay: 1s      # Base delay between retries
api_retry_max_delay: 30s      # Maximum delay between retries
```

Or via command line:

```bash
go run cmd/main.go \
  --api-retry-max-attempts=3 \
  --api-retry-base-delay=2s \
  --api-retry-max-delay=60s
```

**Retry Features:**
- **Exponential Backoff**: Delays increase exponentially with each retry
- **Jitter**: Random variation prevents thundering herd problems
- **Smart Retry**: Only retries on retryable errors (network issues, rate limits, server errors)
- **Proxy Support**: Enhanced reliability for proxy environments
- **Context Awareness**: Respects cancellation and timeouts

### Output

The multi-symbol backtest generates:

1. **Console Summary**: 
   - Total symbols tested
   - Overall win rate and P&L
   - Top 10 and bottom 5 performing symbols
   - Success/failure statistics

2. **JSON Data File** (`multi_symbol_backtest_data.json`):
   - Complete backtest results and chart data in JSON format
   - Structured data for external processing or custom visualization

3. **Interactive HTML Report** (`multi_symbol_backtest_report.html`):
   - Standalone HTML file that loads data from the JSON file
   - Interactive charts for each symbol with real-time data loading
   - Overall performance dashboard with filterable results
   - Responsive design that works offline

**Usage**: Run the backtest to generate the JSON data, then open `multi_symbol_backtest_report.html` in your browser to view the interactive report. The HTML file will automatically fetch and display the latest backtest data.

### Example Usage

Test EngulfingHeikinAshi strategy on top 50 symbols:

```bash
go run cmd/main.go \
  --config=conf.yml \
  --multi-symbol-backtest=true \
  --top-symbols-count=50
```

Test on specific smaller set for quick validation:

```bash
go run cmd/main.go \
  --mode=backtest \
  --multi-symbol-backtest=true \
  --top-symbols-count=10 \
  --strategies=engulfing_heikin_ashi \
  --from=2023-06-01 \
  --to=2023-12-01
```

### Example Workflow

1. **Run Multi-Symbol Backtest**:
```bash
go run cmd/main.go \
  --mode=backtest \
  --multi-symbol-backtest=true \
  --top-symbols-count=20 \
  --strategies=engulfing_heikin_ashi \
  --from=2023-01-01 \
  --to=2023-06-01
```

2. **Generated Files**:
   - `multi_symbol_backtest_data.json` - Raw backtest data
   - `multi_symbol_backtest_report.html` - Interactive report viewer

3. **View Results**:
   - Open `multi_symbol_backtest_report.html` in your browser
   - The page will automatically load data from the JSON file
   - Use the "Refresh Data" button to reload after running new backtests

## Roadmap

- [x] Project scaffolding
- [x] Exchange abstraction
- [x] Mock exchange implementation for testing
- [x] Real-time orderbook functionality
- [x] Optimized websocket implementation
- [x] Database persistence for positions
- [x] ID-based position tracking
- [x] Candle aggregation/storage
- [x] Indicator/pattern engine
- [x] Strategy engine
- [x] Position management with database backing
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
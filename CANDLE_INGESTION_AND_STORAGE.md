# Candle Ingestion and Storage System

## Overview

The Simple Trader bot includes a comprehensive system for storing and managing candle data with the following key capabilities:

1. **Raw 1m Candle Storage**: Efficient storage of raw 1-minute candles from exchanges
2. **Intelligent Aggregation**: Real-time aggregation to higher timeframes (5m, 15m, 30m, 1h, 4h, 1d)
3. **Constructed Candle Management**: Clear distinction between raw exchange data and constructed (aggregated) candles
4. **Flexible Source Tracking**: Support for multiple data sources with proper source attribution

This system provides the foundation for accurate technical analysis across multiple timeframes for both live trading and backtesting scenarios.

## Key Features

### 1. Raw 1m Candle Storage
- **Efficient Storage**: Optimized database operations for high-frequency 1m candle data
- **Data Validation**: Comprehensive validation before storage to ensure data integrity
- **Batch Processing**: Efficient batch insertion with conflict resolution
- **Gap Detection**: Automatic identification of missing 1m candle data

### 2. Intelligent Aggregation Engine
- **Multi-Timeframe Support**: Aggregates 1m candles to 5m, 15m, 30m, 1h, 4h, 1d
- **Path-Based Aggregation**: Calculates optimal aggregation paths between timeframes
- **Incremental Updates**: Efficiently updates existing aggregated candles
- **Bulk Processing**: High-performance bulk aggregation for historical data

### 3. Constructed Candle Management
- **"constructed" Source**: All aggregated candles are marked with source="constructed"
- **Raw vs Constructed**: Clear distinction between exchange data and aggregated data
- **Source Validation**: Built-in validation to ensure proper source assignment
- **Source Statistics**: Comprehensive statistics about candle sources

### 4. Real-time Processing
- **Live Ingestion**: Real-time processing of incoming 1m candles
- **Automatic Aggregation**: Immediate aggregation to higher timeframes
- **Caching System**: In-memory caching for fast access to latest candles
- **Error Recovery**: Robust error handling and recovery mechanisms

## Architecture

### Core Components

#### 1. Candle Structure
```go
type Candle struct {
    Timestamp time.Time
    Open      float64
    High      float64
    Low       float64
    Close     float64
    Volume    float64
    Symbol    string
    Timeframe string
    Source    string
}
```

**Key Methods:**
- `Validate()`: Ensures candle data integrity
- `IsConstructed()`: Checks if candle is constructed (aggregated)
- `IsRaw()`: Checks if candle is from an exchange
- `IsSynthesized()`: Checks if candle is synthetically generated
- `GetSourceType()`: Returns the type of candle source
- `SetConstructed()`: Marks the candle as constructed
- `SetRaw(source)`: Marks the candle as raw with the given source

#### 2. Storage Interface
```go
type Storage interface {
    SaveCandle(ctx context.Context, candle *Candle) error
    SaveCandles(ctx context.Context, candles []Candle) error
    SaveConstructedCandles(ctx context.Context, candles []Candle) error
    GetCandle(ctx context.Context, symbol, timeframe string, timestamp time.Time, source string) (*Candle, error)
    GetCandles(ctx context.Context, symbol, timeframe string, start, end time.Time) ([]Candle, error)
    GetCandlesV2(ctx context.Context, timeframe string, start, end time.Time) ([]Candle, error)
    GetCandlesInRange(ctx context.Context, symbol, timeframe string, start, end time.Time, source string) ([]Candle, error)
    GetConstructedCandles(ctx context.Context, symbol, timeframe string, start, end time.Time) ([]Candle, error)
    GetRawCandles(ctx context.Context, symbol, timeframe string, start, end time.Time) ([]Candle, error)
    GetLatestCandle(ctx context.Context, symbol, timeframe string) (*Candle, error)
    GetLatestCandleInRange(ctx context.Context, symbol, timeframe string, start, end time.Time) (*Candle, error)
    GetLatestConstructedCandle(ctx context.Context, symbol, timeframe string) (*Candle, error)
    GetLatest1mCandle(ctx context.Context, symbol string) (*Candle, error)
    DeleteCandles(ctx context.Context, symbol, timeframe string, before time.Time) error
    DeleteCandlesInRange(ctx context.Context, symbol, timeframe string, start, end time.Time, source string) error
    DeleteConstructedCandles(ctx context.Context, symbol, timeframe string, before time.Time) error
    GetCandleCount(ctx context.Context, symbol, timeframe string, start, end time.Time) (int, error)
    GetConstructedCandleCount(ctx context.Context, symbol, timeframe string, start, end time.Time) (int, error)
    UpdateCandle(ctx context.Context, candle Candle) error
    UpdateCandles(ctx context.Context, candle []Candle) error
    GetAggregationStats(ctx context.Context, symbol string) (map[string]any, error)
    GetMissingCandleRanges(ctx context.Context, symbol string, start, end time.Time) ([]struct{ Start, End time.Time }, error)
    GetCandleSourceStats(ctx context.Context, symbol string, start, end time.Time) (map[string]any, error)
}
```

#### 3. Aggregator Interface
```go
type Aggregator interface {
    Aggregate(candles []Candle, timeframe string) ([]Candle, error)
    AggregateIncremental(newCandle Candle, existingCandles []Candle, timeframe string) ([]Candle, error)
    AggregateFrom1m(oneMCandles []Candle, targetTimeframe string) ([]Candle, error)
    Aggregate1mTimeRange(ctx context.Context, symbol string, start, end time.Time, targetTimeframe string) ([]Candle, error)
}
```

#### 4. Ingester Interface
```go
type Ingester interface {
    IngestCandle(ctx context.Context, c Candle) error
    IngestRaw1mCandles(ctx context.Context, candles []Candle) error
    AggregateSymbolToHigherTimeframes(ctx context.Context, symbol string) error
    BulkAggregateFrom1m(ctx context.Context, symbol string, start, end time.Time) error
    BulkAggregateAllSymbolsFrom1m(ctx context.Context, start, end time.Time) error
    GetLatestCandle(ctx context.Context, symbol, timeframe string) (*Candle, error)
    CleanupOldData(ctx context.Context, symbol, timeframe string, retentionDays int) error
    Subscribe() <-chan []Candle
    Unsubscribe(ch <-chan []Candle)
}
```

#### 5. IngestionService Interface
```go
type IngestionService interface {
    Start() error
    Stop()
    GetIngestionStats() map[string]map[string]any
    Subscribe() <-chan []Candle
    UnSubscribe(ch <-chan []Candle)
}
```

### Database Schema

```sql
CREATE TABLE candles (
    symbol VARCHAR(20) NOT NULL,
    timeframe VARCHAR(10) NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    open DECIMAL(20,8) NOT NULL,
    high DECIMAL(20,8) NOT NULL,
    low DECIMAL(20,8) NOT NULL,
    close DECIMAL(20,8) NOT NULL,
    volume DECIMAL(20,8) NOT NULL,
    source VARCHAR(50) NOT NULL,
    PRIMARY KEY (symbol, timeframe, timestamp, source)
);

-- TimescaleDB hypertable for time-series optimization
SELECT create_hypertable('candles', 'timestamp');

-- Indexes for efficient querying
CREATE INDEX idx_candles_symbol_timeframe_timestamp ON candles(symbol, timeframe, timestamp);
CREATE INDEX idx_candles_source ON candles(source);
CREATE INDEX idx_candles_symbol_timeframe_source ON candles(symbol, timeframe, source);
```

## Key Implementation Details

### 1. Source Field Management

The system uses the `source` field to distinguish between different types of candles:

- **"wallex"**: Raw candles from Wallex exchange
- **"constructed"**: Aggregated candles created by the system
- **"synthetic"**: Synthetically generated candles for filling gaps
- **"binance"**: Raw candles from Binance exchange (future)
- **"unknown"**: Raw candles with unknown source

Source-specific methods ensure proper handling:

```go
// IsConstructed returns true if the candle was constructed (aggregated)
func (c *Candle) IsConstructed() bool {
    return c.Source == "constructed"
}

// IsSynthesized returns true if the candle was synthesized
func (c *Candle) IsSynthesized() bool {
    return c.Source == "synthetic"
}

// IsRaw returns true if the candle is raw (from exchange)
func (c *Candle) IsRaw() bool {
    return c.Source != "constructed"
}

// GetSourceType returns the type of candle source
func (c *Candle) GetSourceType() string {
    if c.IsConstructed() {
        return "constructed"
    }
    if c.IsSynthesized() {
        return "synthetic"
    }
    return "raw"
}
```

### 2. Aggregation Process

The aggregation process automatically marks aggregated candles as "constructed":

```go
func (a *DefaultAggregator) AggregateIncremental(newCandle Candle, existingCandles []Candle, timeframe string) ([]Candle, error) {
    // ... aggregation logic ...
    
    if !bucketFound {
        // Create new aggregated candle
        agg := Candle{
            Timestamp: newBucket,
            Open:      newCandle.Open,
            High:      newCandle.High,
            Low:       newCandle.Low,
            Close:     newCandle.Close,
            Volume:    newCandle.Volume,
            Symbol:    newCandle.Symbol,
            Timeframe: timeframe,
            Source:    "constructed",  // Automatically set
        }
        existingCandles = append(existingCandles, agg)
        
        // ... rest of method ...
    }
    
    return existingCandles, nil
}
```

### 3. Timeframe Management

The system supports multiple timeframes with proper validation and conversion:

```go
// ParseTimeframe parses timeframe string (e.g., "5m", "1h") to time.Duration
func ParseTimeframe(timeframe string) (time.Duration, error) {
    switch timeframe {
    case "1m":
        return time.Minute, nil
    case "5m":
        return 5 * time.Minute, nil
    case "15m":
        return 15 * time.Minute, nil
    case "30m":
        return 30 * time.Minute, nil
    case "1h":
        return time.Hour, nil
    case "4h":
        return 4 * time.Hour, nil
    case "1d":
        return 24 * time.Hour, nil
    default:
        return 0, errors.New("unsupported timeframe")
    }
}

// GetSupportedTimeframes returns all supported timeframes
func GetSupportedTimeframes() []string {
    return []string{"1m", "5m", "15m", "30m", "1h", "4h", "1d"}
}

// GetAggregationTimeframes returns timeframes for aggregation
func GetAggregationTimeframes() []string {
    return []string{"5m", "15m", "30m", "1h", "4h", "1d"}
}
```

### 4. Aggregation Path Calculation

The system calculates optimal paths for aggregation:

```go
// GetAggregationPath returns the path of timeframes needed to aggregate from source to target
func GetAggregationPath(sourceTf, targetTf string) ([]string, error) {
    if !IsValidTimeframe(sourceTf) || !IsValidTimeframe(targetTf) {
        return nil, fmt.Errorf("invalid timeframe: source=%s, target=%s", sourceTf, targetTf)
    }

    sourceDur := GetTimeframeDuration(sourceTf)
    targetDur := GetTimeframeDuration(targetTf)

    if sourceDur >= targetDur {
        return nil, fmt.Errorf("source timeframe must be smaller than target timeframe")
    }

    // Pre-sort timeframes by duration for more efficient lookup
    timeframes := GetSupportedTimeframes()
    sort.Slice(timeframes, func(i, j int) bool {
        return GetTimeframeDuration(timeframes[i]) < GetTimeframeDuration(timeframes[j])
    })

    // Find the path of timeframes needed
    var path []string
    current := sourceTf
    for GetTimeframeDuration(current) < targetDur {
        // Find the next larger timeframe
        found := false
        for _, timeframe := range timeframes {
            tfDuration := GetTimeframeDuration(timeframe)
            if tfDuration > GetTimeframeDuration(current) && tfDuration <= targetDur {
                path = append(path, timeframe)
                current = timeframe
                found = true
                break
            }
        }
        if !found {
            break
        }
    }

    return path, nil
}
```

## Configuration

### IngestionConfig
```go
type IngestionConfig struct {
    Symbols         []string
    FetchCycle      time.Duration
    RetentionDays   int
    MaxRetries      int
    RetryDelay      time.Duration
    DelayUpperbound time.Duration
    EnableCleanup   bool
    CleanupCycle    time.Duration
}
```

### Default Configuration
```go
func DefaultIngestionConfig() IngestionConfig {
    return IngestionConfig{
        Symbols:         []string{"BTCUSDT", "ETHUSDT"},
        FetchCycle:      30 * time.Second,
        RetentionDays:   30,
        MaxRetries:      3,
        RetryDelay:      3 * time.Second,
        DelayUpperbound: 20 * time.Second,
        EnableCleanup:   false,
        CleanupCycle:    24 * time.Hour,
    }
}
```

## Usage Examples

### 1. Basic Setup
```go
// Create components
storage := db.NewPostgresDB(connStr, 10, 5)
aggregator := candle.NewAggregator(storage)
exchanges := map[string]candle.Exchange{
    "wallex": wallexExchange,
}

// Configure ingestion
config := candle.DefaultIngestionConfig()
config.Symbols = []string{"BTCUSDT", "ETHUSDT"}

// Create and start service
ctx := context.Background()
ingestionService := candle.NewIngestionService(ctx, storage, aggregator, exchanges, config)
ingestionService.Start()
```

### 2. Ingesting Raw 1m Candles
```go
// Create ingester
ingester := candle.NewCandleIngester(storage)

// Ingest raw 1m candles
raw1mCandles := []candle.Candle{
    {
        Timestamp: time.Now().Truncate(time.Minute),
        Open:      100.0,
        High:      101.0,
        Low:       99.0,
        Close:     100.5,
        Volume:    1000.0,
        Symbol:    "BTCUSDT",
        Timeframe: "1m",
        Source:    "wallex",
    },
    // ... more candles
}

ctx := context.Background()
err := ingester.IngestRaw1mCandles(ctx, raw1mCandles)
```

### 3. Manual Aggregation
```go
// Aggregate 1m candles to 5m
ctx := context.Background()
oneMCandles, err := storage.GetRawCandles(ctx, "BTCUSDT", "1m", start, end)
if err != nil {
    return err
}

agg5m, err := aggregator.AggregateFrom1m(oneMCandles, "5m")
if err != nil {
    return err
}

// Save as constructed candles
err = storage.SaveConstructedCandles(ctx, agg5m)
```

### 4. Bulk Historical Aggregation
```go
// Aggregate last 7 days of 1m data
ctx := context.Background()
end := time.Now()
start := end.AddDate(0, 0, -7)

err := ingester.BulkAggregateFrom1m(ctx, "BTCUSDT", start, end)
```

### 5. Retrieving Constructed vs Raw Candles
```go
ctx := context.Background()

// Get only constructed candles
constructed, err := storage.GetConstructedCandles(ctx, "BTCUSDT", "5m", start, end)
if err != nil {
    return err
}

// Get only raw candles
raw, err := storage.GetRawCandles(ctx, "BTCUSDT", "1m", start, end)
if err != nil {
    return err
}

// Get all candles (both raw and constructed)
all, err := storage.GetCandles(ctx, "BTCUSDT", "5m", start, end)
if err != nil {
    return err
}
```

## Monitoring and Statistics

### 1. Source Distribution Statistics
```go
ctx := context.Background()
stats, err := storage.GetCandleSourceStats(ctx, "BTCUSDT", start, end)
// Returns:
// {
//   "source_distribution": {
//     "wallex": {
//       "1m": 1440,
//       "5m": 0,
//       "15m": 0
//     },
//     "constructed": {
//       "5m": 288,
//       "15m": 96,
//       "1h": 24
//     }
//   },
//   "constructed_count": 408,
//   "raw_count": 1440,
//   "total_count": 1848
// }
```

### 2. Aggregation Statistics
```go
ctx := context.Background()
aggStats, err := storage.GetAggregationStats(ctx, "BTCUSDT")
// Returns:
// {
//   "latest_1m": {
//     "timestamp": "2024-01-01T10:00:00Z",
//     "close": 100.50,
//     "is_complete": true
//   },
//   "count_24h_1m": 1440,
//   "count_24h_5m": 288,
//   "count_24h_15m": 96,
//   "count_24h_1h": 24,
//   "count_24h_4h": 6,
//   "count_24h_1d": 1
// }
```

## Performance Optimizations

### 1. Database Optimizations
- **Batch Processing**: Multiple candles in single transactions
- **Prepared Statements**: Reusable prepared statements for efficiency
- **Indexed Queries**: Optimized indexes on symbol, timeframe, timestamp, source
- **TimescaleDB**: Time-series optimized storage with hypertables

### 2. Memory Optimizations
- **Caching Strategy**: In-memory cache for latest candles
- **Efficient Data Structures**: Optimized maps for symbol/timeframe lookup
- **Source-based Caching**: Cache constructed vs raw candles separately

### 3. Aggregation Optimizations
- **Incremental Updates**: Only update existing candles when possible
- **Path Calculation**: Pre-calculated aggregation paths
- **Batch Operations**: Process multiple timeframes in single operations

## Troubleshooting

### Common Issues

1. **Missing Candles**
   ```bash
   # Check for gaps
   SELECT symbol, timeframe, COUNT(*) as count, 
          MIN(timestamp) as earliest, MAX(timestamp) as latest
   FROM candles 
   WHERE symbol='BTCUSDT' AND timeframe='1m'
   GROUP BY symbol, timeframe;
   ```

2. **Mixed Source Data**
   ```bash
   # Check for mixed sources
   SELECT symbol, timeframe, source, COUNT(*) as count
   FROM candles 
   WHERE symbol='BTCUSDT' AND timeframe='5m'
   GROUP BY symbol, timeframe, source
   ORDER BY source;
   ```

3. **Missing Constructed Candles**
   ```bash
   # Check constructed candle counts
   SELECT symbol, timeframe, COUNT(*) as count
   FROM candles 
   WHERE symbol='BTCUSDT' AND source='constructed'
   GROUP BY symbol, timeframe
   ORDER BY timeframe;
   ```

### Debug Commands
```bash
# Run with debug logging
./simple-trader --mode=live --symbols=BTCUSDT --timeframes=1m --debug

# Check source statistics
curl http://localhost:8080/candles/source-stats/BTCUSDT

# View constructed candles
psql -d trading_db -c "SELECT * FROM candles WHERE source='constructed' ORDER BY timestamp DESC LIMIT 10;"
```

## Integration with Trading System

### Live Trading Mode
```go
// In live trading, the system automatically:
// 1. Fetches raw 1m candles from exchanges (source="wallex")
// 2. Stores raw candles in database
// 3. Aggregates to higher timeframes (source="constructed")
// 4. Stores constructed candles in database
// 5. Provides both raw and constructed data to strategies
```

### Backtesting Mode
```go
// For backtesting:
// 1. Loads historical raw candles from database
// 2. Aggregates to required timeframes (source="constructed")
// 3. Provides constructed data to backtesting engine
// 4. Maintains source distinction for analysis
```

## Future Enhancements

1. **WebSocket Integration**: Real-time candle streaming
2. **Multi-Exchange Aggregation**: Combine data from multiple sources
3. **Advanced Caching**: Redis-based distributed caching
4. **Machine Learning**: Predictive candle generation
5. **Custom Timeframes**: User-defined aggregation rules
6. **Source Quality Metrics**: Quality scores for different sources
7. **Compression**: Efficient storage compression for historical data

## Conclusion

The candle ingestion and storage system provides:

- **High Performance**: Efficient storage and retrieval of candles
- **Data Integrity**: Comprehensive validation and error handling
- **Clear Data Distinction**: Separation between raw and constructed candles
- **Real-time Processing**: Immediate aggregation to higher timeframes
- **Scalability**: Optimized for high-frequency data processing
- **Monitoring**: Comprehensive statistics and monitoring capabilities
- **Flexibility**: Support for multiple timeframes and aggregation strategies

This system forms the foundation for accurate technical analysis and algorithmic trading across multiple timeframes, ensuring data consistency and reliability for both live trading and backtesting scenarios. 
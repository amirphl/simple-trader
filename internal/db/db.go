// Package db
package db

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/amirphl/simple-trader/internal/tfutils"
)

type Candle struct {
	Timestamp time.Time `json:"timestamp"`
	Open      float64   `json:"open"`
	High      float64   `json:"high"`
	Low       float64   `json:"low"`
	Close     float64   `json:"close"`
	Volume    float64   `json:"volume"`
	Symbol    string    `json:"symbol"`
	Timeframe string    `json:"timeframe"`
	Source    string    `json:"source"`
}

// Validate checks if a candle has valid data
func (c *Candle) Validate() error {
	if c.Timestamp.IsZero() {
		return errors.New("candle timestamp is zero")
	}
	if c.Open <= 0 || c.High <= 0 || c.Low <= 0 || c.Close <= 0 {
		return errors.New("candle prices must be positive")
	}
	if c.High < c.Low {
		return errors.New("candle high cannot be less than low")
	}
	if c.Open < c.Low || c.Open > c.High {
		return errors.New("candle open price must be between high and low")
	}
	if c.Close < c.Low || c.Close > c.High {
		return errors.New("candle close price must be between high and low")
	}
	if c.Volume < 0 {
		return errors.New("candle volume cannot be negative")
	}
	if c.Symbol == "" {
		return errors.New("candle symbol cannot be empty")
	}
	if c.Timeframe == "" {
		return errors.New("candle timeframe cannot be empty")
	}
	return nil
}

// IsComplete checks if a candle is complete (not the current minute)
func (c *Candle) IsComplete() bool {
	now := time.Now().UTC()
	candleEnd := c.Timestamp.Add(tfutils.GetTimeframeDuration(c.Timeframe))
	return now.After(candleEnd)
}

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

// SetConstructed marks the candle as constructed
func (c *Candle) SetConstructed() {
	c.Source = "constructed"
}

// SetRaw marks the candle as raw with the given source
func (c *Candle) SetRaw(source string) {
	if source == "" {
		source = "unknown"
	}
	c.Source = source
}

// Storage interface for saving and retrieving candles.
type CandleStorage interface {
	SaveCandle(ctx context.Context, candle Candle) error
	SaveCandles(ctx context.Context, candles []Candle) error
	SaveConstructedCandles(ctx context.Context, candles []Candle) error
	GetCandle(ctx context.Context, symbol, timeframe string, timestamp time.Time, source string) (*Candle, error)
	GetCandles(ctx context.Context, symbol, timeframe, source string, start, end time.Time) ([]Candle, error)
	GetCandlesV2(ctx context.Context, timeframe string, start, end time.Time) ([]Candle, error)
	GetRawCandles(ctx context.Context, symbol, timeframe string, start, end time.Time) ([]Candle, error)
	GetLatestCandle(ctx context.Context, symbol, timeframe string) (*Candle, error)
	GetLatestCandleInRange(ctx context.Context, symbol, timeframe string, start, end time.Time) (*Candle, error)
	GetLatestConstructedCandle(ctx context.Context, symbol, timeframe string) (*Candle, error)
	DeleteCandles(ctx context.Context, symbol, timeframe string, before time.Time) error
	DeleteCandlesInRange(ctx context.Context, symbol, timeframe, source string, start, end time.Time) error
	DeleteConstructedCandles(ctx context.Context, symbol, timeframe string, before time.Time) error
	GetCandleCount(ctx context.Context, symbol, timeframe string, start, end time.Time) (int, error)
	GetConstructedCandleCount(ctx context.Context, symbol, timeframe string, start, end time.Time) (int, error)
	UpdateCandle(ctx context.Context, candle Candle) error
	UpdateCandles(ctx context.Context, candle []Candle) error
	GetAggregationStats(ctx context.Context, symbol string) (map[string]any, error)
	GetMissingCandleRanges(ctx context.Context, symbol string, start, end time.Time) ([]struct{ Start, End time.Time }, error)
	GetCandleSourceStats(ctx context.Context, symbol string, start, end time.Time) (map[string]any, error)
}

// Order represents the response from the exchange.
type Order struct {
	OrderID   string    `json:"order_id"`
	Status    string    `json:"status"`
	FilledQty float64   `json:"filled_qty"`
	AvgPrice  float64   `json:"avg_price"`
	Timestamp time.Time `json:"timestamp"`
	Symbol    string    `json:"symbol"`
	Side      string    `json:"side"`
	Type      string    `json:"type"`
	Price     float64   `json:"price"`
	Quantity  float64   `json:"quantity"`
	UpdatedAt time.Time `json:"updated_at"`
}

// OrderStorage interface for managing order lifecycle.
type OrderStorage interface {
	GetOrder(ctx context.Context, orderID string) (*Order, error)
	GetOpenOrders(ctx context.Context) ([]Order, error)
	SaveOrder(ctx context.Context, order Order) error
	CloseOrder(ctx context.Context, orderID string) error
}

// OrderBook represents the L2 orderbook snapshot.
type OrderBook struct {
	Symbol    string       `json:"symbol"`
	Bids      [][2]float64 `json:"bids"` // price, quantity
	Asks      [][2]float64 `json:"asks"`
	Timestamp time.Time    `json:"timestamp"`
}

// Tick represents a trade tick.
type Tick struct {
	Symbol    string    `json:"symbol"`
	Price     float64   `json:"price"`
	Quantity  float64   `json:"quantity"`
	Side      string    `json:"side"`
	Timestamp time.Time `json:"timestamp"`
}

// MarketStorage interface for managing orderbook and tick storage.
type MarketStorage interface {
	SaveOrderBook(ctx context.Context, ob OrderBook) error
	SaveTick(ctx context.Context, tick Tick) error
	GetOrderBooks(ctx context.Context, symbol string, start, end time.Time) ([]OrderBook, error)
	GetTicks(ctx context.Context, symbol string, start, end time.Time) ([]Tick, error)
}

// Event represents a journaled event.
type Event struct {
	Time        time.Time      `json:"time"`
	Type        string         `json:"type"` // e.g., "order", "signal", "error", etc.
	Description string         `json:"description"`
	Data        map[string]any `json:"data"`
}

// JournalStorage interface for journaling events.
type JournalStorage interface {
	LogEvent(ctx context.Context, event Event) error
	GetEvents(ctx context.Context, eventType string, start, end time.Time) ([]Event, error)
}

// PositionData represents the data structure for position persistence
type Position struct {
	StrategyName    string                 `json:"strategy_name"`
	Symbol          string                 `json:"symbol"`
	Side            string                 `json:"side"`
	Entry           float64                `json:"entry"`
	Size            float64                `json:"size"`
	OrderID         string                 `json:"order_id"`
	Time            time.Time              `json:"time"`
	Active          bool                   `json:"active"`
	TradingDisabled bool                   `json:"trading_disabled"`
	Balance         float64                `json:"balance"`
	LastPNL         float64                `json:"last_pnl"`
	MeanPNL         float64                `json:"mean_pnl"`
	StdPNL          float64                `json:"std_pnl"`
	Sharpe          float64                `json:"sharpe"`
	Expectancy      float64                `json:"expectancy"`
	TrailingStop    float64                `json:"trailing_stop"`
	LiveEquity      float64                `json:"live_equity"`
	LiveMaxEquity   float64                `json:"live_max_equity"`
	LiveMaxDrawdown float64                `json:"live_max_drawdown"`
	LiveWins        int64                  `json:"live_wins"`
	LiveLosses      int64                  `json:"live_losses"`
	LiveTrades      int64                  `json:"live_trades"`
	LiveWinRate     float64                `json:"live_win_rate"`
	ProfitFactor    float64                `json:"profit_factor"`
	LiveWinPnls     []float64              `json:"live_win_pnls"`
	LiveLossPnls    []float64              `json:"live_loss_pnls"`
	LiveEquityCurve []float64              `json:"live_equity_curve"`
	LiveTradeLog    []Trade                `json:"live_trade_log"`
	RiskParams      map[string]interface{} `json:"risk_params"`
	OrderSpec       map[string]interface{} `json:"order_spec"`
	CreatedAt       time.Time              `json:"created_at"`
	UpdatedAt       time.Time              `json:"updated_at"`
}

// TradeData represents a single trade record
type Trade struct {
	Entry     float64   `json:"entry"`
	Exit      float64   `json:"exit"`
	PnL       float64   `json:"pnl"`
	EntryTime time.Time `json:"entry_time"`
	ExitTime  time.Time `json:"exit_time"`
}

// PositionStorage defines the interface for position persistence operations
type PositionStorage interface {
	SavePosition(ctx context.Context, pos Position) error
	UpdatePosition(ctx context.Context, pos Position) error
	GetPosition(ctx context.Context, strategyName, symbol string) (*Position, error)
	GetAllPositions(ctx context.Context) ([]Position, error)
	GetActivePositions(ctx context.Context) ([]Position, error)
	DeletePosition(ctx context.Context, strategyName, symbol string) error
}

// Storage is the interface for all persistent storage.
type Storage interface {
	GetDB() *sql.DB
	CandleStorage
	OrderStorage
	MarketStorage
	JournalStorage
	PositionStorage
}

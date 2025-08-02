// Package exchange
package exchange

import (
	"context"
	"errors"
	"time"
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

// OrderRequest represents a new order to be submitted.
type OrderRequest struct {
	Symbol      string         `json:"symbol"`
	Side        string         `json:"side"` // "buy" or "sell"
	Type        string         `json:"type"` // "limit", "market", "stop-limit", etc.
	Price       float64        `json:"price"`
	Quantity    float64        `json:"quantity"`
	StopPrice   float64        `json:"stop_price"`   // For stop-limit orders
	AlgoType    string         `json:"algo_type"`    // For advanced orders: "OCO", "STOP", etc.
	ChildOrders []OrderRequest `json:"child_orders"` // For OCO and bracket orders
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

// Tick represents a trade tick.
type Tick struct {
	Symbol    string    `json:"symbol"`
	Price     float64   `json:"price"`
	Quantity  float64   `json:"quantity"`
	Side      string    `json:"side"`
	Timestamp time.Time `json:"timestamp"`
}

// Balance represents an asset balance from an exchange
type Balance struct {
	Asset     string  `json:"asset"`     // Asset symbol (e.g., "BTC", "USDT")
	Available float64 `json:"available"` // Available balance for trading
	Locked    float64 `json:"locked"`    // Balance locked in orders
	Total     float64 `json:"total"`     // Total balance (available + locked)
	Fiat      bool    `json:"fiat"`      // Whether this is a fiat currency
}

// Exchange is the interface for all supported exchanges.
type Exchange interface {
	Name() string
	FetchCandles(ctx context.Context, symbol string, timeframe string, start, end time.Time) ([]Candle, error)
	FetchLatestCandles(ctx context.Context, symbol string, timeframe string, count int) ([]Candle, error)
	SubmitOrder(ctx context.Context, req OrderRequest) (Order, error)
	SubmitOrderWithRetry(ctx context.Context, req OrderRequest, maxAttempts int, delay time.Duration) (Order, error)
	CancelOrder(ctx context.Context, orderID string) error
	GetOrderStatus(ctx context.Context, orderID string) (Order, error)
	FetchBalances(ctx context.Context) (map[string]Balance, error)
	FetchOrderBook(ctx context.Context, symbol string) (map[string]OrderBook, error)
	FetchLatestTick(ctx context.Context, symbol string) (Tick, error)
	FetchMarketStats(ctx context.Context) (map[string]MarketCap, error)
}

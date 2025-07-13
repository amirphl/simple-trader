// Package market
package market

import (
	"context"
	"time"
)

// OrderBook represents the L2 orderbook snapshot.
type OrderBook struct {
	Symbol    string
	Bids      [][2]float64 // price, quantity
	Asks      [][2]float64
	Timestamp time.Time
}

// Tick represents a trade tick.
type Tick struct {
	Symbol    string
	Price     float64
	Quantity  float64
	Side      string // "buy" or "sell"
	Timestamp time.Time
}

// MarketManager interface for managing orderbook and tick storage.
type MarketManager interface {
	SaveOrderBook(ctx context.Context, ob OrderBook) error
	SaveTick(ctx context.Context, tick Tick) error
	GetOrderBooks(ctx context.Context, symbol string, start, end time.Time) ([]OrderBook, error)
	GetTicks(ctx context.Context, symbol string, start, end time.Time) ([]Tick, error)
}

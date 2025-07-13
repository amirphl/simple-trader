// Package exchange
package exchange

import (
	"context"
	"time"

	"github.com/amirphl/simple-trader/internal/candle"
	"github.com/amirphl/simple-trader/internal/market"
	"github.com/amirphl/simple-trader/internal/order"
)

// TODO: Add ctx

// Exchange is the interface for all supported exchanges.
type Exchange interface {
	Name() string
	FetchCandles(ctx context.Context, symbol string, timeframe string, start, end int64) ([]candle.Candle, error)
	FetchCandlesWithRetry(ctx context.Context, symbol string, timeframe string, start, end int64, maxRetries int, retryDelay time.Duration) ([]candle.Candle, error)
	FetchLatestCandles(ctx context.Context, symbol string, timeframe string, count int) ([]candle.Candle, error)
	FetchOrderBook(symbol string) (market.OrderBook, error)
	SubmitOrder(order order.OrderRequest) (order.OrderResponse, error)
	SubmitOrderWithRetry(req order.OrderRequest, maxAttempts int, delay time.Duration) (order.OrderResponse, error)
	CancelOrder(orderID string) error
	FetchTick(symbol string) (market.Tick, error)
	// Add more methods as needed for extensibility
	FetchTicks(symbol string, from, to time.Time) ([]market.Tick, error)
	GetOrderStatus(orderID string) (order.OrderResponse, error)
}

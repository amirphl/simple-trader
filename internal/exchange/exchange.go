// Package exchange
package exchange

import (
	"context"
	"time"

	"github.com/amirphl/simple-trader/internal/candle"
	"github.com/amirphl/simple-trader/internal/market"
	"github.com/amirphl/simple-trader/internal/order"
)

// Exchange is the interface for all supported exchanges.
type Exchange interface {
	Name() string
	FetchCandles(ctx context.Context, symbol string, timeframe string, start, end int64) ([]candle.Candle, error)
	FetchLatestCandles(ctx context.Context, symbol string, timeframe string, count int) ([]candle.Candle, error)
	FetchOrderBook(ctx context.Context, symbol string) (market.OrderBook, error)
	SubmitOrder(ctx context.Context, order order.OrderRequest) (order.OrderResponse, error)
	SubmitOrderWithRetry(ctx context.Context, req order.OrderRequest, maxAttempts int, delay time.Duration) (order.OrderResponse, error)
	CancelOrder(ctx context.Context, orderID string) error
	FetchTick(ctx context.Context, symbol string) (market.Tick, error)
	FetchTicks(ctx context.Context, symbol string, from, to time.Time) ([]market.Tick, error)
	GetOrderStatus(ctx context.Context, orderID string) (order.OrderResponse, error)
}

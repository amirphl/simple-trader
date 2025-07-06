// Package exchange
package exchange

import (
	"github.com/amirphl/simple-trader/internal/candle"
	"github.com/amirphl/simple-trader/internal/market"
	"github.com/amirphl/simple-trader/internal/order"
)

// Exchange is the interface for all supported exchanges.
type Exchange interface {
	Name() string
	FetchCandles(symbol string, timeframe string, start, end int64) ([]candle.Candle, error)
	FetchOrderBook(symbol string) (market.OrderBook, error)
	SubmitOrder(order order.OrderRequest) (order.OrderResponse, error)
	CancelOrder(orderID string) error
	FetchTick(symbol string) (market.Tick, error)
	// Add more methods as needed for extensibility
	FetchTicks(symbol string, from, to int64) ([]market.Tick, error)
	GetOrderStatus(orderID string) (order.OrderResponse, error)
}

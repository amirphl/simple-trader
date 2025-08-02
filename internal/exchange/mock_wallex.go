// Package exchange
package exchange

import (
	"context"
	"fmt"
	"time"

	"github.com/amirphl/simple-trader/internal/notifier"
	"github.com/amirphl/simple-trader/internal/utils"
)

// MockWallexExchange acts as a proxy for some functions and provides mock responses for orders
type MockWallexExchange struct {
	realExchange Exchange // The real WallexExchange to proxy to
	notifier     notifier.Notifier
	orderCounter int64 // Counter for generating unique order IDs
}

// NewMockWallexExchange creates a new MockWallexExchange that proxies to a real WallexExchange
func NewMockWallexExchange(apiKey string, n notifier.Notifier) Exchange {
	realExchange := NewWallexExchange(apiKey, n)
	return &MockWallexExchange{
		realExchange: realExchange,
		notifier:     n,
		orderCounter: 1000, // Start from 1000 for mock order IDs
	}
}

func (m *MockWallexExchange) Name() string {
	return "mock-wallex"
}

// ===== PROXY FUNCTIONS - These call the real WallexExchange =====

func (m *MockWallexExchange) FetchCandles(ctx context.Context, symbol string, timeframe string, start, end time.Time) ([]Candle, error) {
	return m.realExchange.FetchCandles(ctx, symbol, timeframe, start, end)
}

func (m *MockWallexExchange) FetchLatestCandles(ctx context.Context, symbol string, timeframe string, count int) ([]Candle, error) {
	return m.realExchange.FetchLatestCandles(ctx, symbol, timeframe, count)
}

func (m *MockWallexExchange) GetOrderStatus(ctx context.Context, orderID string) (Order, error) {
	return m.realExchange.GetOrderStatus(ctx, orderID)
}

func (m *MockWallexExchange) FetchBalances(ctx context.Context) (map[string]Balance, error) {
	return m.realExchange.FetchBalances(ctx)
}

// ===== MOCK FUNCTIONS - These provide mock responses =====

func (m *MockWallexExchange) SubmitOrder(ctx context.Context, req OrderRequest) (Order, error) {
	select {
	case <-ctx.Done():
		return Order{}, ctx.Err()
	default:
		// Only support limit orders for now
		if req.Type != "limit" {
			return Order{}, fmt.Errorf("mock exchange: order type '%s' not implemented yet, only 'limit' orders are supported", req.Type)
		}

		// Generate a unique order ID
		m.orderCounter++
		orderID := fmt.Sprintf("mock_%d_%d", time.Now().Unix(), m.orderCounter)

		// Create a successful order response
		order := Order{
			OrderID:   orderID,
			Status:    "FILLED",     // Assume immediate fill for limit orders
			FilledQty: req.Quantity, // Full quantity filled
			AvgPrice:  req.Price,    // Filled at exact requested price
			Timestamp: time.Now().UTC(),
			Symbol:    req.Symbol,
			Side:      req.Side,
			Type:      req.Type,
			Price:     req.Price,
			Quantity:  req.Quantity,
			UpdatedAt: time.Now().UTC(),
		}

		// Log the mock order for debugging
		utils.GetLogger().Printf("MockWallexExchange | Mock order filled: OrderID=%s, Symbol=%s, Side=%s, Price=%.8f, Quantity=%.8f\n",
			orderID, req.Symbol, req.Side, req.Price, req.Quantity)

		return order, nil
	}
}

func (m *MockWallexExchange) SubmitOrderWithRetry(ctx context.Context, req OrderRequest, maxAttempts int, delay time.Duration) (Order, error) {
	// For mock exchange, we don't need retries since we always succeed (for limit orders)
	// But we'll implement the same logic as the real exchange for consistency
	var resp Order
	var err error

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		resp, err = m.SubmitOrder(ctx, req)
		if err == nil {
			return resp, nil
		}

		utils.GetLogger().Printf("MockWallexExchange | Mock order submission failed (attempt %d/%d): %v\n", attempt, maxAttempts, err)

		if m.notifier != nil {
			msg := fmt.Sprintf("Mock order submission failed (attempt %d/%d): %v", attempt, maxAttempts, err)
			m.notifier.SendWithRetry(msg)
		}

		// Don't sleep on the last attempt
		if attempt < maxAttempts {
			time.Sleep(delay)
		}
	}

	return resp, err
}

func (m *MockWallexExchange) CancelOrder(ctx context.Context, orderID string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return fmt.Errorf("mock exchange: CancelOrder not implemented yet")
	}
}

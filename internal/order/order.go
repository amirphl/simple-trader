// Package order
package order

import "time"

// OrderRequest represents a new order to be submitted.
type OrderRequest struct {
	Symbol      string
	Side        string // "buy" or "sell"
	Type        string // "limit", "market", "stop-limit", etc.
	Price       float64
	Quantity    float64
	StopPrice   float64        // For stop-limit orders
	AlgoType    string         // For advanced orders: "OCO", "STOP", etc.
	ChildOrders []OrderRequest // For OCO and bracket orders
}

// OrderResponse represents the response from the exchange.
type OrderResponse struct {
	OrderID   string
	Status    string
	FilledQty float64
	AvgPrice  float64
	Timestamp time.Time
	Symbol    string
	Side      string
	Type      string
	Price     float64
	Quantity  float64
	UpdatedAt time.Time
}

// TODO: Add ctx support

// OrderManager interface for managing order lifecycle.
type OrderManager interface {
	SubmitOrder(order OrderRequest) (OrderResponse, error)
	CancelOrder(orderID string) error
	GetOrder(orderID string) (OrderResponse, error)
	GetOpenOrders() ([]OrderResponse, error)
	SaveOrder(order OrderResponse) error
}

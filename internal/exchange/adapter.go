// Package exchange adapter
package exchange

import (
	"strconv"

	"github.com/amirphl/simple-trader/internal/db"
)

func (w *WallexTrade) ToTick(symbol string) Tick {
	price, _ := strconv.ParseFloat(w.Price, 64)
	quantity, _ := strconv.ParseFloat(w.Quantity, 64)
	side := "buy"
	if !w.IsBuyOrder {
		side = "sell"
	}

	return Tick{
		Symbol:    symbol,
		Price:     price,
		Quantity:  quantity,
		Side:      side,
		Timestamp: w.Timestamp,
	}
}

func OrderToDBOrder(order Order) db.Order {
	return db.Order{
		OrderID:   order.OrderID,
		Status:    order.Status,
		FilledQty: order.FilledQty,
		AvgPrice:  order.AvgPrice,
		Timestamp: order.Timestamp,
		Symbol:    order.Symbol,
		Side:      order.Side,
		Type:      order.Type,
		Price:     order.AvgPrice,
		Quantity:  order.Quantity,
		UpdatedAt: order.UpdatedAt,
	}
}

// Package exchange adapter
package exchange

import (
	"strconv"

	"github.com/amirphl/simple-trader/internal/db"
)

func WallexTradeToTick(trade WallexTrade, symbol string) Tick {
	price, _ := strconv.ParseFloat(trade.Price, 64)
	quantity, _ := strconv.ParseFloat(trade.Quantity, 64)
	side := "buy"
	if !trade.IsBuyOrder {
		side = "sell"
	}

	return Tick{
		Symbol:    symbol,
		Price:     price,
		Quantity:  quantity,
		Side:      side,
		Timestamp: trade.Timestamp,
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

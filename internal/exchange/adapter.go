// Package exchange adapter
package exchange

import "strconv"

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

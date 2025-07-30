// Package candle adapter
package candle

import "github.com/amirphl/simple-trader/internal/db"

func DBCandleToCandle(dbCandle db.Candle) Candle {
	return Candle{
		Timestamp: dbCandle.Timestamp,
		Open:      dbCandle.Open,
		High:      dbCandle.High,
		Low:       dbCandle.Low,
		Close:     dbCandle.Close,
		Volume:    dbCandle.Volume,
		Symbol:    dbCandle.Symbol,
		Timeframe: dbCandle.Timeframe,
		Source:    dbCandle.Source,
	}
}

func DBCandlesToCandles(dbCandles []db.Candle) []Candle {
	candles := make([]Candle, len(dbCandles))
	for i, dbCandle := range dbCandles {
		candles[i] = DBCandleToCandle(dbCandle)
	}
	return candles
}

func CandleToDbCandle(candle Candle) db.Candle {
	return db.Candle{
		Timestamp: candle.Timestamp,
		Open:      candle.Open,
		High:      candle.High,
		Low:       candle.Low,
		Close:     candle.Close,
		Volume:    candle.Volume,
		Symbol:    candle.Symbol,
		Timeframe: candle.Timeframe,
		Source:    candle.Source,
	}
}

func CandlesToDbCandles(candles []Candle) []db.Candle {
	dbCandles := make([]db.Candle, len(candles))
	for i, candle := range candles {
		dbCandles[i] = CandleToDbCandle(candle)
	}
	return dbCandles
}

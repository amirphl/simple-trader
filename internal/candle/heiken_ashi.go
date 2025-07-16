// Package candle
package candle

// GenerateHeikenAshiCandles generates Heiken Ashi candles from raw candles.
// Input candles must be sorted by timestamp ascending.
func GenerateHeikenAshiCandles(rawCandles []Candle) []Candle {
	if len(rawCandles) == 0 {
		return nil
	}

	max3 := func(a, b, c float64) float64 {
		m := a
		if b > m {
			m = b
		}
		if c > m {
			m = c
		}
		return m
	}
	min3 := func(a, b, c float64) float64 {
		m := a
		if b < m {
			m = b
		}
		if c < m {
			m = c
		}
		return m
	}

	haCandles := make([]Candle, len(rawCandles))
	var prevHaOpen, prevHaClose float64

	for i, c := range rawCandles {
		ha := c // copy base fields
		ha.Close = (c.Open + c.High + c.Low + c.Close) / 4
		if i == 0 {
			ha.Open = (c.Open + c.Close) / 2
		} else {
			ha.Open = (prevHaOpen + prevHaClose) / 2
		}
		ha.High = max3(c.High, ha.Open, ha.Close)
		ha.Low = min3(c.Low, ha.Open, ha.Close)
		ha.Source = "heiken_ashi"

		haCandles[i] = ha
		prevHaOpen = ha.Open
		prevHaClose = ha.Close
	}
	return haCandles
}

// GenerateNextHeikenAshiCandle generates the next Heiken Ashi candle given the previous Heiken Ashi candle and a new raw candle.
// prevHA: previous Heiken Ashi candle (can be nil for the first candle)
// raw: new raw candle
// Returns the new Heiken Ashi candle.
func GenerateNextHeikenAshiCandle(prevHA *Candle, raw Candle) Candle {
	ha := raw // copy base fields
	ha.Close = (raw.Open + raw.High + raw.Low + raw.Close) / 4
	if prevHA == nil {
		ha.Open = (raw.Open + raw.Close) / 2
	} else {
		ha.Open = (prevHA.Open + prevHA.Close) / 2
	}
	// Use local max3/min3 helpers
	max3 := func(a, b, c float64) float64 {
		m := a
		if b > m {
			m = b
		}
		if c > m {
			m = c
		}
		return m
	}
	min3 := func(a, b, c float64) float64 {
		m := a
		if b < m {
			m = b
		}
		if c < m {
			m = c
		}
		return m
	}
	ha.High = max3(raw.High, ha.Open, ha.Close)
	ha.Low = min3(raw.Low, ha.Open, ha.Close)
	ha.Source = "heiken_ashi"
	return ha
}

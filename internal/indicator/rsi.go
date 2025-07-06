package indicator

import "math"

func CalculateRSI(prices []float64, period int) []float64 {
	if len(prices) < period || period <= 0 {
		return nil
	}
	rsi := make([]float64, len(prices))
	// Initialize first period-1 elements as NaN (invalid)
	for i := 0; i < period-1; i++ {
		rsi[i] = math.NaN()
	}
	var gain, loss float64
	// Calculate initial gains and losses
	for i := 1; i < period; i++ {
		change := prices[i] - prices[i-1]
		if change > 0 {
			gain += change
		} else {
			loss += -change // Fixed: Accumulate absolute value of loss
		}
	}
	avgGain := gain / float64(period)
	avgLoss := loss / float64(period)
	// Calculate first RSI
	if avgLoss == 0 {
		rsi[period-1] = 100
	} else {
		rs := avgGain / avgLoss
		rsi[period-1] = 100 - (100 / (1 + rs))
	}
	// Calculate subsequent RSIs
	for i := period; i < len(prices); i++ {
		change := prices[i] - prices[i-1]
		if change > 0 {
			gain = change
			loss = 0
		} else {
			gain = 0
			loss = -change
		}
		avgGain = (avgGain*float64(period-1) + gain) / float64(period)
		avgLoss = (avgLoss*float64(period-1) + loss) / float64(period)
		if avgLoss == 0 {
			rsi[i] = 100
		} else {
			rs := avgGain / avgLoss
			rsi[i] = 100 - (100 / (1 + rs))
		}
	}
	return rsi
}

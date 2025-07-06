// Package indicator
package indicator

import (
	"fmt"
	"math"
)

func CalculateRSI(prices []float64, period int) []float64 {
	if len(prices) <= period || period <= 0 {
		return nil
	}

	rsi := make([]float64, len(prices))
	// Initialize first period elements as NaN (invalid)
	for i := range period {
		rsi[i] = math.NaN()
	}

	// Calculate first average gain and loss
	var sumGain, sumLoss float64
	for i := 1; i <= period; i++ {
		change := prices[i] - prices[i-1]
		if change > 0 {
			sumGain += change
		} else {
			sumLoss += math.Abs(change) // Use absolute value for loss
		}
	}

	// Calculate first RSI value
	avgGain := sumGain / float64(period)
	avgLoss := sumLoss / float64(period)

	if avgLoss == 0 {
		rsi[period] = 100
	} else {
		rs := avgGain / avgLoss
		rsi[period] = 100 - (100 / (1 + rs))
	}

	// Calculate subsequent RSIs using the smoothed method
	for i := period + 1; i < len(prices); i++ {
		change := prices[i] - prices[i-1]
		var currentGain, currentLoss float64
		if change > 0 {
			currentGain = change
			currentLoss = 0
		} else {
			currentGain = 0
			currentLoss = math.Abs(change)
		}

		// Use proper Wilder's smoothing formula
		avgGain = ((avgGain * float64(period-1)) + currentGain) / float64(period)
		avgLoss = ((avgLoss * float64(period-1)) + currentLoss) / float64(period)

		if avgLoss == 0 {
			rsi[i] = 100
		} else {
			rs := avgGain / avgLoss
			rsi[i] = 100 - (100 / (1 + rs))
		}
	}

	return rsi
}

// CalculateLastRSI efficiently calculates only the last RSI value
// This is much more efficient when we only need the current RSI value
func CalculateLastRSI(prices []float64, period int) (float64, error) {
	if len(prices) <= period || period <= 0 {
		return 0, fmt.Errorf("insufficient data: need more than %d prices", period)
	}

	// Calculate first average gain and loss
	var sumGain, sumLoss float64
	for i := 1; i <= period; i++ {
		change := prices[i] - prices[i-1]
		if change > 0 {
			sumGain += change
		} else {
			sumLoss += math.Abs(change) // Use absolute value for loss
		}
	}

	avgGain := sumGain / float64(period)
	avgLoss := sumLoss / float64(period)

	// Apply smoothing formula for remaining prices
	for i := period + 1; i < len(prices); i++ {
		change := prices[i] - prices[i-1]
		var currentGain, currentLoss float64
		if change > 0 {
			currentGain = change
			currentLoss = 0
		} else {
			currentGain = 0
			currentLoss = math.Abs(change)
		}

		avgGain = ((avgGain * float64(period-1)) + currentGain) / float64(period)
		avgLoss = ((avgLoss * float64(period-1)) + currentLoss) / float64(period)
	}

	if avgLoss == 0 {
		return 100, nil
	}

	rs := avgGain / avgLoss
	return 100 - (100 / (1 + rs)), nil
}


// Package indicator provides technical analysis indicators for financial markets
package indicator

import (
	"fmt"
	"math"

	"github.com/amirphl/simple-trader/internal/candle"
)

// StochasticResult holds the results of stochastic oscillator calculation
type StochasticResult struct {
	K []float64 // %K line values
	D []float64 // %D line values
}

// CalculateStochastic calculates the Stochastic Oscillator (%K and %D) for the given candle data
// This implementation matches Pine Script's ta.stoch() function behavior:
// k = ta.sma(ta.stoch(close, high, low, periodK), smoothK)
// d = ta.sma(k, periodD)
//
// Parameters:
// - candles: Array of candle data
// - periodK: The lookback period for %K calculation (%K Length, default 14)
// - smoothK: The smoothing period for %K line (%K Smoothing, default 1)
// - periodD: The smoothing period for %D line (%D Smoothing, default 3)
func CalculateStochastic(candles []candle.Candle, periodK, smoothK, periodD int) (*StochasticResult, error) {
	// Validate inputs
	if len(candles) == 0 {
		return nil, fmt.Errorf("candles array cannot be empty")
	}

	if len(candles) < periodK {
		return nil, fmt.Errorf("insufficient data: need at least %d candles for periodK", periodK)
	}

	if periodK <= 0 || smoothK <= 0 || periodD <= 0 {
		return nil, fmt.Errorf("all periods must be positive integers")
	}

	// Validate candle data
	for i, c := range candles {
		if err := c.Validate(); err != nil {
			return nil, fmt.Errorf("invalid candle at index %d: %w", i, err)
		}
	}

	dataLength := len(candles)
	result := &StochasticResult{
		K: make([]float64, dataLength),
		D: make([]float64, dataLength),
	}

	// Step 1: Calculate raw stochastic oscillator
	rawStoch := make([]float64, dataLength)

	// Fill initial values with NaN
	for i := 0; i < periodK-1; i++ {
		rawStoch[i] = math.NaN()
		result.K[i] = math.NaN()
		result.D[i] = math.NaN()
	}

	// Calculate raw stochastic values: %K = 100 * (close - lowest_low) / (highest_high - lowest_low)
	for i := periodK - 1; i < dataLength; i++ {
		// Find highest high and lowest low over the periodK period
		startIdx := i - (periodK - 1)
		lowest := candles[startIdx].Low
		highest := candles[startIdx].High

		for j := startIdx + 1; j <= i; j++ {
			if candles[j].Low < lowest {
				lowest = candles[j].Low
			}
			if candles[j].High > highest {
				highest = candles[j].High
			}
		}

		// Calculate raw stochastic
		if highest == lowest {
			rawStoch[i] = 50.0 // Default to middle value when there's no range
		} else {
			rawStoch[i] = 100.0 * (candles[i].Close - lowest) / (highest - lowest)
		}
	}

	// Step 2: Apply smoothK smoothing to raw stochastic (SMA of raw stochastic)
	minIdxForK := periodK - 1 + smoothK - 1
	for i := 0; i < minIdxForK; i++ {
		result.K[i] = math.NaN()
	}

	for i := minIdxForK; i < dataLength; i++ {
		sum := 0.0
		count := 0

		for j := i - (smoothK - 1); j <= i; j++ {
			if !math.IsNaN(rawStoch[j]) {
				sum += rawStoch[j]
				count++
			}
		}

		if count == smoothK {
			result.K[i] = sum / float64(smoothK)
		} else {
			result.K[i] = math.NaN()
		}
	}

	// Step 3: Calculate %D as SMA of smoothed %K
	minIdxForD := minIdxForK + periodD - 1
	for i := 0; i < minIdxForD; i++ {
		result.D[i] = math.NaN()
	}

	for i := minIdxForD; i < dataLength; i++ {
		sum := 0.0
		count := 0

		for j := i - (periodD - 1); j <= i; j++ {
			if !math.IsNaN(result.K[j]) {
				sum += result.K[j]
				count++
			}
		}

		if count == periodD {
			result.D[i] = sum / float64(periodD)
		} else {
			result.D[i] = math.NaN()
		}
	}

	return result, nil
}

// (A) Fix %D Calculation
// Replace:

// go
// // Sum the existing K values
// for i := startIdx; i < len(existingResult.K); i++ {
//     if !math.IsNaN(existingResult.K[i]) {
//         sum += existingResult.K[i]
//         count++
//     }
// }
// With:

// go
// // Only use the last (periodD-1) smoothed %K values
// startIdx := len(existingResult.K) - (periodD - 1)
// if startIdx < minIdxForK {  // Ensure we're only using smoothed %K values
//     startIdx = minIdxForK
// }
// for i := startIdx; i < len(existingResult.K); i++ {
//     if !math.IsNaN(existingResult.K[i]) {
//         sum += existingResult.K[i]
//         count++
//     }
// }
// (B) Optimize Raw %K Calculation
// Use a rolling min/max approach (example pseudocode):

// go
// type RollingExtremes struct {
//     lows  []float64 // Ring buffer of lows
//     highs []float64 // Ring buffer of highs
// }

// func (r *RollingExtremes) Add(candle Candle) {
//     // Update ring buffers and track min/max
//     // (Implementation depends on data structure)
// }

// func (r *RollingExtremes) GetMinMax() (float64, float64) {
//     // Return current min/max
// }
// (C) Match Pine Script’s ta.sma() Behavior
// Pine Script treats na as 0 in ta.sma(). If you want exact parity, modify the averaging logic:

// go
// // Instead of skipping NaN, treat it as 0
// sum := 0.0
// for _, val := range values {
//     if math.IsNaN(val) {
//         val = 0.0
//     }
//     sum += val
// }
// avg := sum / float64(smoothK)

// Potential Improvements
// Optimize High/Low Search:
// Use a sliding window or deque to track the highest high and lowest low over periodK, reducing the time complexity from O(periodK) to O(1) per candle.
// Cache SMA Sums:
// For %D, store the previous SMA sum and update it by subtracting the oldest %K and adding the new one, reducing the complexity from O(periodD) to O(1).
// Documentation:
// Add comments explaining the NaN return cases and the default value of 50.0 for highest == lowest.
// Clarify the expected state of existingResult (e.g., it should contain valid %K/%D values for prior candles).
// Consistency Check:
// Add a check to ensure existingCandles and newCandle are temporally consistent (e.g., timestamps are sequential), if applicable.

// UpdateStochastic efficiently calculates the latest stochastic oscillator values
// This is more efficient than recalculating the entire series when adding new data incrementally
//
// Parameters:
// - existingResult: Previously calculated stochastic result (read-only). Can be nil for first candle.
// - existingCandles: The candles that were used to calculate existingResult. Can be empty for first candle.
// - newCandle: The new candle to add (read-only)
// - periodK: The lookback period for %K calculation
// - smoothK: The smoothing period for %K line
// - periodD: The smoothing period for %D line
//
// Returns the latest K and D values (float64, float64, error)
func UpdateStochastic(existingResult *StochasticResult, existingCandles []candle.Candle, newCandle candle.Candle, periodK, smoothK, periodD int) (float64, float64, error) {
	// Validate inputs
	if periodK <= 0 || smoothK <= 0 || periodD <= 0 {
		return math.NaN(), math.NaN(), fmt.Errorf("all periods must be positive integers")
	}

	// Validate new candle
	if err := newCandle.Validate(); err != nil {
		return math.NaN(), math.NaN(), fmt.Errorf("invalid new candle: %w", err)
	}

	// Handle the case where this is the first candle (existingResult is nil)
	if existingResult == nil {
		// First candle will always be NaN
		return math.NaN(), math.NaN(), nil
	}

	// Validate existing data consistency
	if len(existingCandles) == 0 {
		return math.NaN(), math.NaN(), nil
	}

	if len(existingResult.K) != len(existingCandles) || len(existingResult.D) != len(existingCandles) {
		return math.NaN(), math.NaN(), fmt.Errorf("existingResult length must match existingCandles length")
	}

	// Calculate the new index without creating a new slice
	newIdx := len(existingCandles)

	// Step 1: Calculate smoothed %K for the new candle
	minIdxForK := periodK - 1 + smoothK - 1
	var kValue float64
	if newIdx < minIdxForK {
		kValue = math.NaN()
	} else {
		// Calculate raw stochastic values for the smoothK period
		// We'll calculate them on-the-fly without storing in a slice
		sum := 0.0
		count := 0

		for i := 0; i < smoothK; i++ {
			candleIdx := newIdx - (smoothK - 1) + i

			if candleIdx < periodK-1 {
				// Skip this iteration, don't add to sum
				continue
			}

			// Calculate raw stochastic for this candle
			startIdx := candleIdx - (periodK - 1)

			var lowest, highest float64
			var closePrice float64

			// Check if we need to include the new candle in the range
			if candleIdx == newIdx {
				// This is the new candle position
				lowest = existingCandles[startIdx].Low
				highest = existingCandles[startIdx].High
				closePrice = newCandle.Close

				// Iterate through existing candles in the range
				for j := startIdx + 1; j < newIdx; j++ {
					if existingCandles[j].Low < lowest {
						lowest = existingCandles[j].Low
					}
					if existingCandles[j].High > highest {
						highest = existingCandles[j].High
					}
				}

				// Include the new candle's high/low
				if newCandle.Low < lowest {
					lowest = newCandle.Low
				}
				if newCandle.High > highest {
					highest = newCandle.High
				}
			} else {
				// Use existing candles only
				lowest = existingCandles[startIdx].Low
				highest = existingCandles[startIdx].High
				closePrice = existingCandles[candleIdx].Close

				for j := startIdx + 1; j <= candleIdx; j++ {
					if existingCandles[j].Low < lowest {
						lowest = existingCandles[j].Low
					}
					if existingCandles[j].High > highest {
						highest = existingCandles[j].High
					}
				}
			}

			// Calculate raw stochastic value
			var rawStoch float64
			if highest == lowest {
				rawStoch = 50.0
			} else {
				rawStoch = 100.0 * (closePrice - lowest) / (highest - lowest)
			}

			sum += rawStoch
			count++
		}

		if count == smoothK {
			kValue = sum / float64(smoothK)
		} else {
			kValue = math.NaN()
		}
	}

	// Step 2: Calculate %D for the new candle
	minIdxForD := minIdxForK + periodD - 1
	var dValue float64
	if newIdx < minIdxForD {
		dValue = math.NaN()
	} else {
		// Calculate %D as SMA of the last periodD %K values
		// We need to include the new kValue in this calculation
		sum := 0.0
		count := 0

		// Get the last (periodD-1) K values from existingResult
		startIdx := len(existingResult.K) - (periodD - 1)
		if startIdx < 0 {
			startIdx = 0
		}

		// Sum the existing K values
		for i := startIdx; i < len(existingResult.K); i++ {
			if !math.IsNaN(existingResult.K[i]) {
				sum += existingResult.K[i]
				count++
			}
		}

		// Add the new kValue
		if !math.IsNaN(kValue) {
			sum += kValue
			count++
		}

		if count == periodD {
			dValue = sum / float64(periodD)
		} else {
			dValue = math.NaN()
		}
	}

	return kValue, dValue, nil
}

// CalculateLastStochastic efficiently calculates only the last Stochastic Oscillator values
// This is more efficient when we only need the current stochastic values
func CalculateLastStochastic(candles []candle.Candle, periodK, smoothK, periodD int) (float64, float64, error) {
	// Validate inputs
	if len(candles) == 0 {
		return math.NaN(), math.NaN(), fmt.Errorf("candles array cannot be empty")
	}

	minRequired := periodK + smoothK + periodD - 2
	if len(candles) < minRequired {
		return math.NaN(), math.NaN(), fmt.Errorf("insufficient data: need at least %d candles", minRequired)
	}

	if periodK <= 0 || smoothK <= 0 || periodD <= 0 {
		return math.NaN(), math.NaN(), fmt.Errorf("all periods must be positive integers")
	}

	// Special case: smoothK=1, periodD=1
	if smoothK == 1 && periodD == 1 {
		idx := len(candles) - 1
		startIdx := idx - (periodK - 1)

		lowest := candles[startIdx].Low
		highest := candles[startIdx].High
		for j := startIdx + 1; j <= idx; j++ {
			if candles[j].Low < lowest {
				lowest = candles[j].Low
			}
			if candles[j].High > highest {
				highest = candles[j].High
			}
		}

		var rawStoch float64
		if highest == lowest {
			rawStoch = 50.0
		} else {
			rawStoch = 100.0 * (candles[idx].Close - lowest) / (highest - lowest)
		}
		return rawStoch, rawStoch, nil
	}

	// Compute raw stochastic values for the required window
	rawStochWindow := smoothK + periodD - 1
	rawStochValues := make([]float64, rawStochWindow)

	for i := 0; i < rawStochWindow; i++ {
		idx := len(candles) - rawStochWindow + i
		startIdx := idx - (periodK - 1)

		lowest := candles[startIdx].Low
		highest := candles[startIdx].High
		for j := startIdx + 1; j <= idx; j++ {
			if candles[j].Low < lowest {
				lowest = candles[j].Low
			}
			if candles[j].High > highest {
				highest = candles[j].High
			}
		}

		if highest == lowest {
			rawStochValues[i] = 50.0
		} else {
			rawStochValues[i] = 100.0 * (candles[idx].Close - lowest) / (highest - lowest)
		}
	}

	// Compute smoothed %K values for %D calculation
	kValues := make([]float64, periodD)
	for i := 0; i < periodD; i++ {
		start := len(rawStochValues) - periodD + i - (smoothK - 1)
		end := start + smoothK
		sum := 0.0
		for j := start; j < end; j++ {
			sum += rawStochValues[j]
		}
		kValues[i] = sum / float64(smoothK)
	}

	// Last %K and %D values
	lastK := kValues[periodD-1]
	lastD := 0.0
	for _, k := range kValues {
		lastD += k
	}
	lastD /= float64(periodD)

	return lastK, lastD, nil
}

// CalculateLastStochastic efficiently calculates only the last Stochastic Oscillator values
// This is more efficient when we only need the current stochastic values
func CalculateLastStochasticV2(candles []candle.Candle, periodK, smoothK, periodD int) (float64, float64, error) {
	// Validate inputs
	if len(candles) == 0 {
		return 0, 0, fmt.Errorf("candles array cannot be empty")
	}

	minRequired := periodK + smoothK + periodD - 2
	if len(candles) < minRequired {
		return 0, 0, fmt.Errorf("insufficient data: need at least %d candles", minRequired)
	}

	if periodK <= 0 || smoothK <= 0 || periodD <= 0 {
		return 0, 0, fmt.Errorf("all periods must be positive integers")
	}

	// Validate required candle data (only the ones we'll use)
	startIdx := len(candles) - minRequired
	for i := startIdx; i < len(candles); i++ {
		if err := candles[i].Validate(); err != nil {
			return 0, 0, fmt.Errorf("invalid candle at index %d: %w", i, err)
		}
	}

	dataLength := len(candles)

	// Step 1: Calculate the required raw stochastic values
	rawStochValues := make([]float64, smoothK+periodD-1)

	for i := 0; i < len(rawStochValues); i++ {
		// Index in the original array
		idx := dataLength - len(rawStochValues) + i
		startIdx := idx - (periodK - 1)

		// Find highest high and lowest low in the period
		lowest := candles[startIdx].Low
		highest := candles[startIdx].High

		for j := startIdx + 1; j <= idx; j++ {
			if candles[j].Low < lowest {
				lowest = candles[j].Low
			}
			if candles[j].High > highest {
				highest = candles[j].High
			}
		}

		// Calculate raw stochastic
		if highest == lowest {
			rawStochValues[i] = 50.0 // Default to middle value when there's no range
		} else {
			rawStochValues[i] = 100.0 * (candles[idx].Close - lowest) / (highest - lowest)
		}
	}

	// Step 2: Calculate the required smoothed %K values
	kValues := make([]float64, periodD)

	for i := 0; i < periodD; i++ {
		startIdx := i + smoothK - 1
		sum := 0.0

		for j := startIdx - (smoothK - 1); j <= startIdx; j++ {
			sum += rawStochValues[j]
		}

		kValues[i] = sum / float64(smoothK)
	}

	// Step 3: Calculate the last %K and %D values
	lastK := kValues[periodD-1]

	// Calculate the last %D value (SMA of the last periodD %K values)
	sum := 0.0
	for _, k := range kValues {
		sum += k
	}
	lastD := sum / float64(periodD)

	return lastK, lastD, nil
}

// DefaultStochasticSettings returns the default parameters matching Pine Script defaults
func DefaultStochasticSettings() (periodK, smoothK, periodD int) {
	return 14, 1, 3 // %K Length=14, %K Smoothing=1, %D Smoothing=3
}

// StochasticSignals defines common stochastic oscillator signal levels
const (
	StochasticOverbought = 80.0 // Upper band - overbought condition
	StochasticOversold   = 20.0 // Lower band - oversold condition
	StochasticMiddle     = 50.0 // Middle band - neutral zone
)

// IsOverbought checks if the stochastic oscillator indicates overbought conditions
func IsOverbought(k, d float64) bool {
	return k > StochasticOverbought && d > StochasticOverbought
}

// IsOversold checks if the stochastic oscillator indicates oversold conditions
func IsOversold(k, d float64) bool {
	return k < StochasticOversold && d < StochasticOversold
}

// IsBullishCrossover detects when %K crosses above %D (bullish signal)
func IsBullishCrossover(prevK, prevD, currK, currD float64) bool {
	return prevK <= prevD && currK > currD
}

// IsBearishCrossover detects when %K crosses below %D (bearish signal)
func IsBearishCrossover(prevK, prevD, currK, currD float64) bool {
	return prevK >= prevD && currK < currD
}

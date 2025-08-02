package indicator

import (
	"fmt"
	"log"
	"math"
	"testing"
	"time"

	"github.com/amirphl/simple-trader/internal/candle"
	"github.com/stretchr/testify/assert"
)

// Helper function to create test candles from price arrays
func createTestCandles(highs, lows, closes []float64) []candle.Candle {
	if len(highs) != len(lows) || len(lows) != len(closes) {
		panic("input arrays must have the same length")
	}

	candles := make([]candle.Candle, len(closes))
	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	for i := 0; i < len(closes); i++ {
		// Calculate open price (use previous close or close if first)
		open := closes[i]
		if i > 0 {
			open = closes[i-1]
		}

		candles[i] = candle.Candle{
			Timestamp: baseTime.Add(time.Duration(i) * time.Hour),
			Open:      open,
			High:      highs[i],
			Low:       lows[i],
			Close:     closes[i],
			Volume:    1000.0, // Default volume
			Symbol:    "TEST",
			Timeframe: "1h",
			Source:    "test",
		}
	}

	return candles
}

// Helper function to print actual Stochastic values for test cases
func calculateAndPrintStochastic(t *testing.T) {
	// Sample data
	highs := []float64{12, 13, 14, 15, 16, 17, 18, 19, 20, 21}
	lows := []float64{8, 9, 10, 11, 12, 13, 14, 15, 16, 17}
	closes := []float64{10, 12, 11, 14, 13, 16, 15, 18, 17, 20}
	candles := createTestCandles(highs, lows, closes)

	periodK := 3
	smoothK := 1
	periodD := 2

	result, err := CalculateStochastic(candles, periodK, smoothK, periodD)
	assert.NoError(t, err)

	log.Println("Sample Stochastic values:")
	for i := 0; i < len(result.K); i++ {
		if !math.IsNaN(result.K[i]) && !math.IsNaN(result.D[i]) {
			log.Printf("Index %d: K=%.2f, D=%.2f\n", i, result.K[i], result.D[i])
		} else if !math.IsNaN(result.K[i]) {
			log.Printf("Index %d: K=%.2f, D=NaN\n", i, result.K[i])
		} else {
			log.Printf("Index %d: K=NaN, D=NaN\n", i)
		}
	}
}

func TestCalculateStochastic(t *testing.T) {
	// Uncomment to debug
	// calculateAndPrintStochastic(t)

	tests := []struct {
		name      string
		highs     []float64
		lows      []float64
		closes    []float64
		periodK   int
		smoothK   int
		periodD   int
		expectedK []float64
		expectedD []float64
		expectErr bool
	}{
		{
			name:      "Basic Stochastic calculation (Pine Script defaults)",
			highs:     []float64{12, 13, 14, 15, 16, 17, 18, 19, 20, 21},
			lows:      []float64{8, 9, 10, 11, 12, 13, 14, 15, 16, 17},
			closes:    []float64{10, 12, 11, 14, 13, 16, 15, 18, 17, 20},
			periodK:   3,
			smoothK:   1,
			periodD:   2,
			expectedK: []float64{math.NaN(), math.NaN(), 50.00, 83.33, 66.67, 83.33, 66.67, 83.33, 66.67, 83.33},
			expectedD: []float64{math.NaN(), math.NaN(), math.NaN(), 66.67, 75.00, 75.00, 75.00, 75.00, 75.00, 75.00},
			expectErr: false,
		},
		{
			name:      "Pine Script full defaults (14, 1, 3)",
			highs:     []float64{15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32},
			lows:      []float64{5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22},
			closes:    []float64{10, 12, 14, 16, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31},
			periodK:   14,
			smoothK:   1,
			periodD:   3,
			expectedK: []float64{math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.NaN(), 95.65, 95.65, 95.65, 95.65, 95.65},
			expectedD: []float64{math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.NaN(), 95.65, 95.65, 95.65},
			expectErr: false,
		},
		{
			name:      "With K smoothing (smoothK > 1)",
			highs:     []float64{12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23},
			lows:      []float64{8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19},
			closes:    []float64{10, 12, 11, 14, 13, 16, 15, 18, 17, 20, 19, 22},
			periodK:   3,
			smoothK:   3,
			periodD:   2,
			expectedK: []float64{math.NaN(), math.NaN(), math.NaN(), math.NaN(), 61.11, 72.22, 61.11, 72.22, 61.11, 72.22, 61.11, 72.22},
			expectedD: []float64{math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.NaN(), 66.67, 66.67, 66.67, 66.67, 66.67, 66.67, 66.67},
			expectErr: false,
		},
		{
			name:      "All increasing closes",
			highs:     []float64{15, 16, 17, 18, 19, 20, 21, 22},
			lows:      []float64{5, 6, 7, 8, 9, 10, 11, 12},
			closes:    []float64{10, 12, 14, 16, 18, 19, 20, 21},
			periodK:   3,
			smoothK:   1,
			periodD:   2,
			expectedK: []float64{math.NaN(), math.NaN(), 75.00, 83.33, 91.67, 90.00, 90.00, 90.00},
			expectedD: []float64{math.NaN(), math.NaN(), math.NaN(), 79.17, 87.50, 90.83, 90.00, 90.00},
			expectErr: false,
		},
		{
			name:      "All decreasing closes",
			highs:     []float64{20, 19, 18, 17, 16, 15, 14, 13},
			lows:      []float64{10, 9, 8, 7, 6, 5, 4, 3},
			closes:    []float64{18, 16, 14, 12, 10, 8, 6, 4},
			periodK:   3,
			smoothK:   1,
			periodD:   2,
			expectedK: []float64{math.NaN(), math.NaN(), 50.00, 41.67, 33.33, 25.00, 16.67, 8.33},
			expectedD: []float64{math.NaN(), math.NaN(), math.NaN(), 45.83, 37.50, 29.17, 20.83, 12.50},
			expectErr: false,
		},
		{
			name:      "Flat prices",
			highs:     []float64{15, 15, 15, 15, 15, 15, 15},
			lows:      []float64{10, 10, 10, 10, 10, 10, 10},
			closes:    []float64{12, 12, 12, 12, 12, 12, 12},
			periodK:   3,
			smoothK:   1,
			periodD:   2,
			expectedK: []float64{math.NaN(), math.NaN(), 40.00, 40.00, 40.00, 40.00, 40.00},
			expectedD: []float64{math.NaN(), math.NaN(), math.NaN(), 40.00, 40.00, 40.00, 40.00},
			expectErr: false,
		},
		{
			name:      "No range (high equals low)",
			highs:     []float64{10, 10, 10, 10, 10, 10, 10},
			lows:      []float64{10, 10, 10, 10, 10, 10, 10},
			closes:    []float64{10, 10, 10, 10, 10, 10, 10},
			periodK:   3,
			smoothK:   1,
			periodD:   2,
			expectedK: []float64{math.NaN(), math.NaN(), 50.00, 50.00, 50.00, 50.00, 50.00},
			expectedD: []float64{math.NaN(), math.NaN(), math.NaN(), 50.00, 50.00, 50.00, 50.00},
			expectErr: false,
		},
		{
			name:      "Empty candles array",
			highs:     []float64{},
			lows:      []float64{},
			closes:    []float64{},
			periodK:   3,
			smoothK:   1,
			periodD:   2,
			expectedK: nil,
			expectedD: nil,
			expectErr: true,
		},
		{
			name:      "Insufficient data",
			highs:     []float64{10, 11},
			lows:      []float64{5, 6},
			closes:    []float64{8, 9},
			periodK:   3,
			smoothK:   1,
			periodD:   2,
			expectedK: nil,
			expectedD: nil,
			expectErr: true,
		},
		{
			name:      "Invalid period",
			highs:     []float64{10, 11, 12, 13, 14},
			lows:      []float64{5, 6, 7, 8, 9},
			closes:    []float64{8, 9, 10, 11, 12},
			periodK:   0,
			smoothK:   1,
			periodD:   2,
			expectedK: nil,
			expectedD: nil,
			expectErr: true,
		},
		{
			name:      "Extreme values",
			highs:     []float64{100, 200, 300, 400, 500, 600},
			lows:      []float64{1, 2, 3, 4, 5, 6},
			closes:    []float64{50, 150, 30, 350, 50, 500},
			periodK:   3,
			smoothK:   1,
			periodD:   2,
			expectedK: []float64{math.NaN(), math.NaN(), 9.70, 87.44, 9.46, 83.22},
			expectedD: []float64{math.NaN(), math.NaN(), math.NaN(), 48.57, 48.45, 46.34},
			expectErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var candles []candle.Candle
			if len(tt.highs) > 0 {
				candles = createTestCandles(tt.highs, tt.lows, tt.closes)
			}

			result, err := CalculateStochastic(candles, tt.periodK, tt.smoothK, tt.periodD)

			if tt.expectErr {
				assert.Error(t, err)
				assert.Nil(t, result)
				return
			}

			assert.NoError(t, err)
			assert.NotNil(t, result)
			assert.Equal(t, len(tt.expectedK), len(result.K), "K array length mismatch")
			assert.Equal(t, len(tt.expectedD), len(result.D), "D array length mismatch")

			// Check K values
			for i := 0; i < len(tt.expectedK); i++ {
				if math.IsNaN(tt.expectedK[i]) {
					assert.True(t, math.IsNaN(result.K[i]), "Expected K to be NaN at index %d", i)
				} else {
					// Round to 2 decimal places for comparison
					expected := math.Round(tt.expectedK[i]*100) / 100
					actual := math.Round(result.K[i]*100) / 100
					assert.InDelta(t, expected, actual, 0.01, "K mismatch at index %d", i)
				}
			}

			// Check D values
			for i := 0; i < len(tt.expectedD); i++ {
				if math.IsNaN(tt.expectedD[i]) {
					assert.True(t, math.IsNaN(result.D[i]), "Expected D to be NaN at index %d", i)
				} else {
					// Round to 2 decimal places for comparison
					expected := math.Round(tt.expectedD[i]*100) / 100
					actual := math.Round(result.D[i]*100) / 100
					assert.InDelta(t, expected, actual, 0.01, "D mismatch at index %d", i)
				}
			}
		})
	}
}

func TestUpdateStochastic(t *testing.T) {
	candles := createTestCandles([]float64{100, 110, 105, 115, 120, 118, 125, 130, 128, 135, 140, 138, 145, 150, 148, 155, 160, 158, 165, 170}, // close
		[]float64{95, 105, 100, 110, 115, 113, 120, 125, 123, 130, 135, 133, 140, 145, 143, 150, 155, 153, 160, 165}, // high
		[]float64{90, 100, 95, 105, 110, 108, 115, 120, 118, 125, 130, 128, 135, 140, 138, 145, 150, 148, 155, 160})  // low

	periodK, smoothK, periodD := 14, 1, 3

	// Calculate full stochastic for comparison
	fullResult, err := CalculateStochastic(candles, periodK, smoothK, periodD)
	assert.NoError(t, err)
	assert.NotNil(t, fullResult)

	// Test incremental updates
	var result *StochasticResult
	var existingCandles []candle.Candle

	for i, candle := range candles {
		k, d, err := UpdateStochastic(result, existingCandles, candle, periodK, smoothK, periodD)
		assert.NoError(t, err)

		if i == 0 {
			// First candle should be NaN
			assert.True(t, math.IsNaN(k))
			assert.True(t, math.IsNaN(d))
		} else {
			// Compare with full calculation
			if i < len(fullResult.K) {
				if !math.IsNaN(fullResult.K[i]) {
					assert.InDelta(t, fullResult.K[i], k, 0.001, "K value mismatch at index %d", i)
				}
				if !math.IsNaN(fullResult.D[i]) {
					assert.InDelta(t, fullResult.D[i], d, 0.001, "D value mismatch at index %d", i)
				}
			}
		}

		// Update for next iteration
		if result == nil {
			result = &StochasticResult{
				K: []float64{k},
				D: []float64{d},
			}
		} else {
			result.K = append(result.K, k)
			result.D = append(result.D, d)
		}
		existingCandles = append(existingCandles, candle)
	}
}

func TestUpdateStochasticValidation(t *testing.T) {
	candles := createTestCandles([]float64{100, 110, 105}, []float64{105, 115, 110}, []float64{95, 105, 100})
	validCandle := candles[0]
	invalidCandle := candle.Candle{} // Invalid candle

	testCases := []struct {
		name            string
		existingResult  *StochasticResult
		existingCandles []candle.Candle
		newCandle       candle.Candle
		periodK         int
		smoothK         int
		periodD         int
		expectErr       bool
		errorContains   string
	}{
		{
			name:            "Invalid periods",
			existingResult:  nil,
			existingCandles: []candle.Candle{},
			newCandle:       validCandle,
			periodK:         0,
			smoothK:         1,
			periodD:         3,
			expectErr:       true,
			errorContains:   "all periods must be positive integers",
		},
		{
			name:            "Invalid new candle",
			existingResult:  nil,
			existingCandles: []candle.Candle{},
			newCandle:       invalidCandle,
			periodK:         14,
			smoothK:         1,
			periodD:         3,
			expectErr:       true,
			errorContains:   "invalid new candle",
		},
		{
			name:            "Nil existing result",
			existingResult:  nil,
			existingCandles: []candle.Candle{},
			newCandle:       validCandle,
			periodK:         14,
			smoothK:         1,
			periodD:         3,
			expectErr:       false,
		},
		{
			name:            "Empty existing candles with non-nil result",
			existingResult:  &StochasticResult{K: []float64{50}, D: []float64{45}},
			existingCandles: []candle.Candle{},
			newCandle:       validCandle,
			periodK:         14,
			smoothK:         1,
			periodD:         3,
			expectErr:       true,
			errorContains:   "existingCandles cannot be empty when existingResult is not nil",
		},
		{
			name:            "Length mismatch",
			existingResult:  &StochasticResult{K: []float64{50}, D: []float64{45}},
			existingCandles: []candle.Candle{validCandle, validCandle}, // 2 candles
			newCandle:       validCandle,
			periodK:         14,
			smoothK:         1,
			periodD:         3,
			expectErr:       true,
			errorContains:   "existingResult length must match existingCandles length",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			k, d, err := UpdateStochastic(tc.existingResult, tc.existingCandles, tc.newCandle, tc.periodK, tc.smoothK, tc.periodD)

			if tc.expectErr {
				assert.Error(t, err)
				if tc.errorContains != "" {
					assert.Contains(t, err.Error(), tc.errorContains)
				}
				assert.True(t, math.IsNaN(k))
				assert.True(t, math.IsNaN(d))
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestUpdateStochasticFirstCandle(t *testing.T) {
	newCandle := candle.Candle{
		Timestamp: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		Open:      100, High: 110, Low: 95, Close: 105, Volume: 1000, Symbol: "TEST", Timeframe: "1h", Source: "test",
	}
	periodK, smoothK, periodD := 14, 1, 3

	k, d, err := UpdateStochastic(nil, []candle.Candle{}, newCandle, periodK, smoothK, periodD)
	assert.NoError(t, err)
	assert.True(t, math.IsNaN(k), "First K value should be NaN")
	assert.True(t, math.IsNaN(d), "First D value should be NaN")

	// Test second candle
	secondCandle := candle.Candle{
		Timestamp: time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC),
		Open:      105, High: 115, Low: 100, Close: 110, Volume: 1000, Symbol: "TEST", Timeframe: "1h", Source: "test",
	}

	result := &StochasticResult{
		K: []float64{k},
		D: []float64{d},
	}
	existingCandles := []candle.Candle{newCandle}

	k2, d2, err := UpdateStochastic(result, existingCandles, secondCandle, periodK, smoothK, periodD)
	assert.NoError(t, err)
	assert.True(t, math.IsNaN(k2), "Second K value should be NaN (not enough data)")
	assert.True(t, math.IsNaN(d2), "Second D value should be NaN (not enough data)")
}

func TestUpdateStochasticMultipleUpdates(t *testing.T) {
	candles := createTestCandles([]float64{100, 110, 105, 115, 120}, []float64{105, 115, 110, 120, 125}, []float64{95, 105, 100, 110, 115})
	periodK, smoothK, periodD := 3, 1, 2

	// Calculate full result for comparison
	fullResult, err := CalculateStochastic(candles, periodK, smoothK, periodD)
	assert.NoError(t, err)

	// Test incremental updates
	var result *StochasticResult
	var existingCandles []candle.Candle

	for i, candle := range candles {
		k, d, err := UpdateStochastic(result, existingCandles, candle, periodK, smoothK, periodD)
		assert.NoError(t, err)

		// Compare with full calculation
		if i < len(fullResult.K) {
			if !math.IsNaN(fullResult.K[i]) {
				assert.InDelta(t, fullResult.K[i], k, 0.001, "K value mismatch at index %d", i)
			}
			if !math.IsNaN(fullResult.D[i]) {
				assert.InDelta(t, fullResult.D[i], d, 0.001, "D value mismatch at index %d", i)
			}
		}

		// Update for next iteration
		if result == nil {
			result = &StochasticResult{
				K: []float64{k},
				D: []float64{d},
			}
		} else {
			result.K = append(result.K, k)
			result.D = append(result.D, d)
		}
		existingCandles = append(existingCandles, candle)
	}
}

func TestCalculateLastStochastic(t *testing.T) {
	tests := []struct {
		name        string
		highs       []float64
		lows        []float64
		closes      []float64
		periodK     int
		smoothK     int
		periodD     int
		expectedK   float64
		expectedD   float64
		expectError bool
	}{
		{
			name:        "Basic last Stochastic calculation",
			highs:       []float64{12, 13, 14, 15, 16, 17, 18, 19, 20, 21},
			lows:        []float64{8, 9, 10, 11, 12, 13, 14, 15, 16, 17},
			closes:      []float64{10, 12, 11, 14, 13, 16, 15, 18, 17, 20},
			periodK:     3,
			smoothK:     1,
			periodD:     2,
			expectedK:   83.33,
			expectedD:   75.00,
			expectError: false,
		},
		{
			name:        "Pine Script defaults",
			highs:       []float64{15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32},
			lows:        []float64{5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22},
			closes:      []float64{10, 12, 14, 16, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31},
			periodK:     14,
			smoothK:     1,
			periodD:     3,
			expectedK:   95.65,
			expectedD:   95.65,
			expectError: false,
		},
		{
			name:        "With K smoothing",
			highs:       []float64{12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23},
			lows:        []float64{8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19},
			closes:      []float64{10, 12, 11, 14, 13, 16, 15, 18, 17, 20, 19, 22},
			periodK:     3,
			smoothK:     3,
			periodD:     2,
			expectedK:   72.22,
			expectedD:   66.67,
			expectError: false,
		},
		{
			name:        "All increasing closes",
			highs:       []float64{15, 16, 17, 18, 19, 20, 21, 22},
			lows:        []float64{5, 6, 7, 8, 9, 10, 11, 12},
			closes:      []float64{10, 12, 14, 16, 18, 19, 20, 21},
			periodK:     3,
			smoothK:     1,
			periodD:     2,
			expectedK:   90.00,
			expectedD:   90.00,
			expectError: false,
		},
		{
			name:        "All decreasing closes",
			highs:       []float64{20, 19, 18, 17, 16, 15, 14, 13},
			lows:        []float64{10, 9, 8, 7, 6, 5, 4, 3},
			closes:      []float64{18, 16, 14, 12, 10, 8, 6, 4},
			periodK:     3,
			smoothK:     1,
			periodD:     2,
			expectedK:   8.33,
			expectedD:   12.50,
			expectError: false,
		},
		{
			name:        "Flat prices",
			highs:       []float64{15, 15, 15, 15, 15, 15, 15},
			lows:        []float64{10, 10, 10, 10, 10, 10, 10},
			closes:      []float64{12, 12, 12, 12, 12, 12, 12},
			periodK:     3,
			smoothK:     1,
			periodD:     2,
			expectedK:   40.00,
			expectedD:   40.00,
			expectError: false,
		},
		{
			name:        "No range (high equals low)",
			highs:       []float64{10, 10, 10, 10, 10, 10, 10},
			lows:        []float64{10, 10, 10, 10, 10, 10, 10},
			closes:      []float64{10, 10, 10, 10, 10, 10, 10},
			periodK:     3,
			smoothK:     1,
			periodD:     2,
			expectedK:   50.00,
			expectedD:   50.00,
			expectError: false,
		},
		{
			name:        "Empty candles array",
			highs:       []float64{},
			lows:        []float64{},
			closes:      []float64{},
			periodK:     3,
			smoothK:     1,
			periodD:     2,
			expectedK:   0,
			expectedD:   0,
			expectError: true,
		},
		{
			name:        "Insufficient data",
			highs:       []float64{10, 11},
			lows:        []float64{5, 6},
			closes:      []float64{8, 9},
			periodK:     3,
			smoothK:     1,
			periodD:     2,
			expectedK:   0,
			expectedD:   0,
			expectError: true,
		},
		{
			name:        "Invalid period",
			highs:       []float64{10, 11, 12, 13, 14},
			lows:        []float64{5, 6, 7, 8, 9},
			closes:      []float64{8, 9, 10, 11, 12},
			periodK:     0,
			smoothK:     1,
			periodD:     2,
			expectedK:   0,
			expectedD:   0,
			expectError: true,
		},
		{
			name:        "Minimum required data",
			highs:       []float64{10, 11, 12, 13},
			lows:        []float64{5, 6, 7, 8},
			closes:      []float64{8, 9, 10, 11},
			periodK:     3,
			smoothK:     1,
			periodD:     2,
			expectedK:   50.00,
			expectedD:   41.67,
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var candles []candle.Candle
			if len(tt.highs) > 0 {
				candles = createTestCandles(tt.highs, tt.lows, tt.closes)
			}

			k, d, err := CalculateLastStochastic(candles, tt.periodK, tt.smoothK, tt.periodD)

			if tt.expectError {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			// Round to 2 decimal places for comparison
			expectedK := math.Round(tt.expectedK*100) / 100
			actualK := math.Round(k*100) / 100
			assert.InDelta(t, expectedK, actualK, 0.01, "K value mismatch")

			expectedD := math.Round(tt.expectedD*100) / 100
			actualD := math.Round(d*100) / 100
			assert.InDelta(t, expectedD, actualD, 0.01, "D value mismatch")
		})
	}
}

func TestStochasticConsistency(t *testing.T) {
	// Test that CalculateLastStochastic returns the same values as the last elements of CalculateStochastic
	highs := []float64{12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23}
	lows := []float64{8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19}
	closes := []float64{10, 12, 11, 14, 13, 16, 15, 18, 17, 20, 19, 22}
	candles := createTestCandles(highs, lows, closes)

	periodKs := []int{3, 5, 9}
	smoothKs := []int{1, 2, 3}
	periodDs := []int{2, 3, 4}

	for _, periodK := range periodKs {
		for _, smoothK := range smoothKs {
			for _, periodD := range periodDs {
				testName := fmt.Sprintf("K%d_S%d_D%d", periodK, smoothK, periodD)
				t.Run(testName, func(t *testing.T) {
					result, err := CalculateStochastic(candles, periodK, smoothK, periodD)
					assert.NoError(t, err)

					lastK, lastD, err := CalculateLastStochastic(candles, periodK, smoothK, periodD)
					assert.NoError(t, err)

					assert.InDelta(t, result.K[len(result.K)-1], lastK, 0.0001, "Last K value mismatch")
					assert.InDelta(t, result.D[len(result.D)-1], lastD, 0.0001, "Last D value mismatch")
				})
			}
		}
	}
}

func TestDefaultStochasticSettings(t *testing.T) {
	periodK, smoothK, periodD := DefaultStochasticSettings()
	assert.Equal(t, 14, periodK, "Default periodK should be 14")
	assert.Equal(t, 1, smoothK, "Default smoothK should be 1")
	assert.Equal(t, 3, periodD, "Default periodD should be 3")
}

func TestStochasticSignalHelpers(t *testing.T) {
	// Test overbought/oversold conditions
	assert.True(t, IsOverbought(85, 85))
	assert.False(t, IsOverbought(75, 85))
	assert.False(t, IsOverbought(85, 75))
	assert.False(t, IsOverbought(75, 75))

	assert.True(t, IsOversold(15, 15))
	assert.False(t, IsOversold(25, 15))
	assert.False(t, IsOversold(15, 25))
	assert.False(t, IsOversold(25, 25))

	// Test crossover detection
	assert.True(t, IsBullishCrossover(45, 50, 55, 50))  // K crosses above D
	assert.False(t, IsBullishCrossover(55, 50, 45, 50)) // K crosses below D
	assert.False(t, IsBullishCrossover(55, 50, 65, 60)) // K already above D

	assert.True(t, IsBearishCrossover(55, 50, 45, 50))  // K crosses below D
	assert.False(t, IsBearishCrossover(45, 50, 55, 50)) // K crosses above D
	assert.False(t, IsBearishCrossover(45, 50, 35, 40)) // K already below D
}

func TestInvalidCandleData(t *testing.T) {
	// Test with invalid candle data
	invalidCandles := []candle.Candle{
		{
			Timestamp: time.Now(),
			Open:      10,
			High:      5, // Invalid: high < low
			Low:       15,
			Close:     12,
			Volume:    100,
			Symbol:    "TEST",
			Timeframe: "1h",
			Source:    "test",
		},
	}

	_, err := CalculateStochastic(invalidCandles, 3, 1, 2)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid candle")
}

func BenchmarkCalculateStochastic(b *testing.B) {
	// Generate sample data
	size := 1000
	highs := make([]float64, size)
	lows := make([]float64, size)
	closes := make([]float64, size)

	for i := 0; i < size; i++ {
		highs[i] = 100 + float64(i%20)
		lows[i] = 80 + float64(i%15)
		closes[i] = 90 + float64(i%18)
	}

	candles := createTestCandles(highs, lows, closes)
	periodK, smoothK, periodD := DefaultStochasticSettings()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		CalculateStochastic(candles, periodK, smoothK, periodD)
	}
}

func BenchmarkCalculateLastStochastic(b *testing.B) {
	// Generate sample data
	size := 1000
	highs := make([]float64, size)
	lows := make([]float64, size)
	closes := make([]float64, size)

	for i := 0; i < size; i++ {
		highs[i] = 100 + float64(i%20)
		lows[i] = 80 + float64(i%15)
		closes[i] = 90 + float64(i%18)
	}

	candles := createTestCandles(highs, lows, closes)
	periodK, smoothK, periodD := DefaultStochasticSettings()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		CalculateLastStochastic(candles, periodK, smoothK, periodD)
	}
}

func BenchmarkUpdateStochastic(b *testing.B) {
	candles := createTestCandles([]float64{100, 110, 105, 115, 120, 118, 125, 130, 128, 135, 140, 138, 145, 150, 148, 155, 160, 158, 165, 170}, // close
		[]float64{95, 105, 100, 110, 115, 113, 120, 125, 123, 130, 135, 133, 140, 145, 143, 150, 155, 153, 160, 165}, // high
		[]float64{90, 100, 95, 105, 110, 108, 115, 120, 118, 125, 130, 128, 135, 140, 138, 145, 150, 148, 155, 160})  // low

	periodK, smoothK, periodD := 14, 1, 3

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var result *StochasticResult
		var existingCandles []candle.Candle

		for _, candle := range candles {
			k, d, err := UpdateStochastic(result, existingCandles, candle, periodK, smoothK, periodD)
			if err != nil {
				b.Fatal(err)
			}

			// Update for next iteration
			if result == nil {
				result = &StochasticResult{
					K: []float64{k},
					D: []float64{d},
				}
			} else {
				result.K = append(result.K, k)
				result.D = append(result.D, d)
			}
			existingCandles = append(existingCandles, candle)
		}
	}
}

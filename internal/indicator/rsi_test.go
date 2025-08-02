package indicator

import (
	"log"
	"math"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

// Helper function to print actual RSI values for test cases
func calculateAndPrintRSI(t *testing.T) {
	// Extreme price changes test case
	prices := []float64{10, 100, 5, 200, 1, 300, 2, 400}
	period := 3

	result := CalculateRSI(prices, period)

	log.Println("Extreme price changes RSI values:")
	for i, val := range result {
		if !math.IsNaN(val) {
			log.Printf("Index %d: %.2f\n", i, val)
		} else {
			log.Printf("Index %d: NaN\n", i)
		}
	}
}

func TestCalculateRSI(t *testing.T) {
	// Uncomment to debug
	// calculateAndPrintRSI(t)

	tests := []struct {
		name     string
		prices   []float64
		period   int
		expected []float64
		isNil    bool
	}{
		{
			name:   "Basic RSI calculation",
			prices: []float64{10, 11, 12, 11, 10, 9, 10, 11, 12, 13, 14, 13, 12, 11, 12},
			period: 5,
			expected: []float64{
				math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.NaN(),
				40.00, 52.00, 61.60, 69.28, 75.42, 80.34, 64.27, 51.42, 41.13, 52.91,
			},
		},
		{
			name:   "All increasing prices",
			prices: []float64{10, 11, 12, 13, 14, 15, 16, 17, 18, 19},
			period: 3,
			expected: []float64{
				math.NaN(), math.NaN(), math.NaN(),
				100, 100, 100, 100, 100, 100, 100,
			},
		},
		{
			name:   "All decreasing prices",
			prices: []float64{20, 19, 18, 17, 16, 15, 14, 13, 12, 11},
			period: 3,
			expected: []float64{
				math.NaN(), math.NaN(), math.NaN(),
				0, 0, 0, 0, 0, 0, 0,
			},
		},
		{
			name:   "Flat prices",
			prices: []float64{10, 10, 10, 10, 10, 10, 10, 10},
			period: 3,
			expected: []float64{
				math.NaN(), math.NaN(), math.NaN(),
				100, 100, 100, 100, 100,
			},
		},
		{
			name:   "Alternating prices",
			prices: []float64{10, 11, 10, 11, 10, 11, 10, 11, 10},
			period: 2,
			expected: []float64{
				math.NaN(), math.NaN(),
				50.00, 75.00, 37.50, 68.75, 34.38, 67.19, 33.59,
			},
		},
		{
			name:     "Insufficient data",
			prices:   []float64{10, 11, 12},
			period:   5,
			expected: nil,
			isNil:    true,
		},
		{
			name:     "Invalid period",
			prices:   []float64{10, 11, 12, 13, 14},
			period:   0,
			expected: nil,
			isNil:    true,
		},
		{
			name:     "Empty prices",
			prices:   []float64{},
			period:   5,
			expected: nil,
			isNil:    true,
		},
		{
			name:   "Extreme price changes",
			prices: []float64{10, 100, 5, 200, 1, 300, 2, 400},
			period: 3,
			expected: []float64{
				math.NaN(), math.NaN(), math.NaN(),
				75.00, 42.00, 70.88, 40.63, 67.99,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CalculateRSI(tt.prices, tt.period)

			if tt.isNil {
				assert.Nil(t, result)
				return
			}

			assert.Equal(t, len(tt.expected), len(result), "RSI array length mismatch")

			for i := 0; i < len(tt.expected); i++ {
				if math.IsNaN(tt.expected[i]) {
					assert.True(t, math.IsNaN(result[i]), "Expected NaN at index %d", i)
				} else {
					// Round to 2 decimal places for comparison
					expected := math.Round(tt.expected[i]*100) / 100
					actual := math.Round(result[i]*100) / 100
					assert.InDelta(t, expected, actual, 0.01, "RSI mismatch at index %d", i)
				}
			}
		})
	}
}

func TestCalculateLastRSI(t *testing.T) {
	tests := []struct {
		name        string
		prices      []float64
		period      int
		expected    float64
		expectError bool
	}{
		{
			name:        "Basic last RSI calculation",
			prices:      []float64{10, 11, 12, 11, 10, 9, 10, 11, 12, 13, 14, 13, 12, 11, 12},
			period:      5,
			expected:    52.91,
			expectError: false,
		},
		{
			name:        "All increasing prices",
			prices:      []float64{10, 11, 12, 13, 14, 15, 16, 17, 18, 19},
			period:      3,
			expected:    100,
			expectError: false,
		},
		{
			name:        "All decreasing prices",
			prices:      []float64{20, 19, 18, 17, 16, 15, 14, 13, 12, 11},
			period:      3,
			expected:    0,
			expectError: false,
		},
		{
			name:        "Flat prices",
			prices:      []float64{10, 10, 10, 10, 10, 10, 10, 10},
			period:      3,
			expected:    100,
			expectError: false,
		},
		{
			name:        "Insufficient data",
			prices:      []float64{10, 11, 12},
			period:      5,
			expected:    0,
			expectError: true,
		},
		{
			name:        "Invalid period",
			prices:      []float64{10, 11, 12, 13, 14},
			period:      0,
			expected:    0,
			expectError: true,
		},
		{
			name:        "Empty prices",
			prices:      []float64{},
			period:      5,
			expected:    0,
			expectError: true,
		},
		{
			name:        "Exact minimum data length",
			prices:      []float64{10, 11, 12, 13, 14, 15},
			period:      5,
			expected:    100,
			expectError: false,
		},
		{
			name:        "Zero loss case",
			prices:      []float64{10, 11, 12, 13, 14, 15},
			period:      3,
			expected:    100,
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := CalculateLastRSI(tt.prices, tt.period)

			if tt.expectError {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			// Round to 2 decimal places for comparison
			expected := math.Round(tt.expected*100) / 100
			actual := math.Round(result*100) / 100
			assert.InDelta(t, expected, actual, 0.01)
		})
	}
}

func TestRSIConsistency(t *testing.T) {
	// Test that CalculateLastRSI returns the same value as the last element of CalculateRSI
	prices := []float64{10, 11, 12, 11, 10, 9, 10, 11, 12, 13, 14, 13, 12, 11, 12}
	periods := []int{5, 9, 14}

	for _, period := range periods {
		formatted := strconv.FormatInt(int64(period), 10)
		t.Run("Period "+formatted, func(t *testing.T) {
			fullRSI := CalculateRSI(prices, period)
			lastRSI, err := CalculateLastRSI(prices, period)

			assert.NoError(t, err)
			assert.InDelta(t, fullRSI[len(fullRSI)-1], lastRSI, 0.0001)
		})
	}
}

func BenchmarkCalculateRSI(b *testing.B) {
	prices := make([]float64, 1000)
	for i := range prices {
		prices[i] = float64(i % 100)
	}

	b.ResetTimer()
	for b.Loop() {
		CalculateRSI(prices, 14)
	}
}

func BenchmarkCalculateLastRSI(b *testing.B) {
	prices := make([]float64, 1000)
	for i := range prices {
		prices[i] = float64(i % 100)
	}

	b.ResetTimer()
	for b.Loop() {
		_, _ = CalculateLastRSI(prices, 14)
	}
}

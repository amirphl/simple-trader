package pattern

import (
	"fmt"
	"time"
)

// Candle represents a candlestick with OHLCV data
type Candle struct {
	Timestamp time.Time
	Open      float64
	High      float64
	Low       float64
	Close     float64
	Volume    float64
	Symbol    string
	Timeframe  string
}

// Pattern is the interface for all patterns (candlestick, price action, etc.)
type Pattern interface {
	Name() string
	Description() string
	Detect(candles []Candle) ([]PatternMatch, error)
}

// PatternMatch represents a detected pattern
type PatternMatch struct {
	Index       int
	Pattern     string
	Description string
	Strength    float64 // 0.0 to 1.0, indicating pattern strength
	Direction   string  // "bullish", "bearish", or "neutral"
	Timestamp   time.Time
}

// PatternType represents the type of pattern
type PatternType string

const (
	PatternTypeBullish PatternType = "bullish"
	PatternTypeBearish PatternType = "bearish"
	PatternTypeNeutral PatternType = "neutral"
)

// PatternStrength represents the strength of a pattern
type PatternStrength float64

const (
	StrengthWeak   PatternStrength = 0.3
	StrengthMedium PatternStrength = 0.6
	StrengthStrong PatternStrength = 0.9
)

// ValidateCandle validates a candle's OHLC relationships
func ValidateCandle(c Candle) error {
	if c.High < c.Low {
		return fmt.Errorf("high cannot be less than low")
	}
	if c.Open < c.Low || c.Open > c.High {
		return fmt.Errorf("open must be between high and low")
	}
	if c.Close < c.Low || c.Close > c.High {
		return fmt.Errorf("close must be between high and low")
	}
	if c.Volume < 0 {
		return fmt.Errorf("volume cannot be negative")
	}
	return nil
}

// GetBodySize returns the absolute size of the candle body
func (c *Candle) GetBodySize() float64 {
	return abs(c.Close - c.Open)
}

// GetUpperShadow returns the size of the upper shadow (wick)
func (c *Candle) GetUpperShadow() float64 {
	return c.High - max(c.Open, c.Close)
}

// GetLowerShadow returns the size of the lower shadow (wick)
func (c *Candle) GetLowerShadow() float64 {
	return min(c.Open, c.Close) - c.Low
}

// GetTotalRange returns the total range of the candle
func (c *Candle) GetTotalRange() float64 {
	return c.High - c.Low
}

// IsBullish returns true if the candle is bullish (close > open)
func (c *Candle) IsBullish() bool {
	return c.Close > c.Open
}

// IsBearish returns true if the candle is bearish (close < open)
func (c *Candle) IsBearish() bool {
	return c.Close < c.Open
}

// IsDoji returns true if the candle is a doji (very small body)
func (c *Candle) IsDoji() bool {
	bodySize := c.GetBodySize()
	totalRange := c.GetTotalRange()
	if totalRange == 0 {
		return false
	}
	return bodySize/totalRange < 0.1 // Body is less than 10% of total range
}

// GetBodyRatio returns the ratio of body size to total range
func (c *Candle) GetBodyRatio() float64 {
	bodySize := c.GetBodySize()
	totalRange := c.GetTotalRange()
	if totalRange == 0 {
		return 0
	}
	return bodySize / totalRange
}

// GetUpperShadowRatio returns the ratio of upper shadow to total range
func (c *Candle) GetUpperShadowRatio() float64 {
	upperShadow := c.GetUpperShadow()
	totalRange := c.GetTotalRange()
	if totalRange == 0 {
		return 0
	}
	return upperShadow / totalRange
}

// GetLowerShadowRatio returns the ratio of lower shadow to total range
func (c *Candle) GetLowerShadowRatio() float64 {
	lowerShadow := c.GetLowerShadow()
	totalRange := c.GetTotalRange()
	if totalRange == 0 {
		return 0
	}
	return lowerShadow / totalRange
}

// Utility functions
func abs(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}

func max(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

func min(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}

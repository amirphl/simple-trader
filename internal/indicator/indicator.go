package indicator

// Indicator is the interface for all technical indicators.
type Indicator interface {
	Name() string
	Calculate(values []float64, params ...float64) ([]float64, error)
}

// SMA, EMA, RSI, MACD, etc. will implement this interface. 
package signal

import (
	"time"

	"github.com/amirphl/simple-trader/internal/candle"
	"github.com/amirphl/simple-trader/internal/strategy/position"
)

type Signal struct {
	Time         time.Time         `json:"time"`
	Position     position.Position `json:"position"`
	Reason       string            `json:"reason"`        // indicator/pattern/price action
	StrategyName string            `json:"strategy_name"` // TODO: FILL
	TriggerPrice float64           `json:"trigger_price"` // TODO: FILL
	Candle       *candle.Candle    `json:"candle"`        // TODO: FILL
}

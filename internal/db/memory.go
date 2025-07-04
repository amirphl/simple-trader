package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/amirphl/simple-trader/internal/tfutils"
)

type MemoryStorage struct {
	mu sync.RWMutex

	// Candles keyed by symbol|timeframe|timestamp|source
	candles map[string]Candle

	// Orderbooks and ticks by symbol
	orderbooks map[string][]OrderBook
	ticks      map[string][]Tick

	// Orders by orderID
	orders map[string]Order

	// Events (append-only)
	events []Event

	// Positions by ID and auto-increment counter
	positions      map[int64]Position
	nextPositionID int64
}

func NewMemory() *MemoryStorage {
	return &MemoryStorage{
		candles:    make(map[string]Candle),
		orderbooks: make(map[string][]OrderBook),
		ticks:      make(map[string][]Tick),
		orders:     make(map[string]Order),
		events:     make([]Event, 0, 1024),
		positions:  make(map[int64]Position),
	}
}

// GetDB returns nil for in-memory storage (no SQL database)
func (m *MemoryStorage) GetDB() *sql.DB { return nil }

type sqlDBShim struct{}

// -------- CandleStorage --------

func candleKey(symbol, timeframe string, ts time.Time, source string) string {
	return strings.ToUpper(symbol) + "|" + timeframe + "|" + ts.UTC().Format(time.RFC3339Nano) + "|" + source
}

func (m *MemoryStorage) SaveCandle(ctx context.Context, c Candle) error {
	if err := c.Validate(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	c.Timestamp = c.Timestamp.UTC()
	m.candles[candleKey(c.Symbol, c.Timeframe, c.Timestamp, c.Source)] = c
	return nil
}

func (m *MemoryStorage) SaveCandles(ctx context.Context, candles []Candle) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := range candles {
		if err := candles[i].Validate(); err != nil {
			return err
		}
		candles[i].Timestamp = candles[i].Timestamp.UTC()
		m.candles[candleKey(candles[i].Symbol, candles[i].Timeframe, candles[i].Timestamp, candles[i].Source)] = candles[i]
	}
	return nil
}

func (m *MemoryStorage) SaveConstructedCandles(ctx context.Context, candles []Candle) error {
	for i := range candles {
		candles[i].Source = "constructed"
	}
	return m.SaveCandles(ctx, candles)
}

func (m *MemoryStorage) SaveRaw1mCandles(ctx context.Context, candles []Candle) error {
	for _, c := range candles {
		if c.Timeframe != "1m" {
			return fmt.Errorf("failed to save candle for %s %s at %s: timeframe is not 1m", c.Symbol, c.Timeframe, c.Timestamp)
		}
	}

	return m.SaveCandles(ctx, candles)
}

func (m *MemoryStorage) GetCandle(ctx context.Context, symbol, timeframe string, timestamp time.Time, source string) (*Candle, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if c, ok := m.candles[candleKey(symbol, timeframe, timestamp, source)]; ok {
		cc := c
		return &cc, nil
	}
	return nil, nil
}

func (m *MemoryStorage) GetCandles(ctx context.Context, symbol, timeframe, source string, start, end time.Time) ([]Candle, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	start = start.UTC()
	end = end.UTC()
	var out []Candle
	for _, c := range m.candles {
		if !strings.EqualFold(c.Symbol, symbol) || c.Timeframe != timeframe {
			continue
		}
		if source != "" && c.Source != source {
			continue
		}
		if (c.Timestamp.Equal(start) || c.Timestamp.After(start)) && c.Timestamp.Before(end) {
			out = append(out, c)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Timestamp.Before(out[j].Timestamp) })
	return out, nil
}

func (m *MemoryStorage) GetCandlesV2(ctx context.Context, timeframe string, start, end time.Time) ([]Candle, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	start = start.UTC()
	end = end.UTC()
	var out []Candle
	for _, c := range m.candles {
		if c.Timeframe != timeframe {
			continue
		}
		if (c.Timestamp.Equal(start) || c.Timestamp.After(start)) && c.Timestamp.Before(end) {
			out = append(out, c)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if strings.EqualFold(out[i].Symbol, out[j].Symbol) {
			return out[i].Timestamp.Before(out[j].Timestamp)
		}
		return strings.ToUpper(out[i].Symbol) < strings.ToUpper(out[j].Symbol)
	})
	return out, nil
}

func (m *MemoryStorage) GetRawCandles(ctx context.Context, symbol, timeframe string, start, end time.Time) ([]Candle, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	start = start.UTC()
	end = end.UTC()
	var out []Candle
	for _, c := range m.candles {
		if !strings.EqualFold(c.Symbol, symbol) || c.Timeframe != timeframe {
			continue
		}
		if c.Source == "constructed" {
			continue
		}
		if (c.Timestamp.Equal(start) || c.Timestamp.After(start)) && c.Timestamp.Before(end) {
			out = append(out, c)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Timestamp.Before(out[j].Timestamp) })
	return out, nil
}

func (m *MemoryStorage) GetLatestCandle(ctx context.Context, symbol, timeframe string) (*Candle, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var latest *Candle
	for _, c := range m.candles {
		if !strings.EqualFold(c.Symbol, symbol) || c.Timeframe != timeframe {
			continue
		}
		if latest == nil || c.Timestamp.After(latest.Timestamp) {
			cc := c
			latest = &cc
		}
	}
	return latest, nil
}

func (m *MemoryStorage) GetLatestCandleInRange(ctx context.Context, symbol, timeframe string, start, end time.Time) (*Candle, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	start = start.UTC()
	end = end.UTC()
	var latest *Candle
	for _, c := range m.candles {
		if !strings.EqualFold(c.Symbol, symbol) || c.Timeframe != timeframe {
			continue
		}
		if (c.Timestamp.Equal(start) || c.Timestamp.After(start)) && c.Timestamp.Before(end) {
			if latest == nil || c.Timestamp.After(latest.Timestamp) {
				cc := c
				latest = &cc
			}
		}
	}
	return latest, nil
}

func (m *MemoryStorage) GetLatestConstructedCandle(ctx context.Context, symbol, timeframe string) (*Candle, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var latest *Candle
	for _, c := range m.candles {
		if !strings.EqualFold(c.Symbol, symbol) || c.Timeframe != timeframe || c.Source != "constructed" {
			continue
		}
		if latest == nil || c.Timestamp.After(latest.Timestamp) {
			cc := c
			latest = &cc
		}
	}
	return latest, nil
}

func (m *MemoryStorage) DeleteCandles(ctx context.Context, symbol, timeframe string, before time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	before = before.UTC()
	for k, c := range m.candles {
		if strings.EqualFold(c.Symbol, symbol) && c.Timeframe == timeframe && c.Timestamp.Before(before) {
			delete(m.candles, k)
		}
	}
	return nil
}

func (m *MemoryStorage) DeleteCandlesInRange(ctx context.Context, symbol, timeframe, source string, start, end time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	start = start.UTC()
	end = end.UTC()
	for k, c := range m.candles {
		if !strings.EqualFold(c.Symbol, symbol) || c.Timeframe != timeframe {
			continue
		}
		if source != "" && c.Source != source {
			continue
		}
		if (c.Timestamp.Equal(start) || c.Timestamp.After(start)) && c.Timestamp.Before(end) {
			delete(m.candles, k)
		}
	}
	return nil
}

func (m *MemoryStorage) DeleteConstructedCandles(ctx context.Context, symbol, timeframe string, before time.Time) error {
	return m.DeleteCandles(ctx, symbol, timeframe, before)
}

func (m *MemoryStorage) GetCandleCount(ctx context.Context, symbol, timeframe string, start, end time.Time) (int, error) {
	cs, err := m.GetCandles(ctx, symbol, timeframe, "", start, end)
	if err != nil {
		return 0, err
	}
	return len(cs), nil
}

func (m *MemoryStorage) GetConstructedCandleCount(ctx context.Context, symbol, timeframe string, start, end time.Time) (int, error) {
	cs, err := m.GetCandles(ctx, symbol, timeframe, "constructed", start, end)
	if err != nil {
		return 0, err
	}
	return len(cs), nil
}

func (m *MemoryStorage) UpdateCandle(ctx context.Context, c Candle) error {
	if err := c.Validate(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	key := candleKey(c.Symbol, c.Timeframe, c.Timestamp, c.Source)
	if _, ok := m.candles[key]; !ok {
		return errors.New("candle not found to update")
	}
	m.candles[key] = c
	return nil
}

func (m *MemoryStorage) UpdateCandles(ctx context.Context, candles []Candle) error {
	for i := range candles {
		if err := m.UpdateCandle(ctx, candles[i]); err != nil {
			return err
		}
	}
	return nil
}

func (m *MemoryStorage) GetAggregationStats(ctx context.Context, symbol string) (map[string]any, error) {
	stats := make(map[string]any)
	latest1m, _ := m.GetLatestCandle(ctx, symbol, "1m")
	if latest1m != nil {
		stats["latest_1m"] = map[string]any{
			"timestamp":   latest1m.Timestamp,
			"close":       latest1m.Close,
			"is_complete": latest1m.IsComplete(),
		}
	}
	timeframes := []string{"1m", "5m", "15m", "30m", "1h", "4h", "1d"}
	end := time.Now()
	start := end.Add(-24 * time.Hour)
	for _, tf := range timeframes {
		cnt, _ := m.GetCandleCount(ctx, symbol, tf, start, end)
		stats["count_24h_"+tf] = cnt
	}
	return stats, nil
}

func (m *MemoryStorage) GetMissingCandleRanges(ctx context.Context, symbol string, start, end time.Time) ([]struct{ Start, End time.Time }, error) {
	start = start.UTC()
	end = end.UTC()
	oneMin := tfutils.GetTimeframeDuration("1m")
	present := make(map[time.Time]bool)
	cs, _ := m.GetCandles(ctx, symbol, "1m", "", start, end)
	for _, c := range cs {
		present[c.Timestamp.Truncate(oneMin)] = true
	}
	var gaps []struct{ Start, End time.Time }
	inGap := false
	var gapStart time.Time
	for ts := start; ts.Before(end); ts = ts.Add(oneMin) {
		if !present[ts] {
			if !inGap {
				inGap = true
				gapStart = ts
			}
		} else if inGap {
			gaps = append(gaps, struct{ Start, End time.Time }{Start: gapStart, End: ts})
			inGap = false
		}
	}
	if inGap {
		gaps = append(gaps, struct{ Start, End time.Time }{Start: gapStart, End: end})
	}
	return gaps, nil
}

func (m *MemoryStorage) GetCandleSourceStats(ctx context.Context, symbol string, start, end time.Time) (map[string]any, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	start = start.UTC()
	end = end.UTC()
	bySource := make(map[string]map[string]int)
	var constructedCount, rawCount int
	for _, c := range m.candles {
		if !strings.EqualFold(c.Symbol, symbol) {
			continue
		}
		if (c.Timestamp.Equal(start) || c.Timestamp.After(start)) && c.Timestamp.Before(end) {
			if bySource[c.Source] == nil {
				bySource[c.Source] = make(map[string]int)
			}
			bySource[c.Source][c.Timeframe]++
			if c.Source == "constructed" {
				constructedCount++
			} else {
				rawCount++
			}
		}
	}
	stats := make(map[string]any)
	stats["source_distribution"] = bySource
	stats["constructed_count"] = constructedCount
	stats["raw_count"] = rawCount
	stats["total_count"] = constructedCount + rawCount
	return stats, nil
}

// -------- MarketStorage --------

func (m *MemoryStorage) SaveOrderBook(ctx context.Context, ob OrderBook) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	ob.Timestamp = ob.Timestamp.UTC()
	m.orderbooks[strings.ToUpper(ob.Symbol)] = append(m.orderbooks[strings.ToUpper(ob.Symbol)], ob)
	return nil
}

func (m *MemoryStorage) GetOrderBooks(ctx context.Context, symbol string, start, end time.Time) ([]OrderBook, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	start = start.UTC()
	end = end.UTC()
	var out []OrderBook
	for _, ob := range m.orderbooks[strings.ToUpper(symbol)] {
		if (ob.Timestamp.Equal(start) || ob.Timestamp.After(start)) && ob.Timestamp.Before(end) {
			out = append(out, ob)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Timestamp.Before(out[j].Timestamp) })
	return out, nil
}

func (m *MemoryStorage) SaveTick(ctx context.Context, tick Tick) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	tick.Timestamp = tick.Timestamp.UTC()
	m.ticks[strings.ToUpper(tick.Symbol)] = append(m.ticks[strings.ToUpper(tick.Symbol)], tick)
	return nil
}

func (m *MemoryStorage) GetTicks(ctx context.Context, symbol string, start, end time.Time) ([]Tick, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	start = start.UTC()
	end = end.UTC()
	var out []Tick
	for _, t := range m.ticks[strings.ToUpper(symbol)] {
		if (t.Timestamp.Equal(start) || t.Timestamp.After(start)) && t.Timestamp.Before(end) {
			out = append(out, t)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Timestamp.Before(out[j].Timestamp) })
	return out, nil
}

// -------- OrderStorage --------

func (m *MemoryStorage) SaveOrder(ctx context.Context, o Order) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.orders[o.OrderID] = o
	return nil
}

func (m *MemoryStorage) GetOrder(ctx context.Context, orderID string) (*Order, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if o, ok := m.orders[orderID]; ok {
		oo := o
		return &oo, nil
	}
	return nil, nil
}

func (m *MemoryStorage) GetOpenOrders(ctx context.Context) ([]Order, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var out []Order
	for _, o := range m.orders {
		s := strings.ToUpper(o.Status)
		if s != "FILLED" && s != "CANCELED" && s != "EXPIRED" && s != "CLOSED" {
			out = append(out, o)
		}
	}
	return out, nil
}

func (m *MemoryStorage) CloseOrder(ctx context.Context, orderID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	o, ok := m.orders[orderID]
	if !ok {
		return errors.New("order not found")
	}
	o.Status = "CLOSED"
	o.UpdatedAt = time.Now().UTC()
	m.orders[orderID] = o
	return nil
}

// -------- JournalStorage --------

func (m *MemoryStorage) LogEvent(ctx context.Context, event Event) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	event.Time = event.Time.UTC()
	m.events = append(m.events, event)
	return nil
}

func (m *MemoryStorage) GetEvents(ctx context.Context, eventType string, start, end time.Time) ([]Event, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	start = start.UTC()
	end = end.UTC()
	var out []Event
	for _, e := range m.events {
		if e.Type == eventType && (e.Time.Equal(start) || e.Time.After(start)) && e.Time.Before(end) {
			out = append(out, e)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Time.Before(out[j].Time) })
	return out, nil
}

// -------- PositionStorage --------

func (m *MemoryStorage) SavePosition(ctx context.Context, pos Position) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nextPositionID++
	id := m.nextPositionID
	now := time.Now().UTC()
	pos.ID = id
	pos.CreatedAt = now
	pos.UpdatedAt = now
	m.positions[id] = pos
	return id, nil
}

func (m *MemoryStorage) UpdatePosition(ctx context.Context, pos Position) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	stored, ok := m.positions[pos.ID]
	if !ok {
		return errors.New("position not found")
	}
	pos.CreatedAt = stored.CreatedAt
	pos.UpdatedAt = time.Now().UTC()
	m.positions[pos.ID] = pos
	return nil
}

func (m *MemoryStorage) GetPosition(ctx context.Context, strategyName, symbol string) (*Position, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, p := range m.positions {
		if p.StrategyName == strategyName && strings.EqualFold(p.Symbol, symbol) && p.Active {
			pp := p
			return &pp, nil
		}
	}
	return nil, nil
}

func (m *MemoryStorage) GetPositionByID(ctx context.Context, id int64) (*Position, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if p, ok := m.positions[id]; ok {
		pp := p
		return &pp, nil
	}
	return nil, nil
}

func (m *MemoryStorage) GetAllPositions(ctx context.Context) ([]Position, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]Position, 0, len(m.positions))
	for _, p := range m.positions {
		out = append(out, p)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].StrategyName == out[j].StrategyName {
			return strings.ToUpper(out[i].Symbol) < strings.ToUpper(out[j].Symbol)
		}
		return out[i].StrategyName < out[j].StrategyName
	})
	return out, nil
}

func (m *MemoryStorage) GetActivePositions(ctx context.Context) ([]Position, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var out []Position
	for _, p := range m.positions {
		if p.Active {
			out = append(out, p)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].StrategyName == out[j].StrategyName {
			return strings.ToUpper(out[i].Symbol) < strings.ToUpper(out[j].Symbol)
		}
		return out[i].StrategyName < out[j].StrategyName
	})
	return out, nil
}

func (m *MemoryStorage) DeletePosition(ctx context.Context, strategyName, symbol string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for id, p := range m.positions {
		if p.StrategyName == strategyName && strings.EqualFold(p.Symbol, symbol) {
			delete(m.positions, id)
		}
	}
	return nil
}

func (m *MemoryStorage) DeletePositionByID(ctx context.Context, id int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.positions[id]; !ok {
		return errors.New("position not found")
	}
	delete(m.positions, id)
	return nil
}

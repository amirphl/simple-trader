package db

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/amirphl/simple-trader/internal/candle"
	"github.com/amirphl/simple-trader/internal/journal"
	"github.com/amirphl/simple-trader/internal/market"
	"github.com/amirphl/simple-trader/internal/order"
	_ "github.com/lib/pq"
)

type PostgresDB struct {
	db *sql.DB
}

func NewPostgresDB(connStr string, maxOpen, maxIdle int) (*PostgresDB, error) {
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(maxOpen)
	db.SetMaxIdleConns(maxIdle)
	return &PostgresDB{db: db}, nil
}

func (p *PostgresDB) SaveCandles(candles []candle.Candle) error {
	if len(candles) == 0 {
		return nil
	}

	tx, err := p.db.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	stmt, err := tx.Prepare(`INSERT INTO candles (symbol, timeframe, timestamp, open, high, low, close, volume, source)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
		ON CONFLICT (symbol, timeframe, timestamp, source) DO UPDATE SET 
			open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low, 
			close=EXCLUDED.close, volume=EXCLUDED.volume`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, c := range candles {
		// Validate candle before saving
		if err := c.Validate(); err != nil {
			return fmt.Errorf("invalid candle for %s %s at %s: %w", c.Symbol, c.Timeframe, c.Timestamp, err)
		}

		_, err := stmt.Exec(c.Symbol, c.Timeframe, c.Timestamp, c.Open, c.High, c.Low, c.Close, c.Volume, c.Source)
		if err != nil {
			return fmt.Errorf("failed to save candle for %s %s at %s: %w", c.Symbol, c.Timeframe, c.Timestamp, err)
		}
	}

	return tx.Commit()
}

// SaveRaw1mCandles efficiently saves raw 1m candles with optimized batch processing
// TODO: TX
func (p *PostgresDB) SaveRaw1mCandles(candles []candle.Candle) error {
	if len(candles) == 0 {
		return nil
	}

	for _, c := range candles {
		if c.Timeframe != "1m" {
			return fmt.Errorf("failed to save candle for %s %s at %s: timeframe is not 1m", c.Symbol, c.Timeframe, c.Timestamp)
		}
	}

	return p.SaveCandles(candles)
}

// Get1mCandlesForAggregation efficiently retrieves 1m candles for aggregation
func (p *PostgresDB) Get1mCandlesForAggregation(symbol string, start, end time.Time) ([]candle.Candle, error) {
	rows, err := p.db.Query(`
		SELECT timestamp, open, high, low, close, volume, symbol, timeframe, source 
		FROM candles 
		WHERE symbol=$1 AND timeframe='1m' AND timestamp >= $2 AND timestamp <= $3 
		ORDER BY timestamp ASC`,
		symbol, start, end)
	if err != nil {
		return nil, fmt.Errorf("failed to query 1m candles: %w", err)
	}
	defer rows.Close()

	var candles []candle.Candle
	for rows.Next() {
		var c candle.Candle
		if err := rows.Scan(&c.Timestamp, &c.Open, &c.High, &c.Low, &c.Close, &c.Volume, &c.Symbol, &c.Timeframe, &c.Source); err != nil {
			return nil, fmt.Errorf("failed to scan candle: %w", err)
		}
		candles = append(candles, c)
	}

	return candles, nil
}

// GetCandles retrieves candles with optimized querying
func (p *PostgresDB) GetCandles(symbol, timeframe string, start, end time.Time) ([]candle.Candle, error) {
	rows, err := p.db.Query(`
		SELECT timestamp, open, high, low, close, volume, symbol, timeframe, source 
		FROM candles 
		WHERE symbol=$1 AND timeframe=$2 AND timestamp >= $3 AND timestamp <= $4 
		ORDER BY timestamp ASC`,
		symbol, timeframe, start, end)
	if err != nil {
		return nil, fmt.Errorf("failed to query candles: %w", err)
	}
	defer rows.Close()

	var candles []candle.Candle
	for rows.Next() {
		var c candle.Candle
		if err := rows.Scan(&c.Timestamp, &c.Open, &c.High, &c.Low, &c.Close, &c.Volume, &c.Symbol, &c.Timeframe, &c.Source); err != nil {
			return nil, fmt.Errorf("failed to scan candle: %w", err)
		}
		candles = append(candles, c)
	}

	return candles, nil
}

// GetCandlesV2 retrieves candles with a specific timeframe in a time range without filtering by symbol
// This is useful for bulk operations across all symbols
func (p *PostgresDB) GetCandlesV2(timeframe string, start, end time.Time) ([]candle.Candle, error) {
	query := `
		SELECT timestamp, open, high, low, close, volume, symbol, timeframe, source 
		FROM candles 
		WHERE timeframe=$1 AND timestamp >= $2 AND timestamp <= $3 
		ORDER BY symbol, timestamp ASC`

	rows, err := p.db.Query(query, timeframe, start, end)
	if err != nil {
		return nil, fmt.Errorf("failed to query candles: %w", err)
	}
	defer rows.Close()

	// Pre-allocate slice with reasonable capacity to reduce reallocations
	candles := make([]candle.Candle, 0, 1000)

	for rows.Next() {
		var c candle.Candle
		if err := rows.Scan(&c.Timestamp, &c.Open, &c.High, &c.Low, &c.Close, &c.Volume,
			&c.Symbol, &c.Timeframe, &c.Source); err != nil {
			return nil, fmt.Errorf("failed to scan candle: %w", err)
		}
		candles = append(candles, c)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating candle rows: %w", err)
	}

	return candles, nil
}

// GetLatestCandle retrieves the latest candle with optimized querying
func (p *PostgresDB) GetLatestCandle(symbol, timeframe string) (*candle.Candle, error) {
	row := p.db.QueryRow(`
		SELECT timestamp, open, high, low, close, volume, symbol, timeframe, source 
		FROM candles 
		WHERE symbol=$1 AND timeframe=$2 
		ORDER BY timestamp DESC LIMIT 1`,
		symbol, timeframe)

	var c candle.Candle
	if err := row.Scan(&c.Timestamp, &c.Open, &c.High, &c.Low, &c.Close, &c.Volume, &c.Symbol, &c.Timeframe, &c.Source); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to scan latest candle: %w", err)
	}
	return &c, nil
}

// GetLatest1mCandle retrieves the latest 1m candle for a symbol
func (p *PostgresDB) GetLatest1mCandle(symbol string) (*candle.Candle, error) {
	return p.GetLatestCandle(symbol, "1m")
}

// DeleteCandles removes old candles with optimized deletion
func (p *PostgresDB) DeleteCandles(symbol, timeframe string, before time.Time) error {
	result, err := p.db.Exec(`DELETE FROM candles WHERE symbol=$1 AND timeframe=$2 AND timestamp < $3`,
		symbol, timeframe, before)
	if err != nil {
		return fmt.Errorf("failed to delete candles: %w", err)
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected > 0 {
		fmt.Printf("Deleted %d old candles for %s %s\n", rowsAffected, symbol, timeframe)
	}

	return nil
}

// GetCandleCount returns the count of candles in a time range
func (p *PostgresDB) GetCandleCount(symbol, timeframe string, start, end time.Time) (int, error) {
	var count int
	err := p.db.QueryRow(`
		SELECT COUNT(*) FROM candles 
		WHERE symbol=$1 AND timeframe=$2 AND timestamp >= $3 AND timestamp <= $4`,
		symbol, timeframe, start, end).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to get candle count: %w", err)
	}
	return count, nil
}

// UpdateCandle updates an existing candle
// TODO: TX
func (p *PostgresDB) UpdateCandle(c candle.Candle) error {
	if err := c.Validate(); err != nil {
		return fmt.Errorf("invalid candle for update: %w", err)
	}

	result, err := p.db.Exec(`
		UPDATE candles 
		SET open=$1, high=$2, low=$3, close=$4, volume=$5 
		WHERE symbol=$6 AND timeframe=$7 AND timestamp=$8 AND source=$9`,
		c.Open, c.High, c.Low, c.Close, c.Volume, c.Symbol, c.Timeframe, c.Timestamp, c.Source)
	if err != nil {
		return fmt.Errorf("failed to update candle: %w", err)
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		return fmt.Errorf("no candle found to update for %s %s at %s", c.Symbol, c.Timeframe, c.Timestamp)
	}

	return nil
}

// UpdateCandles updates multiple candles in a single transaction using a bulk update approach
func (p *PostgresDB) UpdateCandles(candles []candle.Candle) error {
	if len(candles) == 0 {
		return nil
	}

	// Start a transaction
	tx, err := p.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	// Create a temporary table for the updates
	_, err = tx.Exec(`
        CREATE TEMPORARY TABLE temp_candle_updates (
            symbol TEXT,
            timeframe TEXT,
            timestamp TIMESTAMP,
            source TEXT,
            open FLOAT,
            high FLOAT,
            low FLOAT,
            close FLOAT,
            volume FLOAT
        ) ON COMMIT DROP
    `)
	if err != nil {
		return fmt.Errorf("failed to create temporary table: %w", err)
	}

	// Prepare the statement for bulk insert into temp table
	stmt, err := tx.Prepare(`
        INSERT INTO temp_candle_updates (symbol, timeframe, timestamp, source, open, high, low, close, volume)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
    `)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	// Insert all candles into the temp table
	for _, c := range candles {
		// Validate candle before updating
		if err := c.Validate(); err != nil {
			return fmt.Errorf("invalid candle for update (%s %s at %s): %w",
				c.Symbol, c.Timeframe, c.Timestamp, err)
		}

		_, err = stmt.Exec(
			c.Symbol, c.Timeframe, c.Timestamp, c.Source,
			c.Open, c.High, c.Low, c.Close, c.Volume)
		if err != nil {
			return fmt.Errorf("failed to insert into temp table: %w", err)
		}
	}

	// Perform the actual update in a single statement
	result, err := tx.Exec(`
        UPDATE candles AS c
        SET 
            open = t.open,
            high = t.high,
            low = t.low,
            close = t.close,
            volume = t.volume
        FROM temp_candle_updates AS t
        WHERE 
            c.symbol = t.symbol AND
            c.timeframe = t.timeframe AND
            c.timestamp = t.timestamp AND
            c.source = t.source
    `)
	if err != nil {
		return fmt.Errorf("failed to update candles: %w", err)
	}

	// Check if any rows were affected
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("no candles were updated out of %d attempted", len(candles))
	}

	// Commit the transaction
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// DeleteCandle removes a specific candle
// TODO: TX
func (p *PostgresDB) DeleteCandle(symbol, timeframe string, timestamp time.Time, source string) error {
	result, err := p.db.Exec(`DELETE FROM candles WHERE symbol=$1 AND timeframe=$2 AND timestamp=$3 AND source=$4`,
		symbol, timeframe, timestamp, source)
	if err != nil {
		return fmt.Errorf("failed to delete candle: %w", err)
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		return fmt.Errorf("no candle found to delete for %s %s at %s", symbol, timeframe, timestamp)
	}

	return nil
}

// GetAggregationStats returns statistics useful for aggregation
func (p *PostgresDB) GetAggregationStats(symbol string) (map[string]any, error) {
	stats := make(map[string]any)

	// Get latest 1m candle
	latest1m, err := p.GetLatest1mCandle(symbol)
	if err != nil {
		return nil, fmt.Errorf("failed to get latest 1m candle: %w", err)
	}

	if latest1m != nil {
		stats["latest_1m"] = map[string]any{
			"timestamp":   latest1m.Timestamp,
			"close":       latest1m.Close,
			"is_complete": latest1m.IsComplete(),
		}
	}

	// Get candle counts for different timeframes
	timeframes := []string{"1m", "5m", "15m", "30m", "1h", "4h", "1d"}
	end := time.Now()
	start := end.Add(-24 * time.Hour)

	for _, timeframe := range timeframes {
		count, err := p.GetCandleCount(symbol, timeframe, start, end)
		if err != nil {
			continue // Skip if error
		}
		stats[fmt.Sprintf("count_24h_%s", timeframe)] = count
	}

	return stats, nil
}

// GetMissingCandleRanges identifies gaps in 1m candle data
func (p *PostgresDB) GetMissingCandleRanges(symbol string, start, end time.Time) ([]struct{ Start, End time.Time }, error) {
	// Query for gaps in 1m candles
	rows, err := p.db.Query(`
		WITH time_series AS (
			SELECT generate_series($1::timestamp, $2::timestamp, '1 minute'::timeframe) as expected_time
		),
		actual_candles AS (
			SELECT timestamp FROM candles 
			WHERE symbol=$3 AND timeframe='1m' AND timestamp >= $1 AND timestamp <= $2
		)
		SELECT 
			ts.expected_time as gap_start,
			LEAD(ts.expected_time) OVER (ORDER BY ts.expected_time) as gap_end
		FROM time_series ts
		LEFT JOIN actual_candles ac ON ts.expected_time = ac.timestamp
		WHERE ac.timestamp IS NULL
		ORDER BY ts.expected_time`,
		start, end, symbol)
	if err != nil {
		return nil, fmt.Errorf("failed to query missing ranges: %w", err)
	}
	defer rows.Close()

	var gaps []struct{ Start, End time.Time }
	for rows.Next() {
		var gap struct{ Start, End time.Time }
		if err := rows.Scan(&gap.Start, &gap.End); err != nil {
			return nil, fmt.Errorf("failed to scan gap: %w", err)
		}
		gaps = append(gaps, gap)
	}

	return gaps, nil
}

func (p *PostgresDB) SaveOrderBook(ob market.OrderBook) error {
	tx, err := p.db.Begin()
	if err != nil {
		return err
	}
	bids, _ := json.Marshal(ob.Bids)
	asks, _ := json.Marshal(ob.Asks)
	_, err = tx.Exec(`INSERT INTO orderbooks (symbol, timestamp, bids, asks) VALUES ($1,$2,$3,$4)`, ob.Symbol, ob.Timestamp, bids, asks)
	if err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}

func (p *PostgresDB) GetOrderBooks(symbol string, start, end time.Time) ([]market.OrderBook, error) {
	rows, err := p.db.Query(`SELECT symbol, timestamp, bids, asks FROM orderbooks WHERE symbol=$1 AND timestamp >= $2 AND timestamp <= $3 ORDER BY timestamp ASC`, symbol, start, end)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var obs []market.OrderBook
	for rows.Next() {
		var ob market.OrderBook
		var bids, asks []byte
		if err := rows.Scan(&ob.Symbol, &ob.Timestamp, &bids, &asks); err != nil {
			return nil, err
		}
		json.Unmarshal(bids, &ob.Bids)
		json.Unmarshal(asks, &ob.Asks)
		obs = append(obs, ob)
	}
	return obs, nil
}

func (p *PostgresDB) DeleteOrderBooks(symbol string, before time.Time) error {
	// TODO: TX
	_, err := p.db.Exec(`DELETE FROM orderbooks WHERE symbol=$1 AND timestamp < $2`, symbol, before)
	return err
}

func (p *PostgresDB) SaveTicks(ticks []market.Tick) error {
	tx, err := p.db.Begin()
	if err != nil {
		return err
	}
	stmt, err := tx.Prepare(`INSERT INTO ticks (symbol, price, quantity, side, timestamp) VALUES ($1,$2,$3,$4,$5)`)
	if err != nil {
		tx.Rollback()
		return err
	}
	defer stmt.Close()
	for _, t := range ticks {
		_, err := stmt.Exec(t.Symbol, t.Price, t.Quantity, t.Side, t.Timestamp)
		if err != nil {
			tx.Rollback()
			return err
		}
	}
	return tx.Commit()
}

func (p *PostgresDB) GetTicks(symbol string, start, end time.Time) ([]market.Tick, error) {
	rows, err := p.db.Query(`SELECT symbol, price, quantity, side, timestamp FROM ticks WHERE symbol=$1 AND timestamp >= $2 AND timestamp <= $3 ORDER BY timestamp ASC`, symbol, start, end)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ticks []market.Tick
	for rows.Next() {
		var t market.Tick
		if err := rows.Scan(&t.Symbol, &t.Price, &t.Quantity, &t.Side, &t.Timestamp); err != nil {
			return nil, err
		}
		ticks = append(ticks, t)
	}
	return ticks, nil
}

func (p *PostgresDB) DeleteTicks(symbol string, before time.Time) error {
	// TODO: TX
	_, err := p.db.Exec(`DELETE FROM ticks WHERE symbol=$1 AND timestamp < $2`, symbol, before)
	return err
}

func (p *PostgresDB) SaveOrder(o order.OrderResponse) error {
	// TODO: TX
	_, err := p.db.Exec(`INSERT INTO orders (order_id, symbol, side, type, price, quantity, status, filled_qty, avg_price, created_at, updated_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
		ON CONFLICT (order_id) DO UPDATE SET status=EXCLUDED.status, filled_qty=EXCLUDED.filled_qty, avg_price=EXCLUDED.avg_price, updated_at=EXCLUDED.updated_at`,
		o.OrderID, o.Symbol, o.Side, o.Type, o.Price, o.Quantity, o.Status, o.FilledQty, o.AvgPrice, o.Timestamp, o.Timestamp)
	return err
}

func (p *PostgresDB) GetOrder(orderID string) (order.OrderResponse, error) {
	row := p.db.QueryRow(`SELECT order_id, symbol, side, type, price, quantity, status, filled_qty, avg_price, created_at, updated_at FROM orders WHERE order_id=$1`, orderID)
	var o order.OrderResponse
	if err := row.Scan(&o.OrderID, &o.Symbol, &o.Side, &o.Type, &o.Price, &o.Quantity, &o.Status, &o.FilledQty, &o.AvgPrice, &o.Timestamp, &o.UpdatedAt); err != nil {
		return o, err
	}
	return o, nil
}

func (p *PostgresDB) UpdateOrderStatus(orderID, status string, filledQty, avgPrice float64, updatedAt time.Time) error {
	// TODO: TX
	_, err := p.db.Exec(`UPDATE orders SET status=$1, filled_qty=$2, avg_price=$3, updated_at=$4 WHERE order_id=$5`, status, filledQty, avgPrice, updatedAt, orderID)
	return err
}

func (p *PostgresDB) DeleteOrder(orderID string) error {
	// TODO: TX
	_, err := p.db.Exec(`DELETE FROM orders WHERE order_id=$1`, orderID)
	return err
}

func (p *PostgresDB) LogEvent(event journal.Event) error {
	data, _ := json.Marshal(event.Data)
	// TODO: TX
	_, err := p.db.Exec(`INSERT INTO events (time, type, description, data) VALUES ($1,$2,$3,$4)`, event.Time, event.Type, event.Description, data)
	return err
}

func (p *PostgresDB) GetEvents(eventType string, start, end time.Time) ([]journal.Event, error) {
	rows, err := p.db.Query(`SELECT time, type, description, data FROM events WHERE type=$1 AND time >= $2 AND time <= $3 ORDER BY time ASC`, eventType, start, end)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var events []journal.Event
	for rows.Next() {
		var e journal.Event
		var data []byte
		if err := rows.Scan(&e.Time, &e.Type, &e.Description, &data); err != nil {
			return nil, err
		}
		json.Unmarshal(data, &e.Data)
		events = append(events, e)
	}
	return events, nil
}

func (p *PostgresDB) DeleteEvents(eventType string, before time.Time) error {
	// TODO: TX
	_, err := p.db.Exec(`DELETE FROM events WHERE type=$1 AND time < $2`, eventType, before)
	return err
}

func (p *PostgresDB) SaveState(state map[string]any) error {
	tx, err := p.db.Begin()
	if err != nil {
		return err
	}
	for k, v := range state {
		val, _ := json.Marshal(v)
		_, err := tx.Exec(`INSERT INTO state (key, value) VALUES ($1, $2) ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value`, k, val)
		if err != nil {
			tx.Rollback()
			return err
		}
	}
	return tx.Commit()
}

func (p *PostgresDB) LoadState() (map[string]any, error) {
	rows, err := p.db.Query(`SELECT key, value FROM state`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	state := make(map[string]any)
	for rows.Next() {
		var k string
		var val []byte
		if err := rows.Scan(&k, &val); err != nil {
			return nil, err
		}
		var v any
		json.Unmarshal(val, &v)
		state[k] = v
	}
	return state, nil
}

func (p *PostgresDB) DeleteState(key string) error {
	// TODO: TX
	_, err := p.db.Exec(`DELETE FROM state WHERE key=$1`, key)
	return err
}

// SaveConstructedCandles efficiently saves constructed (aggregated) candles
func (p *PostgresDB) SaveConstructedCandles(candles []candle.Candle) error {
	if len(candles) == 0 {
		return nil
	}

	// Set source to "constructed" for all candles
	for i := range candles {
		candles[i].Source = "constructed"
	}

	return p.SaveCandles(candles)
}

// GetConstructedCandles retrieves only constructed candles
func (p *PostgresDB) GetConstructedCandles(symbol, timeframe string, start, end time.Time) ([]candle.Candle, error) {
	rows, err := p.db.Query(`
		SELECT timestamp, open, high, low, close, volume, symbol, timeframe, source 
		FROM candles 
		WHERE symbol=$1 AND timeframe=$2 AND source='constructed' AND timestamp >= $3 AND timestamp <= $4 
		ORDER BY timestamp ASC`,
		symbol, timeframe, start, end)
	if err != nil {
		return nil, fmt.Errorf("failed to query constructed candles: %w", err)
	}
	defer rows.Close()

	var candles []candle.Candle
	for rows.Next() {
		var c candle.Candle
		if err := rows.Scan(&c.Timestamp, &c.Open, &c.High, &c.Low, &c.Close, &c.Volume, &c.Symbol, &c.Timeframe, &c.Source); err != nil {
			return nil, fmt.Errorf("failed to scan constructed candle: %w", err)
		}
		candles = append(candles, c)
	}

	return candles, nil
}

// GetRawCandles retrieves only raw candles (not constructed)
func (p *PostgresDB) GetRawCandles(symbol, timeframe string, start, end time.Time) ([]candle.Candle, error) {
	rows, err := p.db.Query(`
		SELECT timestamp, open, high, low, close, volume, symbol, timeframe, source 
		FROM candles 
		WHERE symbol=$1 AND timeframe=$2 AND source != 'constructed' AND timestamp >= $3 AND timestamp <= $4 
		ORDER BY timestamp ASC`,
		symbol, timeframe, start, end)
	if err != nil {
		return nil, fmt.Errorf("failed to query raw candles: %w", err)
	}
	defer rows.Close()

	var candles []candle.Candle
	for rows.Next() {
		var c candle.Candle
		if err := rows.Scan(&c.Timestamp, &c.Open, &c.High, &c.Low, &c.Close, &c.Volume, &c.Symbol, &c.Timeframe, &c.Source); err != nil {
			return nil, fmt.Errorf("failed to scan raw candle: %w", err)
		}
		candles = append(candles, c)
	}

	return candles, nil
}

// GetLatestConstructedCandle retrieves the latest constructed candle
func (p *PostgresDB) GetLatestConstructedCandle(symbol, timeframe string) (*candle.Candle, error) {
	row := p.db.QueryRow(`
		SELECT timestamp, open, high, low, close, volume, symbol, timeframe, source 
		FROM candles 
		WHERE symbol=$1 AND timeframe=$2 AND source='constructed' 
		ORDER BY timestamp DESC LIMIT 1`,
		symbol, timeframe)

	var c candle.Candle
	if err := row.Scan(&c.Timestamp, &c.Open, &c.High, &c.Low, &c.Close, &c.Volume, &c.Symbol, &c.Timeframe, &c.Source); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to scan latest constructed candle: %w", err)
	}
	return &c, nil
}

// GetConstructedCandleCount returns the count of constructed candles
func (p *PostgresDB) GetConstructedCandleCount(symbol, timeframe string, start, end time.Time) (int, error) {
	var count int
	err := p.db.QueryRow(`
		SELECT COUNT(*) FROM candles 
		WHERE symbol=$1 AND timeframe=$2 AND source='constructed' AND timestamp >= $3 AND timestamp <= $4`,
		symbol, timeframe, start, end).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to get constructed candle count: %w", err)
	}
	return count, nil
}

// DeleteConstructedCandles removes old constructed candles
func (p *PostgresDB) DeleteConstructedCandles(symbol, timeframe string, before time.Time) error {
	// TODO: TX
	result, err := p.db.Exec(`DELETE FROM candles WHERE symbol=$1 AND timeframe=$2 AND source='constructed' AND timestamp < $3`,
		symbol, timeframe, before)
	if err != nil {
		return fmt.Errorf("failed to delete constructed candles: %w", err)
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected > 0 {
		fmt.Printf("Deleted %d old constructed candles for %s %s\n", rowsAffected, symbol, timeframe)
	}

	return nil
}

// GetCandleSourceStats returns statistics about candle sources
func (p *PostgresDB) GetCandleSourceStats(symbol string, start, end time.Time) (map[string]any, error) {
	stats := make(map[string]any)

	// Get source distribution
	rows, err := p.db.Query(`
		SELECT source, timeframe, COUNT(*) as count
		FROM candles 
		WHERE symbol=$1 AND timestamp >= $2 AND timestamp <= $3
		GROUP BY source, timeframe
		ORDER BY source, timeframe`,
		symbol, start, end)
	if err != nil {
		return nil, fmt.Errorf("failed to query source stats: %w", err)
	}
	defer rows.Close()

	sourceStats := make(map[string]map[string]int)
	for rows.Next() {
		var source, timeframe string
		var count int
		if err := rows.Scan(&source, &timeframe, &count); err != nil {
			return nil, fmt.Errorf("failed to scan source stat: %w", err)
		}

		if sourceStats[source] == nil {
			sourceStats[source] = make(map[string]int)
		}
		sourceStats[source][timeframe] = count
	}

	stats["source_distribution"] = sourceStats

	// Get constructed vs raw candle counts
	var constructedCount, rawCount int

	err = p.db.QueryRow(`
		SELECT COUNT(*) FROM candles 
		WHERE symbol=$1 AND source='constructed' AND timestamp >= $2 AND timestamp <= $3`,
		symbol, start, end).Scan(&constructedCount)
	if err != nil {
		constructedCount = 0
	}

	err = p.db.QueryRow(`
		SELECT COUNT(*) FROM candles 
		WHERE symbol=$1 AND source != 'constructed' AND timestamp >= $2 AND timestamp <= $3`,
		symbol, start, end).Scan(&rawCount)
	if err != nil {
		rawCount = 0
	}

	stats["constructed_count"] = constructedCount
	stats["raw_count"] = rawCount
	stats["total_count"] = constructedCount + rawCount

	return stats, nil
}

// GetOpenOrders retrieves all open orders (not filled/canceled/expired) from the DB
func (p *PostgresDB) GetOpenOrders() ([]order.OrderResponse, error) {
	rows, err := p.db.Query(`SELECT order_id, symbol, side, type, price, quantity, status, filled_qty, avg_price, created_at, updated_at FROM orders WHERE status NOT IN ('FILLED', 'CANCELED', 'EXPIRED', 'filled', 'canceled', 'expired')`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var orders []order.OrderResponse
	for rows.Next() {
		var o order.OrderResponse
		if err := rows.Scan(&o.OrderID, &o.Symbol, &o.Side, &o.Type, &o.Price, &o.Quantity, &o.Status, &o.FilledQty, &o.AvgPrice, &o.Timestamp, &o.UpdatedAt); err != nil {
			return nil, err
		}
		orders = append(orders, o)
	}
	return orders, nil
}

// GetLatestCandleInRange retrieves the latest candle within a specific time range
// This is useful when you need the most recent candle before a certain time
func (p *PostgresDB) GetLatestCandleInRange(symbol, timeframe string, start, end time.Time) (*candle.Candle, error) {
	row := p.db.QueryRow(`
		SELECT timestamp, open, high, low, close, volume, symbol, timeframe, source 
		FROM candles 
		WHERE symbol=$1 AND timeframe=$2 AND timestamp >= $3 AND timestamp <= $4
		ORDER BY timestamp DESC LIMIT 1`,
		symbol, timeframe, start, end)

	var c candle.Candle
	if err := row.Scan(&c.Timestamp, &c.Open, &c.High, &c.Low, &c.Close, &c.Volume, &c.Symbol, &c.Timeframe, &c.Source); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to scan latest candle in range: %w", err)
	}
	return &c, nil
}

// GetCandle retrieves a single candle by symbol, timeframe, timestamp, and source
func (p *PostgresDB) GetCandle(symbol, timeframe string, timestamp time.Time, source string) (*candle.Candle, error) {
	row := p.db.QueryRow(`
		SELECT timestamp, open, high, low, close, volume, symbol, timeframe, source 
		FROM candles 
		WHERE symbol=$1 AND timeframe=$2 AND timestamp=$3 AND source=$4 
		LIMIT 1`,
		symbol, timeframe, timestamp, source)

	var c candle.Candle
	if err := row.Scan(&c.Timestamp, &c.Open, &c.High, &c.Low, &c.Close, &c.Volume, &c.Symbol, &c.Timeframe, &c.Source); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to scan candle: %w", err)
	}
	return &c, nil
}

// DeleteCandlesInRange removes candles in a specific time range for a symbol and timeframe
func (p *PostgresDB) DeleteCandlesInRange(symbol, timeframe string, start, end time.Time, source string) error {
	query := `DELETE FROM candles WHERE symbol=$1 AND timeframe=$2 AND timestamp >= $3 AND timestamp <= $4`
	args := []any{symbol, timeframe, start, end}

	if source != "" {
		query += " AND source=$5"
		args = append(args, source)
	}

	_, err := p.db.Exec(query, args...)
	if err != nil {
		return fmt.Errorf("failed to delete candles in range: %w", err)
	}
	return nil
}

// SaveCandle saves a single candle to the database
func (p *PostgresDB) SaveCandle(c *candle.Candle) error {
	// Validate candle before saving
	if err := c.Validate(); err != nil {
		return fmt.Errorf("invalid candle for %s %s at %s: %w", c.Symbol, c.Timeframe, c.Timestamp, err)
	}

	_, err := p.db.Exec(`
		INSERT INTO candles (symbol, timeframe, timestamp, open, high, low, close, volume, source)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
		ON CONFLICT (symbol, timeframe, timestamp, source) DO UPDATE SET 
			open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low, 
			close=EXCLUDED.close, volume=EXCLUDED.volume`,
		c.Symbol, c.Timeframe, c.Timestamp, c.Open, c.High, c.Low, c.Close, c.Volume, c.Source)
	if err != nil {
		return fmt.Errorf("failed to save candle for %s %s at %s: %w", c.Symbol, c.Timeframe, c.Timestamp, err)
	}

	return nil
}

// GetCandlesInRange retrieves candles in a specific time range for a symbol and timeframe
func (p *PostgresDB) GetCandlesInRange(symbol, timeframe string, start, end time.Time, source string) ([]candle.Candle, error) {
	query := `
		SELECT timestamp, open, high, low, close, volume, symbol, timeframe, source 
		FROM candles 
		WHERE symbol=$1 AND timeframe=$2 AND timestamp >= $3 AND timestamp <= $4`
	args := []any{symbol, timeframe, start, end}

	if source != "" {
		query += " AND source=$5"
		args = append(args, source)
	}

	query += " ORDER BY timestamp ASC"

	rows, err := p.db.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query candles in range: %w", err)
	}
	defer rows.Close()

	var candles []candle.Candle
	for rows.Next() {
		var c candle.Candle
		if err := rows.Scan(&c.Timestamp, &c.Open, &c.High, &c.Low, &c.Close, &c.Volume, &c.Symbol, &c.Timeframe, &c.Source); err != nil {
			return nil, fmt.Errorf("failed to scan candle: %w", err)
		}
		candles = append(candles, c)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating candle rows: %w", err)
	}

	return candles, nil
}

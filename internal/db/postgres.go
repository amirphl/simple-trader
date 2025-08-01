package db

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/amirphl/simple-trader/internal/candle"
	"github.com/amirphl/simple-trader/internal/db/conf"
	"github.com/amirphl/simple-trader/internal/journal"
	"github.com/amirphl/simple-trader/internal/market"
	"github.com/amirphl/simple-trader/internal/order"
	_ "github.com/lib/pq"
)

// TODO: TX from context
// If exists use it otherwise only create for insert, update, and delete.

type Default struct {
	db *sql.DB
}

func New(c conf.Config) (*Default, error) {
	return &Default{db: c.DB}, nil
}

// SaveCandle saves a single candle to the database
func (p *Default) SaveCandle(ctx context.Context, c *candle.Candle) error {
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

func (p *Default) SaveCandles(ctx context.Context, candles []candle.Candle) error {
	if len(candles) == 0 {
		return nil
	}

	// Validate all candles first
	for i, c := range candles {
		if err := c.Validate(); err != nil {
			return fmt.Errorf("invalid candle at index %d for %s %s at %s: %w",
				i, c.Symbol, c.Timeframe, c.Timestamp, err)
		}
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

	// Build the batch insert query
	query := `INSERT INTO candles (symbol, timeframe, timestamp, open, high, low, close, volume, source)
		VALUES `

	args := []any{}
	paramCount := 0

	for i, c := range candles {
		// Add comma if not the first value
		if i > 0 {
			query += ","
		}

		// Add parameter placeholders for this candle - $1,$2,$3...
		query += fmt.Sprintf("($%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d)",
			paramCount+1, paramCount+2, paramCount+3, paramCount+4,
			paramCount+5, paramCount+6, paramCount+7, paramCount+8, paramCount+9)

		// Add the actual parameters
		args = append(args, c.Symbol, c.Timeframe, c.Timestamp, c.Open, c.High, c.Low, c.Close, c.Volume, c.Source)
		paramCount += 9
	}

	// Add ON CONFLICT clause
	query += ` ON CONFLICT (symbol, timeframe, timestamp, source) DO UPDATE SET 
		open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low, 
		close=EXCLUDED.close, volume=EXCLUDED.volume`

	// Execute the single batch statement
	_, err = tx.Exec(query, args...)
	if err != nil {
		return fmt.Errorf("failed to batch save candles: %w", err)
	}

	return tx.Commit()
}

// SaveConstructedCandles efficiently saves constructed (aggregated) candles
func (p *Default) SaveConstructedCandles(ctx context.Context, candles []candle.Candle) error {
	if len(candles) == 0 {
		return nil
	}

	// Set source to "constructed" for all candles
	for i := range candles {
		candles[i].Source = "constructed"
	}

	return p.SaveCandles(ctx, candles)
}

// SaveRaw1mCandles efficiently saves raw 1m candles with optimized batch processing
// TODO: TX
func (p *Default) SaveRaw1mCandles(ctx context.Context, candles []candle.Candle) error {
	if len(candles) == 0 {
		return nil
	}

	for _, c := range candles {
		if c.Timeframe != "1m" {
			return fmt.Errorf("failed to save candle for %s %s at %s: timeframe is not 1m", c.Symbol, c.Timeframe, c.Timestamp)
		}
	}

	return p.SaveCandles(ctx, candles)
}

// Get1mCandlesForAggregation efficiently retrieves 1m candles for aggregation
func (p *Default) Get1mCandlesForAggregation(ctx context.Context, symbol string, start, end time.Time) ([]candle.Candle, error) {
	rows, err := p.db.Query(`
		SELECT timestamp, open, high, low, close, volume, symbol, timeframe, source 
		FROM candles 
		WHERE symbol=$1 AND timeframe='1m' AND timestamp >= $2 AND timestamp < $3 
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
		c.Timestamp = c.Timestamp.UTC() // TODO:
		candles = append(candles, c)
	}

	return candles, nil
}

// GetCandle retrieves a single candle by symbol, timeframe, timestamp, and source
func (p *Default) GetCandle(ctx context.Context, symbol, timeframe string, timestamp time.Time, source string) (*candle.Candle, error) {
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
	c.Timestamp = c.Timestamp.UTC() // TODO:

	return &c, nil
}

// GetCandlesV2 retrieves candles with a specific timeframe in a time range without filtering by symbol
// This is useful for bulk operations across all symbols
func (p *Default) GetCandlesV2(ctx context.Context, timeframe string, start, end time.Time) ([]candle.Candle, error) {
	query := `
		SELECT timestamp, open, high, low, close, volume, symbol, timeframe, source 
		FROM candles 
		WHERE timeframe=$1 AND timestamp >= $2 AND timestamp < $3 
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
		c.Timestamp = c.Timestamp.UTC() // TODO:
		candles = append(candles, c)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating candle rows: %w", err)
	}

	return candles, nil
}

// GetCandles retrieves candles in a specific time range for a symbol and timeframe and source
func (p *Default) GetCandles(ctx context.Context, symbol, timeframe, source string, start, end time.Time) ([]candle.Candle, error) {
	query := `
		SELECT timestamp, open, high, low, close, volume, symbol, timeframe, source 
		FROM candles 
		WHERE symbol=$1 AND timeframe=$2 AND timestamp >= $3 AND timestamp < $4`
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
		c.Timestamp = c.Timestamp.UTC() // TODO:
		candles = append(candles, c)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating candle rows: %w", err)
	}

	return candles, nil
}

// GetRawCandles retrieves only raw candles (not constructed)
func (p *Default) GetRawCandles(ctx context.Context, symbol, timeframe string, start, end time.Time) ([]candle.Candle, error) {
	rows, err := p.db.Query(`
		SELECT timestamp, open, high, low, close, volume, symbol, timeframe, source 
		FROM candles 
		WHERE symbol=$1 AND timeframe=$2 AND source != 'constructed' AND timestamp >= $3 AND timestamp < $4 
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
		c.Timestamp = c.Timestamp.UTC() // TODO:
		candles = append(candles, c)
	}

	return candles, nil
}

// GetLatestCandle retrieves the latest candle with optimized querying
func (p *Default) GetLatestCandle(ctx context.Context, symbol, timeframe string) (*candle.Candle, error) {
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
	c.Timestamp = c.Timestamp.UTC() // TODO:
	return &c, nil
}

// GetLatestCandleInRange retrieves the latest candle within a specific time range
// This is useful when you need the most recent candle before a certain time
func (p *Default) GetLatestCandleInRange(ctx context.Context, symbol, timeframe string, start, end time.Time) (*candle.Candle, error) {
	row := p.db.QueryRow(`
		SELECT timestamp, open, high, low, close, volume, symbol, timeframe, source 
		FROM candles 
		WHERE symbol=$1 AND timeframe=$2 AND timestamp >= $3 AND timestamp < $4
		ORDER BY timestamp DESC LIMIT 1`,
		symbol, timeframe, start, end)

	var c candle.Candle
	if err := row.Scan(&c.Timestamp, &c.Open, &c.High, &c.Low, &c.Close, &c.Volume, &c.Symbol, &c.Timeframe, &c.Source); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to scan latest candle in range: %w", err)
	}
	c.Timestamp = c.Timestamp.UTC() // TODO:
	return &c, nil
}

// GetLatestConstructedCandle retrieves the latest constructed candle
func (p *Default) GetLatestConstructedCandle(ctx context.Context, symbol, timeframe string) (*candle.Candle, error) {
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
	c.Timestamp = c.Timestamp.UTC() // TODO:
	return &c, nil
}

// DeleteCandle removes a specific candle
// TODO: TX
func (p *Default) DeleteCandle(ctx context.Context, symbol, timeframe, source string, timestamp time.Time) error {
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

// DeleteCandles removes old candles with optimized deletion
func (p *Default) DeleteCandles(ctx context.Context, symbol, timeframe string, before time.Time) error {
	_, err := p.db.Exec(`DELETE FROM candles WHERE symbol=$1 AND timeframe=$2 AND timestamp < $3`,
		symbol, timeframe, before)
	if err != nil {
		return fmt.Errorf("failed to delete candles: %w", err)
	}

	return nil
}

// DeleteCandlesInRange removes candles in a specific time range for a symbol and timeframe
func (p *Default) DeleteCandlesInRange(ctx context.Context, symbol, timeframe, source string, start, end time.Time) error {
	query := `DELETE FROM candles WHERE symbol=$1 AND timeframe=$2 AND timestamp >= $3 AND timestamp < $4`
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

// DeleteConstructedCandles removes old constructed candles
func (p *Default) DeleteConstructedCandles(ctx context.Context, symbol, timeframe string, before time.Time) error {
	// TODO: TX
	_, err := p.db.Exec(`DELETE FROM candles WHERE symbol=$1 AND timeframe=$2 AND source='constructed' AND timestamp < $3`,
		symbol, timeframe, before)
	if err != nil {
		return fmt.Errorf("failed to delete constructed candles: %w", err)
	}

	return nil
}

// GetCandleCount returns the count of candles in a time range
func (p *Default) GetCandleCount(ctx context.Context, symbol, timeframe string, start, end time.Time) (int, error) {
	var count int
	err := p.db.QueryRow(`
		SELECT COUNT(*) FROM candles 
		WHERE symbol=$1 AND timeframe=$2 AND timestamp >= $3 AND timestamp < $4`,
		symbol, timeframe, start, end).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to get candle count: %w", err)
	}
	return count, nil
}

// GetConstructedCandleCount returns the count of constructed candles
func (p *Default) GetConstructedCandleCount(ctx context.Context, symbol, timeframe string, start, end time.Time) (int, error) {
	var count int
	err := p.db.QueryRow(`
		SELECT COUNT(*) FROM candles 
		WHERE symbol=$1 AND timeframe=$2 AND source='constructed' AND timestamp >= $3 AND timestamp < $4`,
		symbol, timeframe, start, end).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to get constructed candle count: %w", err)
	}
	return count, nil
}

// UpdateCandle updates an existing candle
// TODO: TX
func (p *Default) UpdateCandle(ctx context.Context, c candle.Candle) error {
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
func (p *Default) UpdateCandles(ctx context.Context, candles []candle.Candle) error {
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

// GetAggregationStats returns statistics useful for aggregation
func (p *Default) GetAggregationStats(ctx context.Context, symbol string) (map[string]any, error) {
	stats := make(map[string]any)

	// Get latest 1m candle
	latest1m, err := p.GetLatestCandle(ctx, symbol, "1m")
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
		count, err := p.GetCandleCount(ctx, symbol, timeframe, start, end)
		if err != nil {
			continue // Skip if error
		}
		stats[fmt.Sprintf("count_24h_%s", timeframe)] = count
	}

	return stats, nil
}

// GetMissingCandleRanges identifies gaps in 1m candle data
func (p *Default) GetMissingCandleRanges(ctx context.Context, symbol string, start, end time.Time) ([]struct{ Start, End time.Time }, error) {
	// TODO: Logic
	// Query for gaps in 1m candles
	rows, err := p.db.Query(`
		WITH time_series AS (
			SELECT generate_series($1::timestamp, $2::timestamp, '1 minute'::timeframe) as expected_time
		),
		actual_candles AS (
			SELECT timestamp FROM candles 
			WHERE symbol=$3 AND timeframe='1m' AND timestamp >= $1 AND timestamp < $2
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

// GetCandleSourceStats returns statistics about candle sources
func (p *Default) GetCandleSourceStats(ctx context.Context, symbol string, start, end time.Time) (map[string]any, error) {
	stats := make(map[string]any)

	// Get source distribution
	rows, err := p.db.Query(`
		SELECT source, timeframe, COUNT(*) as count
		FROM candles 
		WHERE symbol=$1 AND timestamp >= $2 AND timestamp < $3
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
		WHERE symbol=$1 AND source='constructed' AND timestamp >= $2 AND timestamp < $3`,
		symbol, start, end).Scan(&constructedCount)
	if err != nil {
		constructedCount = 0
	}

	err = p.db.QueryRow(`
		SELECT COUNT(*) FROM candles 
		WHERE symbol=$1 AND source != 'constructed' AND timestamp >= $2 AND timestamp < $3`,
		symbol, start, end).Scan(&rawCount)
	if err != nil {
		rawCount = 0
	}

	stats["constructed_count"] = constructedCount
	stats["raw_count"] = rawCount
	stats["total_count"] = constructedCount + rawCount

	return stats, nil
}

func (p *Default) SaveOrderBook(ctx context.Context, ob market.OrderBook) error {
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

func (p *Default) GetOrderBooks(ctx context.Context, symbol string, start, end time.Time) ([]market.OrderBook, error) {
	rows, err := p.db.Query(`SELECT symbol, timestamp, bids, asks FROM orderbooks WHERE symbol=$1 AND timestamp >= $2 AND timestamp < $3 ORDER BY timestamp ASC`, symbol, start, end)
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
		ob.Timestamp = ob.Timestamp.UTC() // TODO:
		obs = append(obs, ob)
	}
	return obs, nil
}

func (p *Default) DeleteOrderBooks(ctx context.Context, symbol string, before time.Time) error {
	// TODO: TX
	_, err := p.db.Exec(`DELETE FROM orderbooks WHERE symbol=$1 AND timestamp < $2`, symbol, before)
	return err
}

func (p *Default) SaveTick(ctx context.Context, tick market.Tick) error {
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
	_, err = stmt.Exec(tick.Symbol, tick.Price, tick.Quantity, tick.Side, tick.Timestamp)
	if err != nil {
		tx.Rollback()
		return err
	}

	return tx.Commit()
}

// TODO: Batch
func (p *Default) SaveTicks(ctx context.Context, ticks []market.Tick) error {
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

func (p *Default) GetTicks(ctx context.Context, symbol string, start, end time.Time) ([]market.Tick, error) {
	rows, err := p.db.Query(`SELECT symbol, price, quantity, side, timestamp FROM ticks WHERE symbol=$1 AND timestamp >= $2 AND timestamp < $3 ORDER BY timestamp ASC`, symbol, start, end)
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
		t.Timestamp = t.Timestamp.UTC() // TODO:
		ticks = append(ticks, t)
	}
	return ticks, nil
}

func (p *Default) DeleteTicks(ctx context.Context, symbol string, before time.Time) error {
	// TODO: TX
	_, err := p.db.Exec(`DELETE FROM ticks WHERE symbol=$1 AND timestamp < $2`, symbol, before)
	return err
}

func (p *Default) SaveOrder(ctx context.Context, o order.OrderResponse) error {
	// TODO: TX
	_, err := p.db.Exec(`INSERT INTO orders (order_id, symbol, side, type, price, quantity, status, filled_qty, avg_price, created_at, updated_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
		ON CONFLICT (order_id) DO UPDATE SET status=EXCLUDED.status, filled_qty=EXCLUDED.filled_qty, avg_price=EXCLUDED.avg_price, updated_at=EXCLUDED.updated_at`,
		o.OrderID, o.Symbol, o.Side, o.Type, o.Price, o.Quantity, o.Status, o.FilledQty, o.AvgPrice, o.Timestamp, o.Timestamp)
	return err
}

func (p *Default) GetOrder(ctx context.Context, orderID string) (order.OrderResponse, error) {
	row := p.db.QueryRow(`SELECT order_id, symbol, side, type, price, quantity, status, filled_qty, avg_price, created_at, updated_at FROM orders WHERE order_id=$1`, orderID)
	var o order.OrderResponse
	if err := row.Scan(&o.OrderID, &o.Symbol, &o.Side, &o.Type, &o.Price, &o.Quantity, &o.Status, &o.FilledQty, &o.AvgPrice, &o.Timestamp, &o.UpdatedAt); err != nil {
		return o, err
	}
	o.Timestamp = o.Timestamp.UTC() // TODO:
	o.UpdatedAt = o.UpdatedAt.UTC() // TODO:
	return o, nil
}

func (p *Default) GetOpenOrders(ctx context.Context) ([]order.OrderResponse, error) {
	rows, err := p.db.Query(`SELECT order_id, symbol, side, type, price, quantity, status, filled_qty, avg_price, created_at, updated_at FROM orders WHERE status NOT IN ('FILLED', 'CANCELED', 'EXPIRED')`)
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
		o.Timestamp = o.Timestamp.UTC() // TODO:
		o.UpdatedAt = o.UpdatedAt.UTC() // TODO:
		orders = append(orders, o)
	}
	return orders, nil
}

func (p *Default) CloseOrder(ctx context.Context, orderID string) error {
	_, err := p.db.Exec(`UPDATE orders SET status='CLOSED', updated_at=$1 WHERE order_id=$2`, time.Now(), orderID)
	return err
}

func (p *Default) UpdateOrderStatus(ctx context.Context, orderID, status string, filledQty, avgPrice float64, updatedAt time.Time) error {
	// TODO: TX
	_, err := p.db.Exec(`UPDATE orders SET status=$1, filled_qty=$2, avg_price=$3, updated_at=$4 WHERE order_id=$5`, status, filledQty, avgPrice, updatedAt, orderID)
	return err
}

func (p *Default) DeleteOrder(ctx context.Context, orderID string) error {
	// TODO: TX
	_, err := p.db.Exec(`DELETE FROM orders WHERE order_id=$1`, orderID)
	return err
}

func (p *Default) LogEvent(ctx context.Context, event journal.Event) error {
	data, _ := json.Marshal(event.Data)
	// TODO: TX
	_, err := p.db.Exec(`INSERT INTO events (time, type, description, data) VALUES ($1,$2,$3,$4)`, event.Time, event.Type, event.Description, data)
	return err
}

func (p *Default) GetEvents(ctx context.Context, eventType string, start, end time.Time) ([]journal.Event, error) {
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
		e.Time = e.Time.UTC() // TODO:
		events = append(events, e)
	}
	return events, nil
}

func (p *Default) DeleteEvents(ctx context.Context, eventType string, before time.Time) error {
	// TODO: TX
	_, err := p.db.Exec(`DELETE FROM events WHERE type=$1 AND time < $2`, eventType, before)
	return err
}

func (p *Default) SaveState(ctx context.Context, state map[string]any) error {
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

func (p *Default) LoadState(ctx context.Context) (map[string]any, error) {
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

func (p *Default) DeleteState(ctx context.Context, key string) error {
	// TODO: TX
	_, err := p.db.Exec(`DELETE FROM state WHERE key=$1`, key)
	return err
}

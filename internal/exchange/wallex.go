package exchange

import (
	"errors"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/amirphl/simple-trader/internal/candle"
	"github.com/amirphl/simple-trader/internal/market"
	"github.com/amirphl/simple-trader/internal/order"
	wallex "github.com/wallexchange/wallex-go"
)

type WallexExchange struct {
	client *wallex.Client
}

func NewWallexExchange(apiKey string) *WallexExchange {
	return &WallexExchange{
		client: wallex.New(wallex.ClientOptions{APIKey: apiKey}),
	}
}

func (w *WallexExchange) Name() string {
	return "wallex"
}

// retry wraps a function with retry logic for transient errors.
// retry wraps a function with retry logic for transient errors, using exponential backoff and error logging.
func retry(attempts int, sleep time.Duration, fn func() error) error {
	backoff := sleep
	for i := 1; i <= attempts; i++ {
		err := fn()
		if err == nil {
			return nil
		}
		log.Printf("Retry attempt %d/%d failed: %v. Backing off for %v", i, attempts, err, backoff)
		time.Sleep(backoff)
		// Exponential backoff, but cap at 5 minutes
		if backoff < 5*time.Minute {
			backoff *= 2
			if backoff > 5*time.Minute {
				backoff = 5 * time.Minute
			}
		}
	}
	return errors.New("all retry attempts failed")
}

func (w *WallexExchange) FetchCandles(symbol string, timeframe string, start, end int64) ([]candle.Candle, error) {
	// Validate timeframe
	if !candle.IsValidTimeframe(timeframe) {
		return nil, fmt.Errorf("unsupported timeframe: %s", timeframe)
	}

	var wallexCandles []*wallex.Candle
	err := retry(3, 2*time.Second, func() error {
		from := time.Unix(start, 0)
		to := time.Unix(end, 0)
		var err error
		wallexCandles, err = w.client.Candles(symbol, timeframe, from, to)
		if err != nil {
			return fmt.Errorf("fetching candles: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("wallex FetchCandles failed: %w", err)
	}

	var candles []candle.Candle
	for _, wc := range wallexCandles {
		open, _ := strconv.ParseFloat(string(wc.Open), 64)
		high, _ := strconv.ParseFloat(string(wc.High), 64)
		low, _ := strconv.ParseFloat(string(wc.Low), 64)
		close, _ := strconv.ParseFloat(string(wc.Close), 64)
		volume, _ := strconv.ParseFloat(string(wc.Volume), 64)

		c := candle.Candle{
			Timestamp: wc.Timestamp,
			Open:      open,
			High:      high,
			Low:       low,
			Close:     close,
			Volume:    volume,
			Symbol:    symbol,
			Timeframe: timeframe,
			Source:    "wallex",
		}

		// ISSUE: Side effect on strategies
		// Validate candle before adding
		if err := c.Validate(); err != nil {
			continue // Skip invalid candles
		}

		candles = append(candles, c)
	}

	return candles, nil
}

// FetchCandlesWithRetry fetches candles with configurable retry logic
func (w *WallexExchange) FetchCandlesWithRetry(symbol string, timeframe string, start, end int64, maxRetries int, retryDelay time.Duration) ([]candle.Candle, error) {
	var lastErr error
	for attempt := 1; attempt <= maxRetries; attempt++ {
		candles, err := w.FetchCandles(symbol, timeframe, start, end)
		if err == nil {
			return candles, nil
		}

		lastErr = err
		if attempt < maxRetries {
			time.Sleep(retryDelay)
		}
	}

	return nil, fmt.Errorf("failed to fetch candles after %d attempts: %w", maxRetries, lastErr)
}

// FetchLatestCandles fetches the most recent candles for a symbol and timeframe
func (w *WallexExchange) FetchLatestCandles(symbol string, timeframe string, count int) ([]candle.Candle, error) {
	end := time.Now()
	duration := candle.GetTimeframeDuration(timeframe)
	if duration == 0 {
		return nil, fmt.Errorf("invalid timeframe: %s", timeframe)
	}

	// Calculate start time based on count and timeframe
	start := end.Add(-duration * time.Duration(count))

	return w.FetchCandles(symbol, timeframe, start.Unix(), end.Unix())
}

func (w *WallexExchange) FetchOrderBook(symbol string) (market.OrderBook, error) {
	asks, bids, err := w.client.MarketOrders(symbol)
	if err != nil {
		return market.OrderBook{}, err
	}
	var ob market.OrderBook
	ob.Symbol = symbol
	ob.Timestamp = time.Now()
	for _, a := range asks {
		price, _ := strconv.ParseFloat(string(a.Price), 64)
		qty, _ := strconv.ParseFloat(string(a.Quantity), 64)
		ob.Asks = append(ob.Asks, [2]float64{price, qty})
	}
	for _, b := range bids {
		price, _ := strconv.ParseFloat(string(b.Price), 64)
		qty, _ := strconv.ParseFloat(string(b.Quantity), 64)
		ob.Bids = append(ob.Bids, [2]float64{price, qty})
	}
	return ob, nil
}

func (w *WallexExchange) FetchTick(symbol string) (market.Tick, error) {
	// TODO: timerange
	trades, err := w.client.MarketTrades(symbol)
	if err != nil || len(trades) == 0 {
		return market.Tick{}, err
	}
	last := trades[len(trades)-1]
	price, _ := strconv.ParseFloat(string(last.Price), 64)
	qty, _ := strconv.ParseFloat(string(last.Quantity), 64)

	return market.Tick{
		Symbol:    symbol,
		Price:     price,
		Quantity:  qty,
		Side:      "", // Wallex may not provide side; TODO: map if available
		Timestamp: last.Timestamp,
	}, nil
}

func (w *WallexExchange) SubmitOrder(req order.OrderRequest) (order.OrderResponse, error) {
	price := strconv.FormatFloat(req.Price, 'f', 8, 64)
	qty := strconv.FormatFloat(req.Quantity, 'f', 8, 64)

	params := &wallex.OrderParams{
		Symbol:   req.Symbol,
		Type:     req.Type,
		Side:     req.Side,
		Price:    wallex.Number(price),
		Quantity: wallex.Number(qty),
	}
	resp, err := w.client.PlaceOrder(params)
	if err != nil {
		return order.OrderResponse{}, err
	}

	return order.OrderResponse{
		OrderID:   resp.ClientOrderID,
		Status:    resp.Status,
		FilledQty: float64Ptr(resp.ExecutedQty),
		AvgPrice:  float64Ptr(resp.ExecutedPrice),
		Timestamp: resp.CreatedAt,
		Symbol:    resp.Symbol,
		Side:      resp.Side,
		Type:      resp.Type,
		Price:     req.Price,
		Quantity:  req.Quantity,
	}, nil
}

func (w *WallexExchange) CancelOrder(orderID string) error {
	return w.client.CancelOrder(orderID)
}

// Helper to safely dereference *wallex.Number
func float64Ptr(n *wallex.Number) float64 {
	if n == nil {
		return 0
	}
	out, _ := strconv.ParseFloat(string(*n), 64)
	return out
}

func (w *WallexExchange) GetOrderStatus(orderID string) (order.OrderResponse, error) {
	resp, err := w.client.Order(orderID)
	if err != nil {
		return order.OrderResponse{}, err
	}
	return order.OrderResponse{
		OrderID:   resp.ClientOrderID,
		Status:    resp.Status,
		FilledQty: float64Ptr(resp.ExecutedQty),
		AvgPrice:  float64Ptr(resp.ExecutedPrice),
		Timestamp: resp.CreatedAt,
		Symbol:    resp.Symbol,
		Side:      resp.Side,
		Type:      resp.Type,
		Price:     float64Ptr(&resp.Price),
		Quantity:  float64Ptr(&resp.OrigQty),
		UpdatedAt: resp.CreatedAt,
	}, nil
}

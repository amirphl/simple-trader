// Package exchange
//
// WebSocket Implementation Notes:
//   - Instead of broadcasting all ticks to subscribers (which can overwhelm them),
//     we now store only the last tick and provide methods to retrieve it on demand.
//   - Use GetLastTick() or GetLastTickAsTick() to get the most recent tick data.
//   - Use HasFreshTick() to check if there's recent tick data available.
//   - This approach prevents subscribers from being overwhelmed by high-frequency ticks.
package exchange

import (
	"context"
	"encoding/json"
	"log"
	"net/url"
	"strings"
	"sync"
	"time"

	"fmt"

	"github.com/gorilla/websocket"
)

// TradeChannel interface for managing trade subscriptions
type TradeChannel interface {
	Subscribe(subscriberID string, bufferSize int) (<-chan WallexTrade, error)
	Unsubscribe(subscriberID string) error
	GetSubscriberCount() int
	GetLastTick() (*Tick, time.Time)
	HasFreshTick() bool
	Close()
	IsConnected() bool
	Health() error
	Start(ctx context.Context, symbol string)
}

// DepthWatcher interface for market depth monitoring
type DepthWatcher interface {
	IsConnected() bool
	Health() error
	Close()
	Start(ctx context.Context) error
}

// MarketCapWatcher interface for market cap monitoring
type MarketCapWatcher interface {
	IsConnected() bool
	Health() error
	Close()
	Start(ctx context.Context) error
}

// MarketDepthStateManager interface for managing market depth state
type MarketDepthStateManager interface {
	update(symbol, depthType string, data []byte) error
	Get(symbol, depthType string) (*OrderBook, bool)
}

// MarketCapStateManager interface for managing market cap state
type MarketCapStateManager interface {
	update(symbol string, data *MarketCap)
	Get(symbol string) (*MarketCap, bool)
}

// WallexTrade represents a trade message from Wallex
// (fields based on the provided JSON)
type WallexTrade struct {
	IsBuyOrder bool      `json:"isBuyOrder"`
	Quantity   string    `json:"quantity"`
	Price      string    `json:"price"`
	Timestamp  time.Time `json:"timestamp"`
}

// ConnectionState represents the state of the websocket connection
// (for health checks and monitoring)
type ConnectionState int

const (
	Disconnected ConnectionState = iota
	Connecting
	Connected
	Reconnecting
)

// Subscriber represents a single subscriber with their own channel
type Subscriber struct {
	ID   string
	Chan chan WallexTrade
}

// WallexTradeChannel wraps multiple subscriber channels and manages the websocket connection.
type WallexTradeChannel struct {
	subscribers map[string]*Subscriber
	mu          sync.RWMutex
	closed      bool
	state       ConnectionState
	lastPing    time.Time
	lastPong    time.Time
	healthErr   error
	symbol      string
	conn        *websocket.Conn
	cancelFunc  context.CancelFunc

	// Store only the last tick instead of broadcasting all ticks
	lastTick     *WallexTrade
	lastTickTime time.Time
}

// NewWallexTradeChannel creates a new trade channel manager
func NewWallexTradeChannel() TradeChannel {
	return &WallexTradeChannel{
		subscribers: make(map[string]*Subscriber),
		state:       Disconnected,
	}
}

// Subscribe adds a new subscriber and returns their channel
// Note: Since we no longer broadcast all ticks, subscribers should use GetLastTick() to get the latest data
func (w *WallexTradeChannel) Subscribe(subscriberID string, bufferSize int) (<-chan WallexTrade, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return nil, fmt.Errorf("channel is closed")
	}

	if _, exists := w.subscribers[subscriberID]; exists {
		return nil, fmt.Errorf("subscriber %s already exists", subscriberID)
	}

	sub := &Subscriber{
		ID:   subscriberID,
		Chan: make(chan WallexTrade, bufferSize),
	}
	w.subscribers[subscriberID] = sub

	log.Printf("WallexWebsocket | Subscriber %s added for symbol %s (use GetLastTick() to get latest data)", subscriberID, w.symbol)
	return sub.Chan, nil
}

// Unsubscribe removes a subscriber and closes their channel
func (w *WallexTradeChannel) Unsubscribe(subscriberID string) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	sub, exists := w.subscribers[subscriberID]
	if !exists {
		return fmt.Errorf("subscriber %s not found", subscriberID)
	}

	close(sub.Chan)
	delete(w.subscribers, subscriberID)
	log.Printf("WallexWebsocket | Subscriber %s removed for symbol %s", subscriberID, w.symbol)
	return nil
}

// updateLastTick stores the latest tick instead of broadcasting to all subscribers
func (w *WallexTradeChannel) updateLastTick(trade WallexTrade) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.lastTick = &trade
	w.lastTickTime = time.Now()
}

// GetLastTick returns the most recent tick data
func (w *WallexTradeChannel) GetLastTick() (*Tick, time.Time) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	if w.lastTick == nil {
		return nil, time.Time{}
	}

	// Convert to Tick format
	tick := w.lastTick.ToTick(w.symbol)
	return &tick, w.lastTickTime
}

// HasFreshTick returns true if there's tick data available and it's recent (within last 1 seconds)
func (w *WallexTradeChannel) HasFreshTick() bool {
	w.mu.RLock()
	defer w.mu.RUnlock()

	if w.lastTick == nil {
		return false
	}

	// Consider tick fresh if it's within the last 1 seconds
	return time.Since(w.lastTickTime) < 1*time.Second
}

// broadcast sends a trade to all subscribers (non-blocking)
// This function is kept for backward compatibility but is no longer used
func (w *WallexTradeChannel) broadcast(trade WallexTrade) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	for _, sub := range w.subscribers {
		select {
		case sub.Chan <- trade:
			// Successfully sent
		default:
			// Channel is full, skip this subscriber (don't close the channel)
			log.Printf("WallexWebsocket | Subscriber %s channel is full, skipping trade", sub.ID)
		}
	}

	// Note: We don't remove subscribers here anymore
	// Subscribers should be explicitly unsubscribed when they're done
	// or we can add a separate health check mechanism if needed
}

// GetSubscriberCount returns the number of active subscribers
func (w *WallexTradeChannel) GetSubscriberCount() int {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return len(w.subscribers)
}

// Close closes the channel and all subscriber channels
func (w *WallexTradeChannel) Close() {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return
	}

	w.closed = true

	// Cancel the context to stop the websocket connection
	if w.cancelFunc != nil {
		w.cancelFunc()
	}

	// Close all subscriber channels
	for _, sub := range w.subscribers {
		close(sub.Chan)
	}
	w.subscribers = make(map[string]*Subscriber)

	// Close the websocket connection
	if w.conn != nil {
		w.conn.Close()
	}

	log.Printf("WallexWebsocket | Trade channel closed for symbol %s", w.symbol)
}

// IsConnected returns true if the websocket is connected
func (w *WallexTradeChannel) IsConnected() bool {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.state == Connected && w.conn != nil
}

// Health returns the last health error (if any)
func (w *WallexTradeChannel) Health() error {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.healthErr
}

// setLastPing updates the last ping time
func (w *WallexTradeChannel) setLastPing(t time.Time) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.lastPing = t
}

// setLastPong updates the last pong time
func (w *WallexTradeChannel) setLastPong(t time.Time) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.lastPong = t
}

// NormalizeSymbol converts e.g. btc-usdt to BTCUSDT for Wallex API
func NormalizeSymbol(symbol string) string {
	return strings.ToUpper(strings.ReplaceAll(symbol, "-", ""))
}

func NormalizedTimeframe(timeframe string) string {
	trimmedTimeframe := strings.TrimSuffix(timeframe, "m")
	return trimmedTimeframe
}

// ExtractQuoteCurrency extracts the quote currency from a trading symbol
// e.g., "ETH/DAI" -> "DAI", "BTC/USDT" -> "USDT"
func ExtractQuoteCurrency(symbol string) string {
	parts := strings.Split(symbol, "/")
	if len(parts) != 2 {
		parts = strings.Split(symbol, "-")
		if len(parts) != 2 {
			return ""
		}
	}
	return parts[len(parts)-1]
}

// GetBuyDepthKey returns the normalized key for buy depth data
func GetBuyDepthKey(symbol string) string {
	return fmt.Sprintf("%s@buyDepth", NormalizeSymbol(symbol))
}

// GetSellDepthKey returns the normalized key for sell depth data
func GetSellDepthKey(symbol string) string {
	return fmt.Sprintf("%s@sellDepth", NormalizeSymbol(symbol))
}

// Start connects to Wallex websocket and streams trades to all subscribers, with reconnect and health check
func (w *WallexTradeChannel) Start(ctx context.Context, symbol string) {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return
	}
	w.symbol = NormalizeSymbol(symbol)
	w.state = Connecting
	w.mu.Unlock()

	ctx, cancel := context.WithCancel(ctx)
	w.cancelFunc = cancel

	go func() {
		defer w.Close()
		retryDelay := time.Second
		for {
			select {
			case <-ctx.Done():
				return
			default:
				if err := w.connectAndStream(ctx); err != nil {
					w.mu.Lock()
					w.state = Reconnecting
					w.healthErr = err
					w.mu.Unlock()
					log.Printf("WallexWebsocket | Disconnected, retrying in %v: %v", retryDelay, err)
					time.Sleep(retryDelay)
					if retryDelay < 60*time.Second {
						retryDelay *= 2
					} else {
						retryDelay = 60 * time.Second
					}
					continue
				}
				// If connectAndStream returns nil, exit
				return
			}
		}
	}()
}

// SubscribeMessage is used to subscribe to a channel via Socket.IO
// e.g. {"channel": "USDTTMN@trade"}
type SubscribeMessage struct {
	Channel string `json:"channel"`
}

// BroadcasterMessage is the message format for channel data
// e.g. {"channel": "USDTTMN@trade", "data": {...}}
type BroadcasterMessage struct {
	Channel string      `json:"channel"`
	Data    interface{} `json:"data"`
}

// connectAndStream handles a single websocket connection session
func (w *WallexTradeChannel) connectAndStream(ctx context.Context) error {
	w.mu.Lock()
	w.state = Connecting
	w.healthErr = nil
	w.mu.Unlock()

	u := url.URL{Scheme: "wss", Host: "api.wallex.ir", Path: "/socket.io/"}
	// Add Socket.IO query parameters
	query := u.Query()
	query.Set("EIO", "4")
	query.Set("transport", "websocket")
	u.RawQuery = query.Encode()

	c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		return err
	}
	w.mu.Lock()
	w.conn = c
	w.state = Connected
	w.lastPing = time.Now()
	w.lastPong = time.Now()
	w.mu.Unlock()

	log.Printf("WallexWebsocket | Connection established for symbol %s", w.symbol)
	defer func() {
		c.Close()
		w.mu.Lock()
		w.conn = nil
		w.state = Disconnected
		w.mu.Unlock()
	}()

	// Send Socket.IO connect message ("40")
	if err := c.WriteMessage(websocket.TextMessage, []byte("40")); err != nil {
		return err
	}

	// Subscribe to the channel using Socket.IO event format: 42["subscribe",{...}]
	subscribeMsg := SubscribeMessage{Channel: w.symbol + "@trade"}
	subscribeJSON, err := json.Marshal(subscribeMsg)
	if err != nil {
		return err
	}
	socketIOMsg := fmt.Sprintf(`42["subscribe",%s]`, string(subscribeJSON))
	if err := c.WriteMessage(websocket.TextMessage, []byte(socketIOMsg)); err != nil {
		return err
	}

	log.Printf("WallexWebsocket | Subscribed to %s@trade channel", w.symbol)

	c.SetPongHandler(func(appData string) error {
		w.setLastPong(time.Now())
		return nil
	})

	pingTicker := time.NewTicker(20 * time.Second)
	defer pingTicker.Stop()

	handshakeComplete := false

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-pingTicker.C:
			w.mu.Lock()
			if w.conn != nil {
				w.conn.WriteMessage(websocket.PingMessage, nil)
				w.setLastPing(time.Now())
			}
			w.mu.Unlock()
		default:
			c.SetReadDeadline(time.Now().Add(30 * time.Second))
			_, message, err := c.ReadMessage()
			if err != nil {
				return err
			}
			msgStr := string(message)
			if msgStr == "2" {
				// Socket.IO ping, respond with pong
				c.WriteMessage(websocket.TextMessage, []byte("3"))
				continue
			}
			if msgStr == "40" && !handshakeComplete {
				// Handshake complete, now subscribe
				handshakeComplete = true
				subscribeMsg := SubscribeMessage{Channel: w.symbol + "@trade"}
				subscribeJSON, err := json.Marshal(subscribeMsg)
				if err != nil {
					return err
				}
				socketIOMsg := fmt.Sprintf(`42["subscribe",%s]`, string(subscribeJSON))
				if err := c.WriteMessage(websocket.TextMessage, []byte(socketIOMsg)); err != nil {
					return err
				}
				log.Printf("WallexWebsocket | Subscribed to %s@trade channel", w.symbol)
				continue
			}
			// Handle Socket.IO event message (starts with "42")
			if len(msgStr) >= 2 && msgStr[:2] == "42" {
				jsonPart := msgStr[2:]
				var eventArray []interface{}
				if err := json.Unmarshal([]byte(jsonPart), &eventArray); err != nil {
					continue
				}
				if len(eventArray) >= 3 {
					eventName, ok := eventArray[0].(string)
					if !ok || eventName != "Broadcaster" {
						continue
					}
					channel, ok := eventArray[1].(string)
					if !ok || channel != w.symbol+"@trade" {
						continue
					}
					data := eventArray[2]
					dataJSON, err := json.Marshal(data)
					if err != nil {
						continue
					}
					var trade WallexTrade
					if err := json.Unmarshal(dataJSON, &trade); err != nil {
						continue
					}
					// Broadcast to all subscribers
					// w.broadcast(trade)
					// Store the latest tick instead of broadcasting to all subscribers
					w.updateLastTick(trade)
				}
			}
		}
	}
}

// OrderBookEntry represents a single order book entry
type OrderBookEntry struct {
	Quantity float64 `json:"quantity"`
	Price    string  `json:"price"`
	Sum      float64 `json:"sum"`
}

// UnmarshalJSON custom unmarshaler to handle price as both string and number
func (o *OrderBookEntry) UnmarshalJSON(data []byte) error {
	// Create a temporary struct to unmarshal into
	type tempOrderBookEntry struct {
		Quantity float64     `json:"quantity"`
		Price    interface{} `json:"price"` // Use interface{} to handle both string and number
		Sum      float64     `json:"sum"`
	}

	var temp tempOrderBookEntry
	if err := json.Unmarshal(data, &temp); err != nil {
		return err
	}

	// Copy quantity and sum
	o.Quantity = temp.Quantity
	o.Sum = temp.Sum

	// Handle price conversion
	switch v := temp.Price.(type) {
	case string:
		o.Price = v
	case float64:
		o.Price = fmt.Sprintf("%.8f", v) // Convert to string with precision
	case int:
		o.Price = fmt.Sprintf("%d", v)
	case int64:
		o.Price = fmt.Sprintf("%d", v)
	default:
		o.Price = fmt.Sprintf("%v", v) // Fallback for any other type
	}

	return nil
}

// OrderBook represents the complete order book (buy or sell depth)
type OrderBook map[string]OrderBookEntry

// MarketDepthState holds the latest order book state for a symbol/depth type.
type MarketDepthState struct {
	mu    sync.RWMutex
	state map[string]*OrderBook // key: GetBuyDepthKey(symbol) or GetSellDepthKey(symbol)
}

// NewMarketDepthState creates a new MarketDepthState.
func NewMarketDepthState() MarketDepthStateManager {
	return &MarketDepthState{
		state: make(map[string]*OrderBook),
	}
}

// Update sets the latest state for a symbol and depth type.
func (m *MarketDepthState) update(symbol, depthType string, data []byte) error {
	key := fmt.Sprintf("%s@%s", NormalizeSymbol(symbol), depthType)

	var orderBook OrderBook
	if err := json.Unmarshal(data, &orderBook); err != nil {
		return fmt.Errorf("failed to unmarshal order book for %s: %w", key, err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	m.state[key] = &orderBook
	return nil
}

// Get returns the latest state for a symbol and depth type.
func (m *MarketDepthState) Get(symbol, depthType string) (*OrderBook, bool) {
	key := fmt.Sprintf("%s@%s", NormalizeSymbol(symbol), depthType)
	m.mu.RLock()
	defer m.mu.RUnlock()
	data, ok := m.state[key]
	return data, ok
}

// WallexDepthWatcher connects to Wallex and updates MarketDepthState for a symbol/depthType.
type WallexDepthWatcher struct {
	conn      *websocket.Conn
	cancel    context.CancelFunc
	state     MarketDepthStateManager
	symbol    string
	depthType string // "buyDepth" or "sellDepth"

	mu        sync.RWMutex
	closed    bool
	healthErr error
	connState ConnectionState
	lastPing  time.Time
	lastPong  time.Time
}

func NewWallexDepthWatcher(state MarketDepthStateManager, symbol, depthType string) DepthWatcher {
	return &WallexDepthWatcher{
		state:     state,
		symbol:    symbol,
		depthType: depthType,
		connState: Disconnected,
	}
}

// IsConnected returns true if the websocket is currently connected
func (w *WallexDepthWatcher) IsConnected() bool {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.connState == Connected
}

// Health returns the last health error (if any)
func (w *WallexDepthWatcher) Health() error {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.healthErr
}

// Close closes the websocket connection and cancels the context
func (w *WallexDepthWatcher) Close() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.closed {
		if w.conn != nil {
			w.conn.Close()
		}
		if w.cancel != nil {
			w.cancel()
		}
		w.closed = true
		w.connState = Disconnected
		log.Printf("WallexDepthWatcher | Closed connection for %s@%s", w.symbol, w.depthType)
	}
}

func (w *WallexDepthWatcher) Start(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	w.cancel = cancel

	go w.run(ctx)
	return nil
}

func (w *WallexDepthWatcher) run(ctx context.Context) {
	defer w.setClosed()
	retryDelay := time.Second
	for {
		select {
		case <-ctx.Done():
			w.logState("Context cancelled, stopping depth watcher")
			return
		default:
			if err := w.connectAndStream(ctx); err != nil {
				w.setHealthErr(err)
				w.setConnState(Reconnecting)
				w.logState("Disconnected, retrying in %v: %v", retryDelay, err)
				time.Sleep(retryDelay)
				if retryDelay < 60*time.Second {
					retryDelay *= 2
				} else {
					retryDelay = 60 * time.Second
				}
				continue
			}
			// If connectAndStream returns nil, exit
			return
		}
	}
}

func (w *WallexDepthWatcher) connectAndStream(ctx context.Context) error {
	w.setConnState(Connecting)
	w.setHealthErr(nil)

	u := url.URL{Scheme: "wss", Host: "api.wallex.ir", Path: "/socket.io/"}
	query := u.Query()
	query.Set("EIO", "4")
	query.Set("transport", "websocket")
	u.RawQuery = query.Encode()

	c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		return err
	}
	w.setConn(c)
	w.setConnState(Connected)
	w.setLastPing(time.Now())
	w.setLastPong(time.Now())
	w.logState("Connection established for %s@%s", w.symbol, w.depthType)
	defer func() {
		c.Close()
		w.setConn(nil)
		w.setConnState(Disconnected)
	}()

	// Socket.IO handshake
	c.WriteMessage(websocket.TextMessage, []byte("40"))

	// Subscribe to depth channel
	channelName := fmt.Sprintf("%s@%s", NormalizeSymbol(w.symbol), w.depthType)
	subscribeMsg := map[string]string{"channel": channelName}
	subscribeJSON, _ := json.Marshal(subscribeMsg)
	socketIOMsg := fmt.Sprintf(`42["subscribe",%s]`, string(subscribeJSON))
	c.WriteMessage(websocket.TextMessage, []byte(socketIOMsg))

	pingTicker := time.NewTicker(20 * time.Second)
	defer pingTicker.Stop()

	handshakeComplete := false

	// Set up pong handler
	c.SetPongHandler(func(appData string) error {
		w.setLastPong(time.Now())
		return nil
	})

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-pingTicker.C:
			w.mu.RLock()
			if w.conn != nil {
				w.conn.WriteMessage(websocket.PingMessage, nil)
				w.setLastPing(time.Now())
			}
			w.mu.RUnlock()
		default:
			c.SetReadDeadline(time.Now().Add(30 * time.Second))
			_, message, err := c.ReadMessage()
			if err != nil {
				return err
			}
			msgStr := string(message)
			if msgStr == "2" {
				c.WriteMessage(websocket.TextMessage, []byte("3"))
				continue
			}
			if msgStr == "40" && !handshakeComplete {
				handshakeComplete = true
				subscribeMsg := map[string]string{"channel": channelName}
				subscribeJSON, _ := json.Marshal(subscribeMsg)
				socketIOMsg := fmt.Sprintf(`42["subscribe",%s]`, string(subscribeJSON))
				c.WriteMessage(websocket.TextMessage, []byte(socketIOMsg))
				w.logState("Resubscribed to %s", channelName)
				continue
			}
			if len(msgStr) >= 2 && msgStr[:2] == "42" {
				jsonPart := msgStr[2:]
				var eventArray []interface{}
				if err := json.Unmarshal([]byte(jsonPart), &eventArray); err != nil {
					continue
				}
				if len(eventArray) >= 3 {
					eventName, _ := eventArray[0].(string)
					channel, _ := eventArray[1].(string)
					if eventName == "Broadcaster" && channel == channelName {
						dataJSON, _ := json.Marshal(eventArray[2])
						if err := w.state.update(w.symbol, w.depthType, dataJSON); err != nil {
							w.logState("Failed to update order book data: %v", err)
							continue
						}
						// w.logState("Updated order book data for %s@%s", w.symbol, w.depthType)
					}
				}
			}
		}
	}
}

func (w *WallexDepthWatcher) setConn(c *websocket.Conn) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.conn = c
}

func (w *WallexDepthWatcher) setConnState(state ConnectionState) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.connState = state
}

func (w *WallexDepthWatcher) setHealthErr(err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.healthErr = err
}

func (w *WallexDepthWatcher) setLastPing(t time.Time) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.lastPing = t
}

func (w *WallexDepthWatcher) setLastPong(t time.Time) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.lastPong = t
}

func (w *WallexDepthWatcher) setClosed() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.closed = true
}

func (w *WallexDepthWatcher) logState(format string, args ...interface{}) {
	log.Printf("WallexDepthWatcher | "+format, args...)
}

// MarketCap represents the market cap data from Wallex
type MarketCap struct {
	Symbol         string  `json:"symbol"`
	Ch24h          float64 `json:"24h_ch"`
	Ch7d           float64 `json:"7d_ch"`
	Volume24h      string  `json:"24h_volume"`
	Volume7d       string  `json:"7d_volume"`
	QuoteVolume24h string  `json:"24h_quoteVolume"`
	HighPrice24h   string  `json:"24h_highPrice"`
	LowPrice24h    string  `json:"24h_lowPrice"`
	LastPrice      string  `json:"lastPrice"`
	LastQty        string  `json:"lastQty"`
	BidPrice       string  `json:"bidPrice"`
	AskPrice       string  `json:"askPrice"`
	LastTradeSide  string  `json:"lastTradeSide"`
	BidVolume      string  `json:"bidVolume"`
	AskVolume      string  `json:"askVolume"`
	BidCount       int     `json:"bidCount"`
	AskCount       int     `json:"askCount"`
	Direction      struct {
		SELL int `json:"SELL"`
		BUY  int `json:"BUY"`
	} `json:"direction"`
	CreatedAt string `json:"createdAt"`
}

// UnmarshalJSON custom unmarshaler to handle string fields that might come as numbers
func (m *MarketCap) UnmarshalJSON(data []byte) error {
	// Create a temporary struct to unmarshal into
	type tempMarketCapData struct {
		Symbol         string      `json:"symbol"`
		Ch24h          float64     `json:"24h_ch"`
		Ch7d           float64     `json:"7d_ch"`
		Volume24h      interface{} `json:"24h_volume"`
		Volume7d       interface{} `json:"7d_volume"`
		QuoteVolume24h interface{} `json:"24h_quoteVolume"`
		HighPrice24h   interface{} `json:"24h_highPrice"`
		LowPrice24h    interface{} `json:"24h_lowPrice"`
		LastPrice      interface{} `json:"lastPrice"`
		LastQty        interface{} `json:"lastQty"`
		BidPrice       interface{} `json:"bidPrice"`
		AskPrice       interface{} `json:"askPrice"`
		LastTradeSide  string      `json:"lastTradeSide"`
		BidVolume      interface{} `json:"bidVolume"`
		AskVolume      interface{} `json:"askVolume"`
		BidCount       int         `json:"bidCount"`
		AskCount       int         `json:"askCount"`
		Direction      struct {
			SELL int `json:"SELL"`
			BUY  int `json:"BUY"`
		} `json:"direction"`
		CreatedAt string `json:"createdAt"`
	}

	var temp tempMarketCapData
	if err := json.Unmarshal(data, &temp); err != nil {
		return err
	}

	// Copy non-string fields
	m.Symbol = temp.Symbol
	m.Ch24h = temp.Ch24h
	m.Ch7d = temp.Ch7d
	m.LastTradeSide = temp.LastTradeSide
	m.BidCount = temp.BidCount
	m.AskCount = temp.AskCount
	m.Direction = temp.Direction
	m.CreatedAt = temp.CreatedAt

	// Convert interface{} fields to string
	m.Volume24h = convertToString(temp.Volume24h)
	m.Volume7d = convertToString(temp.Volume7d)
	m.QuoteVolume24h = convertToString(temp.QuoteVolume24h)
	m.HighPrice24h = convertToString(temp.HighPrice24h)
	m.LowPrice24h = convertToString(temp.LowPrice24h)
	m.LastPrice = convertToString(temp.LastPrice)
	m.LastQty = convertToString(temp.LastQty)
	m.BidPrice = convertToString(temp.BidPrice)
	m.AskPrice = convertToString(temp.AskPrice)
	m.BidVolume = convertToString(temp.BidVolume)
	m.AskVolume = convertToString(temp.AskVolume)

	return nil
}

// convertToString converts various types to string
func convertToString(v interface{}) string {
	if v == nil {
		return ""
	}

	switch val := v.(type) {
	case string:
		return val
	case float64:
		return fmt.Sprintf("%.8f", val)
	case int:
		return fmt.Sprintf("%d", val)
	case int64:
		return fmt.Sprintf("%d", val)
	default:
		return fmt.Sprintf("%v", val)
	}
}

// MarketCapState holds the latest market cap data for a symbol
type MarketCapState struct {
	mu    sync.RWMutex
	state map[string]*MarketCap // key: "SYMBOL"
}

// NewMarketCapState creates a new MarketCapState
func NewMarketCapState() MarketCapStateManager {
	return &MarketCapState{
		state: make(map[string]*MarketCap),
	}
}

// Update sets the latest market cap data for a symbol
func (m *MarketCapState) update(symbol string, data *MarketCap) {
	key := NormalizeSymbol(symbol)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.state[key] = data
}

// Get returns the latest market cap data for a symbol
func (m *MarketCapState) Get(symbol string) (*MarketCap, bool) {
	key := NormalizeSymbol(symbol)
	m.mu.RLock()
	defer m.mu.RUnlock()
	data, ok := m.state[key]
	return data, ok
}

// WallexMarketCapWatcher connects to Wallex and updates MarketCapState for a symbol
type WallexMarketCapWatcher struct {
	conn   *websocket.Conn
	cancel context.CancelFunc
	state  MarketCapStateManager
	symbol string

	mu        sync.RWMutex
	closed    bool
	healthErr error
	connState ConnectionState
	lastPing  time.Time
	lastPong  time.Time
}

func NewWallexMarketCapWatcher(state MarketCapStateManager, symbol string) MarketCapWatcher {
	return &WallexMarketCapWatcher{
		state:     state,
		symbol:    symbol,
		connState: Disconnected,
	}
}

// IsConnected returns true if the websocket is currently connected
func (w *WallexMarketCapWatcher) IsConnected() bool {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.connState == Connected
}

// Health returns the last health error (if any)
func (w *WallexMarketCapWatcher) Health() error {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.healthErr
}

// Close closes the websocket connection and cancels the context
func (w *WallexMarketCapWatcher) Close() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.closed {
		if w.conn != nil {
			w.conn.Close()
		}
		if w.cancel != nil {
			w.cancel()
		}
		w.closed = true
		w.connState = Disconnected
		log.Printf("WallexMarketCapWatcher | Closed connection for %s@marketCap", w.symbol)
	}
}

func (w *WallexMarketCapWatcher) Start(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	w.cancel = cancel

	go w.run(ctx)
	return nil
}

func (w *WallexMarketCapWatcher) run(ctx context.Context) {
	defer w.setClosed()
	retryDelay := time.Second
	for {
		select {
		case <-ctx.Done():
			w.logState("Context cancelled, stopping market cap watcher")
			return
		default:
			if err := w.connectAndStream(ctx); err != nil {
				w.setHealthErr(err)
				w.setConnState(Reconnecting)
				w.logState("Disconnected, retrying in %v: %v", retryDelay, err)
				time.Sleep(retryDelay)
				if retryDelay < 60*time.Second {
					retryDelay *= 2
				} else {
					retryDelay = 60 * time.Second
				}
				continue
			}
			// If connectAndStream returns nil, exit
			return
		}
	}
}

func (w *WallexMarketCapWatcher) connectAndStream(ctx context.Context) error {
	w.setConnState(Connecting)
	w.setHealthErr(nil)

	u := url.URL{Scheme: "wss", Host: "api.wallex.ir", Path: "/socket.io/"}
	query := u.Query()
	query.Set("EIO", "4")
	query.Set("transport", "websocket")
	u.RawQuery = query.Encode()

	c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		return err
	}
	w.setConn(c)
	w.setConnState(Connected)
	w.setLastPing(time.Now())
	w.setLastPong(time.Now())
	w.logState("Connection established for %s@marketCap", w.symbol)
	defer func() {
		c.Close()
		w.setConn(nil)
		w.setConnState(Disconnected)
	}()

	// Socket.IO handshake
	c.WriteMessage(websocket.TextMessage, []byte("40"))

	// Subscribe to market cap channel
	channelName := fmt.Sprintf("%s@marketCap", NormalizeSymbol(w.symbol))
	subscribeMsg := map[string]string{"channel": channelName}
	subscribeJSON, _ := json.Marshal(subscribeMsg)
	socketIOMsg := fmt.Sprintf(`42["subscribe",%s]`, string(subscribeJSON))
	c.WriteMessage(websocket.TextMessage, []byte(socketIOMsg))

	pingTicker := time.NewTicker(20 * time.Second)
	defer pingTicker.Stop()

	handshakeComplete := false

	// Set up pong handler
	c.SetPongHandler(func(appData string) error {
		w.setLastPong(time.Now())
		return nil
	})

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-pingTicker.C:
			w.mu.RLock()
			if w.conn != nil {
				w.conn.WriteMessage(websocket.PingMessage, nil)
				w.setLastPing(time.Now())
			}
			w.mu.RUnlock()
		default:
			c.SetReadDeadline(time.Now().Add(30 * time.Second))
			_, message, err := c.ReadMessage()
			if err != nil {
				return err
			}
			msgStr := string(message)
			if msgStr == "2" {
				c.WriteMessage(websocket.TextMessage, []byte("3"))
				continue
			}
			if msgStr == "40" && !handshakeComplete {
				handshakeComplete = true
				subscribeMsg := map[string]string{"channel": channelName}
				subscribeJSON, _ := json.Marshal(subscribeMsg)
				socketIOMsg := fmt.Sprintf(`42["subscribe",%s]`, string(subscribeJSON))
				c.WriteMessage(websocket.TextMessage, []byte(socketIOMsg))
				w.logState("Resubscribed to %s", channelName)
				continue
			}
			if len(msgStr) >= 2 && msgStr[:2] == "42" {
				jsonPart := msgStr[2:]
				var eventArray []interface{}
				if err := json.Unmarshal([]byte(jsonPart), &eventArray); err != nil {
					continue
				}
				if len(eventArray) >= 3 {
					eventName, _ := eventArray[0].(string)
					channel, _ := eventArray[1].(string)
					if eventName == "Broadcaster" && channel == channelName {
						// eventArray[2] is the market cap data
						dataJSON, _ := json.Marshal(eventArray[2])
						var marketCapData MarketCap
						if err := json.Unmarshal(dataJSON, &marketCapData); err != nil {
							w.logState("Failed to parse market cap data: %v", err)
							continue
						}
						w.state.update(w.symbol, &marketCapData)
						w.logState("Updated market cap data for %s: LastPrice=%s, 24h_volume=%s",
							w.symbol, marketCapData.LastPrice, marketCapData.Volume24h)
					}
				}
			}
		}
	}
}

func (w *WallexMarketCapWatcher) setConn(c *websocket.Conn) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.conn = c
}

func (w *WallexMarketCapWatcher) setConnState(state ConnectionState) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.connState = state
}

func (w *WallexMarketCapWatcher) setHealthErr(err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.healthErr = err
}

func (w *WallexMarketCapWatcher) setLastPing(t time.Time) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.lastPing = t
}

func (w *WallexMarketCapWatcher) setLastPong(t time.Time) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.lastPong = t
}

func (w *WallexMarketCapWatcher) setClosed() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.closed = true
}

func (w *WallexMarketCapWatcher) logState(format string, args ...interface{}) {
	log.Printf("WallexMarketCapWatcher | "+format, args...)
}

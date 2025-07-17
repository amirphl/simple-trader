// Package exchange
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
}

// NewWallexTradeChannel creates a new trade channel manager
func NewWallexTradeChannel() *WallexTradeChannel {
	return &WallexTradeChannel{
		subscribers: make(map[string]*Subscriber),
		state:       Disconnected,
	}
}

// Subscribe adds a new subscriber and returns their channel
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

	log.Printf("WallexWebsocket | Subscriber %s added for symbol %s", subscriberID, w.symbol)
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

// broadcast sends a trade to all active subscribers
func (w *WallexTradeChannel) broadcast(trade WallexTrade) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	for id, sub := range w.subscribers {
		select {
		case sub.Chan <- trade:
			// Successfully sent
		default:
			// Channel is full, skip this trade for this subscriber
			// Don't remove the subscriber - they might be slow but still active
			log.Printf("WallexWebsocket | Channel full for subscriber %s, skipping trade", id)
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

// Close closes all subscriber channels and the websocket connection
func (w *WallexTradeChannel) Close() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.closed {
		// Close all subscriber channels
		for id, sub := range w.subscribers {
			close(sub.Chan)
			log.Printf("WallexWebsocket | Closed subscriber %s channel for symbol %s", id, w.symbol)
		}
		w.subscribers = make(map[string]*Subscriber)

		if w.conn != nil {
			w.conn.Close()
		}
		if w.cancelFunc != nil {
			w.cancelFunc()
		}
		w.closed = true
		w.state = Disconnected
	}
}

// IsConnected returns true if the websocket is currently connected
func (w *WallexTradeChannel) IsConnected() bool {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.state == Connected
}

// Health returns the last health error (if any)
func (w *WallexTradeChannel) Health() error {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.healthErr
}

// normalizeSymbol converts e.g. btc-usdt to BTCUSDT for Wallex API
func normalizeSymbol(symbol string) string {
	s := symbol
	s = strings.ReplaceAll(s, "-", "")
	s = strings.ToUpper(s)
	return s
}

// Start connects to Wallex websocket and streams trades to all subscribers, with reconnect and health check
func (w *WallexTradeChannel) Start(ctx context.Context, symbol string) {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return
	}
	w.symbol = normalizeSymbol(symbol)
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
		w.mu.Lock()
		w.lastPong = time.Now()
		w.mu.Unlock()
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
				w.lastPing = time.Now()
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
					w.broadcast(trade)
				}
			}
		}
	}
}

type WebsocketManager interface {
	Connect() error
	Disconnect() error
	Subscribe(channel string, params map[string]any) error
	Unsubscribe(channel string) error
	IsConnected() bool
	// Add more as needed for state management
}

type WallexWebsocketManager struct {
	// TODO: Add fields for connection, state, etc.
}

func (w *WallexWebsocketManager) Connect() error {
	// TODO: Implement connection logic
	return nil
}

func (w *WallexWebsocketManager) Disconnect() error {
	// TODO: Implement disconnect logic
	return nil
}

func (w *WallexWebsocketManager) Subscribe(channel string, params map[string]any) error {
	// TODO: Implement subscribe logic
	return nil
}

func (w *WallexWebsocketManager) Unsubscribe(channel string) error {
	// TODO: Implement unsubscribe logic
	return nil
}

func (w *WallexWebsocketManager) IsConnected() bool {
	// TODO: Implement connection state check
	return false
}

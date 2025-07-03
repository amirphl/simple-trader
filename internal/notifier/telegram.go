// Package notifier
package notifier

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/amirphl/simple-trader/internal/utils"
)

type TelegramNotifier struct {
	Token       string
	ChatID      string
	MaxAttempts int
	Delay       time.Duration
	ProxyURL    string // Add proxy URL field
	ParseMode   string // Add parse mode field (HTML, Markdown, etc.)

	mu        sync.Mutex
	queue     []string
	started   bool
	lastFlush time.Time
}

// Updated constructor to accept proxy URL and parse mode
func NewTelegramNotifier(token, chatID, proxyURL string, maxAttempts int, delay time.Duration) Notifier {
	t := &TelegramNotifier{
		Token:       token,
		ChatID:      chatID,
		ProxyURL:    proxyURL,
		MaxAttempts: maxAttempts,
		Delay:       delay,
		ParseMode:   "HTML", // Default to HTML parse mode
		queue:       make([]string, 0, 64),
	}
	// Start background flusher
	t.start()
	return t
}

// Send enqueues the message for background delivery
func (t *TelegramNotifier) Send(msg string) error {
	t.mu.Lock()
	t.queue = append(t.queue, msg)
	t.mu.Unlock()
	return nil
}

// Helper function to create HTTP client with proxy
func (t *TelegramNotifier) createHTTPClient() (*http.Client, error) {
	if t.ProxyURL == "" {
		return http.DefaultClient, nil
	}

	proxyURL, err := url.Parse(t.ProxyURL)
	if err != nil {
		return nil, fmt.Errorf("invalid proxy URL: %w", err)
	}

	transport := &http.Transport{
		Proxy: http.ProxyURL(proxyURL),
	}

	return &http.Client{
		Transport: transport,
		Timeout:   30 * time.Second, // Add timeout for safety
	}, nil
}

// sendNow posts a message to Telegram immediately (used by the background worker)
func (t *TelegramNotifier) sendNow(msg string) error {
	apiURL := fmt.Sprintf("https://api.telegram.org/bot%s/sendMessage", t.Token)

	client, err := t.createHTTPClient()
	if err != nil {
		return fmt.Errorf("failed to create HTTP client: %w", err)
	}

	formValues := url.Values{
		"chat_id": {t.ChatID},
		"text":    {msg},
	}
	if t.ParseMode != "" {
		formValues.Add("parse_mode", t.ParseMode)
	}

	resp, err := client.PostForm(apiURL, formValues)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("telegram send failed: %s", resp.Status)
	}
	return nil
}

func (t *TelegramNotifier) SendWithRetry(msg string) error {
	// Enqueue and let the background worker deliver
	return t.Send(msg)
}

func (t *TelegramNotifier) RetryWithNotification(action func() error, description string) error {
	var err error
	for attempt := 1; attempt <= t.MaxAttempts; attempt++ {
		err = action()
		if err == nil {
			return nil
		}
		utils.GetLogger().Printf("Telegram | %s failed (attempt %d/%d): %v", description, attempt, t.MaxAttempts, err)
		msg := fmt.Sprintf("[ERROR RETRY]\nContext: %s\nAttempt: %d/%d\nError: %v\nTime: %s", description, attempt, t.MaxAttempts, err, time.Now().Format(time.RFC3339))
		_ = t.Send(msg)
		time.Sleep(t.Delay)
	}
	msg := fmt.Sprintf("[ERROR PERMANENT]\nContext: %s\nError: %v\nTime: %s", description, err, time.Now().Format(time.RFC3339))
	_ = t.Send(msg)
	return err
}

func (t *TelegramNotifier) start() {
	t.mu.Lock()
	if t.started {
		t.mu.Unlock()
		return
	}
	t.started = true
	t.lastFlush = time.Now()
	t.mu.Unlock()

	go func() {
		for {
			batch := make([]string, 0, 10)

			// Build a batch if possible
			t.mu.Lock()
			if len(t.queue) >= 10 {
				batch = append(batch, t.queue[:10]...)
				t.queue = t.queue[10:]
				t.mu.Unlock()
			} else if len(t.queue) > 0 && time.Since(t.lastFlush) >= 10*time.Second {
				// Flush whatever we have every 10s
				batch = append(batch, t.queue...)
				t.queue = t.queue[:0]
				t.mu.Unlock()
			} else {
				t.mu.Unlock()
				time.Sleep(500 * time.Millisecond)
				continue
			}

			// Combine and send
			msg := strings.Join(batch, "\n")

			var err error
			for attempt := 1; attempt <= t.MaxAttempts; attempt++ {
				err = t.sendNow(msg)
				if err == nil {
					break
				}
				utils.GetLogger().Printf("Telegram | Batch send failed (attempt %d/%d): %v", attempt, t.MaxAttempts, err)
				time.Sleep(t.Delay)
			}

			// Update flush time and sleep 10s between batches as requested
			t.mu.Lock()
			t.lastFlush = time.Now()
			t.mu.Unlock()
			time.Sleep(10 * time.Second)
		}
	}()
}

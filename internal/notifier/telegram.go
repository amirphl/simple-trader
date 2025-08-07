// Package notifier
package notifier

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/amirphl/simple-trader/internal/utils"
)

// Telegram message limits
const (
	MaxTelegramMessageLength = 4000 // Leave some buffer below 4096 limit
	MaxTelegramBatchSize     = 5    // Reduce batch size to prevent long messages
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
	// Validate and sanitize message before enqueueing
	sanitizedMsg, err := t.sanitizeMessage(msg)
	if err != nil {
		utils.GetLogger().Printf("Telegram | Message sanitization failed: %v", err)
		// Try to send a simplified error message instead
		sanitizedMsg = fmt.Sprintf("⚠️ Message delivery failed: %v", err)
	}

	t.mu.Lock()
	t.queue = append(t.queue, sanitizedMsg)
	t.mu.Unlock()
	return nil
}

// sanitizeMessage validates and sanitizes messages for Telegram
func (t *TelegramNotifier) sanitizeMessage(msg string) (string, error) {
	// Check if message is empty
	if strings.TrimSpace(msg) == "" {
		return "", fmt.Errorf("message is empty")
	}

	// Check message length
	if utf8.RuneCountInString(msg) > MaxTelegramMessageLength {
		// Truncate message and add indicator
		truncated := string([]rune(msg)[:MaxTelegramMessageLength-100]) + "\n\n... [Message truncated due to length]"
		utils.GetLogger().Printf("Telegram | Message truncated from %d to %d characters", utf8.RuneCountInString(msg), utf8.RuneCountInString(truncated))
		msg = truncated
	}

	// Basic HTML validation for parse_mode: HTML
	if t.ParseMode == "HTML" {
		msg = t.sanitizeHTML(msg)
	}

	return msg, nil
}

// sanitizeHTML performs basic HTML sanitization for Telegram
func (t *TelegramNotifier) sanitizeHTML(msg string) string {
	// Replace problematic characters that could break HTML parsing
	replacements := map[string]string{
		"&": "&amp;",
		"<": "&lt;",
		">": "&gt;",
	}

	// Only replace &, <, > if they're not part of valid HTML tags
	// This is a simplified approach - in production you might want more sophisticated HTML parsing
	for old, new := range replacements {
		// Skip replacement if it's part of a valid HTML tag
		if old == "&" {
			// Don't replace & if it's followed by a valid HTML entity
			continue
		}
		if old == "<" || old == ">" {
			// Don't replace < or > if they're part of valid HTML tags
			continue
		}
		msg = strings.ReplaceAll(msg, old, new)
	}

	return msg
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

	// Final length check before sending
	if utf8.RuneCountInString(msg) > MaxTelegramMessageLength {
		msg = string([]rune(msg)[:MaxTelegramMessageLength-100]) + "\n\n... [Message truncated]"
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
		// Log the response body for debugging
		body := make([]byte, 1024)
		n, _ := resp.Body.Read(body)
		bodyStr := string(body[:n])
		return fmt.Errorf("telegram send failed: %s - Response: %s", resp.Status, bodyStr)
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
			batch := make([]string, 0, MaxTelegramBatchSize)

			// Build a batch if possible
			t.mu.Lock()
			if len(t.queue) >= MaxTelegramBatchSize {
				batch = append(batch, t.queue[:MaxTelegramBatchSize]...)
				t.queue = t.queue[MaxTelegramBatchSize:]
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
			msg := strings.Join(batch, "\n\n---\n\n")

			// Check combined message length
			if utf8.RuneCountInString(msg) > MaxTelegramMessageLength {
				// If combined message is too long, send messages individually
				utils.GetLogger().Printf("Telegram | Combined message too long (%d chars), sending individually", utf8.RuneCountInString(msg))
				for _, individualMsg := range batch {
					if err := t.sendNow(individualMsg); err != nil {
						utils.GetLogger().Printf("Telegram | Individual message send failed: %v", err)
					}
					time.Sleep(1 * time.Second) // Rate limiting
				}
			} else {
				// Send as batch
				var err error
				for attempt := 1; attempt <= t.MaxAttempts; attempt++ {
					err = t.sendNow(msg)
					if err == nil {
						break
					}
					utils.GetLogger().Printf("Telegram | Batch send failed (attempt %d/%d): %v", attempt, t.MaxAttempts, err)
					time.Sleep(t.Delay)
				}
			}

			// Update flush time and sleep 10s between batches as requested
			t.mu.Lock()
			t.lastFlush = time.Now()
			t.mu.Unlock()
			time.Sleep(10 * time.Second)
		}
	}()
}

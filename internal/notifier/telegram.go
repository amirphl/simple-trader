// Package notifier
package notifier

import (
	"fmt"
	"log"
	"net/http"
	"net/url"
	"time"
)

type TelegramNotifier struct {
	Token       string
	ChatID      string
	MaxAttempts int
	Delay       time.Duration
	ProxyURL    string // Add proxy URL field
}

// Updated constructor to accept proxy URL
func NewTelegramNotifier(token, chatID, proxyURL string, maxAttempts int, delay time.Duration) Notifier {
	return &TelegramNotifier{
		Token:       token,
		ChatID:      chatID,
		ProxyURL:    proxyURL,
		MaxAttempts: maxAttempts,
		Delay:       delay,
	}
}

func (t *TelegramNotifier) Send(msg string) error {
	apiURL := fmt.Sprintf("https://api.telegram.org/bot%s/sendMessage", t.Token)

	// Create HTTP client with proxy
	client, err := t.createHTTPClient()
	if err != nil {
		return fmt.Errorf("failed to create HTTP client: %w", err)
	}

	resp, err := client.PostForm(apiURL, url.Values{
		"chat_id": {t.ChatID},
		"text":    {msg},
	})
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return fmt.Errorf("telegram send failed: %s", resp.Status)
	}
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

func (t *TelegramNotifier) SendWithRetry(msg string) error {
	var err error
	for attempt := 1; attempt <= t.MaxAttempts; attempt++ {
		err = t.Send(msg)
		if err == nil {
			return nil
		}
		log.Printf("Telegram | Notification send failed (attempt %d/%d): %v", attempt, t.MaxAttempts, err)
		time.Sleep(t.Delay)
	}
	log.Printf("Telegram | ESCALATION: Notification send failed after %d attempts: %v\n", t.MaxAttempts, err)
	return err
}

func (t *TelegramNotifier) RetryWithNotification(action func() error, description string) error {
	var err error
	for attempt := 1; attempt <= t.MaxAttempts; attempt++ {
		err = action()
		if err == nil {
			return nil
		}
		log.Printf("Telegram | %s failed (attempt %d/%d): %v", description, attempt, t.MaxAttempts, err)
		msg := fmt.Sprintf("[ERROR RETRY]\nContext: %s\nAttempt: %d/%d\nError: %v\nTime: %s", description, attempt, t.MaxAttempts, err, time.Now().Format(time.RFC3339))
		t.Send(msg)
		time.Sleep(t.Delay)
	}
	msg := fmt.Sprintf("[ERROR PERMANENT]\nContext: %s\nError: %v\nTime: %s", description, err, time.Now().Format(time.RFC3339))
	t.Send(msg)
	return err
}

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
}

func NewTelegramNotifier(token, chatID string, maxAttemps int, delay time.Duration) *TelegramNotifier {
	return &TelegramNotifier{Token: token, ChatID: chatID, MaxAttempts: maxAttemps, Delay: delay}
}

func (t *TelegramNotifier) Send(msg string) error {
	apiURL := fmt.Sprintf("https://api.telegram.org/bot%s/sendMessage", t.Token)
	resp, err := http.PostForm(apiURL, url.Values{
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

func (t *TelegramNotifier) SendWithRetry(msg string) error {
	var err error
	for attempt := 1; attempt <= t.MaxAttempts; attempt++ {
		err = t.Send(msg)
		if err == nil {
			return nil
		}
		log.Printf("Notification send failed (attempt %d/%d): %v", attempt, t.MaxAttempts, err)
		time.Sleep(t.Delay)
	}
	log.Printf("Notification send permanently failed: %v", err)
	fmt.Printf("ESCALATION: Notification send failed after %d attempts: %v\n", t.MaxAttempts, err)
	return err
}

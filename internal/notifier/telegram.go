package notifier

import (
	"fmt"
	"net/http"
	"net/url"
)

type TelegramNotifier struct {
	Token  string
	ChatID string
}

func NewTelegramNotifier(token, chatID string) *TelegramNotifier {
	return &TelegramNotifier{Token: token, ChatID: chatID}
}

func (t *TelegramNotifier) Send(message string) error {
	apiURL := fmt.Sprintf("https://api.telegram.org/bot%s/sendMessage", t.Token)
	resp, err := http.PostForm(apiURL, url.Values{
		"chat_id": {t.ChatID},
		"text":    {message},
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

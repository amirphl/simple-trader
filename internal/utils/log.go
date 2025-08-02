// Package utils
package utils

import (
	"log"
	"os"
	"sync"
)

var (
	logger *log.Logger
	once   sync.Once
)

func GetLogger() *log.Logger {
	once.Do(func() {
		file, err := os.OpenFile("application.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Fatal(err)
		}
		// Create logger with custom format:
		// [DATE TIME FILE:LINE] PREFIX message
		logger = log.New(file, "Simple Trader: ", log.Ldate|log.Ltime|log.Lshortfile)
	})
	return logger
}

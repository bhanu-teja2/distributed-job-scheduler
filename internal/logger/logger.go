package logger

import "go.uber.org/zap"

// New creates a structured JSON logger at the requested level.
func New(level string) (*zap.Logger, error) {
	if level == "debug" {
		return zap.NewDevelopment()
	}
	return zap.NewProduction()
}

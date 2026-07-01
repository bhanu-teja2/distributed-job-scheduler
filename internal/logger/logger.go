package logger

import "go.uber.org/zap"

func New(level string) (*zap.Logger, error) {
	if level == "debug" {
		return zap.NewDevelopment()
	}
	return zap.NewProduction()
}

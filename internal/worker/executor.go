package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net/url"
	"time"

	"github.com/bhanuteja/distributed-job-scheduler/internal/job"
)

type Handler interface {
	Execute(ctx context.Context, j job.Job) error
}

type HandlerFunc func(ctx context.Context, j job.Job) error

func (f HandlerFunc) Execute(ctx context.Context, j job.Job) error {
	return f(ctx, j)
}

type Executor struct {
	handlers map[string]Handler
}

func NewExecutor() *Executor {
	return &Executor{handlers: defaultHandlers()}
}

func (e *Executor) Execute(ctx context.Context, j job.Job) error {
	handler, ok := e.handlers[j.JobType]
	if !ok {
		return fmt.Errorf("unsupported job type %s", j.JobType)
	}
	return handler.Execute(ctx, j)
}

func defaultHandlers() map[string]Handler {
	return map[string]Handler{
		"SEND_EMAIL": HandlerFunc(func(ctx context.Context, j job.Job) error {
			var payload struct {
				To      string `json:"to"`
				Subject string `json:"subject"`
				Body    string `json:"body"`
			}
			if err := json.Unmarshal(j.Payload, &payload); err != nil {
				return err
			}
			if payload.To == "" || payload.Subject == "" {
				return errors.New("email payload requires to and subject")
			}
			return sleepOrCancel(ctx, time.Second)
		}),
		"CALL_WEBHOOK": HandlerFunc(func(ctx context.Context, j job.Job) error {
			var payload struct {
				URL string `json:"url"`
			}
			if err := json.Unmarshal(j.Payload, &payload); err != nil {
				return err
			}
			parsed, err := url.Parse(payload.URL)
			if err != nil || parsed.Scheme == "" || parsed.Host == "" {
				return errors.New("webhook payload requires valid url")
			}
			return sleepOrCancel(ctx, 500*time.Millisecond)
		}),
		"GENERATE_REPORT": HandlerFunc(func(ctx context.Context, j job.Job) error {
			return sleepOrCancel(ctx, 2*time.Second)
		}),
		"PROCESS_PAYMENT_RETRY": HandlerFunc(func(ctx context.Context, j job.Job) error {
			var payload struct {
				PaymentID string `json:"payment_id"`
				Amount    int64  `json:"amount"`
				Currency  string `json:"currency"`
			}
			if err := json.Unmarshal(j.Payload, &payload); err != nil {
				return err
			}
			if payload.PaymentID == "" || payload.Amount <= 0 || payload.Currency == "" {
				return errors.New("payment retry payload requires payment_id, amount, and currency")
			}
			if rand.Intn(100) < 5 {
				return errors.New("simulated payment retry failure")
			}
			return sleepOrCancel(ctx, 750*time.Millisecond)
		}),
	}
}

func sleepOrCancel(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

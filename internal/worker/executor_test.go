package worker

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/bhanuteja/distributed-job-scheduler/internal/job"
	"github.com/google/uuid"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) { return f(request) }

func TestWebhookHandlerSendsIdempotencyHeaders(t *testing.T) {
	var jobID, attempt string
	handler := NewWebhookHandler(ExecutorOptions{AllowedHosts: []string{"webhook.example"}})
	handler.client = &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		jobID = request.Header.Get("X-Scheduler-Job-ID")
		attempt = request.Header.Get("X-Scheduler-Attempt")
		return response(http.StatusNoContent), nil
	})}
	id := uuid.New()
	_, err := handler.Execute(context.Background(), job.Job{ID: id, AttemptNumber: 2, Payload: []byte(`{"url":"https://webhook.example/hook","method":"POST","body":{"ok":true}}`)})
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	if jobID != id.String() || attempt != "2" {
		t.Fatalf("missing scheduler headers: job=%q attempt=%q", jobID, attempt)
	}
}
func TestWebhookHandlerRejectsPrivateTarget(t *testing.T) {
	handler := NewWebhookHandler(ExecutorOptions{})
	_, err := handler.Execute(context.Background(), job.Job{Payload: []byte(`{"url":"http://127.0.0.1/internal"}`)})
	outcome := ClassifyError(err)
	if outcome.ErrorCode != "UNSAFE_WEBHOOK_TARGET" || outcome.Retryable {
		t.Fatalf("unexpected outcome: %+v", outcome)
	}
}
func TestWebhookHandlerClassifiesHTTPFailures(t *testing.T) {
	for _, test := range []struct {
		status    int
		retryable bool
	}{{400, false}, {429, true}, {503, true}} {
		t.Run(http.StatusText(test.status), func(t *testing.T) {
			handler := NewWebhookHandler(ExecutorOptions{AllowedHosts: []string{"webhook.example"}})
			handler.client = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) { return response(test.status), nil })}
			_, err := handler.Execute(context.Background(), job.Job{Payload: []byte(`{"url":"https://webhook.example/hook"}`)})
			if got := ClassifyError(err).Retryable; got != test.retryable {
				t.Fatalf("expected retryable=%v, got %v", test.retryable, got)
			}
		})
	}
}
func response(status int) *http.Response {
	return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("response"))}
}

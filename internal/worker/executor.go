package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/bhanuteja/distributed-job-scheduler/internal/job"
)

type Handler interface {
	Execute(context.Context, job.Job) (json.RawMessage, error)
}
type HandlerFunc func(context.Context, job.Job) (json.RawMessage, error)

func (f HandlerFunc) Execute(ctx context.Context, j job.Job) (json.RawMessage, error) {
	return f(ctx, j)
}

type ExecutorOptions struct {
	AllowedHosts         []string
	AllowPrivateNetworks bool
}

type Executor struct{ handlers map[string]Handler }

func NewExecutor() *Executor { return NewExecutorWithOptions(ExecutorOptions{}) }
func NewExecutorWithOptions(options ExecutorOptions) *Executor {
	webhook := NewWebhookHandler(options)
	return &Executor{handlers: map[string]Handler{"CALL_WEBHOOK": webhook}}
}
func (e *Executor) Execute(ctx context.Context, j job.Job) (json.RawMessage, error) {
	handler, ok := e.handlers[j.JobType]
	if !ok {
		return nil, &ExecutionError{Code: "UNSUPPORTED_JOB_TYPE", Message: "unsupported job type " + j.JobType, Retryable: false}
	}
	return handler.Execute(ctx, j)
}

type ExecutionError struct {
	Code       string
	Message    string
	Retryable  bool
	RetryAfter time.Duration
	Metadata   json.RawMessage
}

func (e *ExecutionError) Error() string { return e.Message }

func ClassifyError(err error) job.ExecutionResult {
	var executionErr *ExecutionError
	if errors.As(err, &executionErr) {
		return job.ExecutionResult{Metadata: executionErr.Metadata, ErrorCode: executionErr.Code, Retryable: executionErr.Retryable, Message: executionErr.Message}
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return job.ExecutionResult{ErrorCode: "TIMEOUT", Retryable: true, Message: err.Error()}
	}
	if errors.Is(err, context.Canceled) {
		return job.ExecutionResult{ErrorCode: "CANCELLED", Retryable: true, Message: err.Error()}
	}
	return job.ExecutionResult{ErrorCode: "HANDLER_ERROR", Retryable: true, Message: err.Error()}
}
func retryAfter(err error) time.Duration {
	var executionErr *ExecutionError
	if errors.As(err, &executionErr) {
		return executionErr.RetryAfter
	}
	return 0
}

type webhookPayload struct {
	URL     string            `json:"url"`
	Method  string            `json:"method"`
	Headers map[string]string `json:"headers"`
	Body    json.RawMessage   `json:"body"`
}

type WebhookHandler struct {
	client  *http.Client
	options ExecutorOptions
}

func NewWebhookHandler(options ExecutorOptions) *WebhookHandler {
	h := &WebhookHandler{options: options}
	dialer := &net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}
	transport := &http.Transport{Proxy: http.ProxyFromEnvironment, DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
		host, _, err := net.SplitHostPort(address)
		if err != nil {
			return nil, err
		}
		if err = h.validateHost(ctx, host); err != nil {
			return nil, err
		}
		return dialer.DialContext(ctx, network, address)
	}, TLSHandshakeTimeout: 5 * time.Second, ResponseHeaderTimeout: 10 * time.Second, IdleConnTimeout: 30 * time.Second, MaxIdleConns: 50}
	h.client = &http.Client{Transport: transport, Timeout: 30 * time.Second, CheckRedirect: func(req *http.Request, via []*http.Request) error {
		if len(via) >= 3 {
			return errors.New("too many redirects")
		}
		return h.validateURL(req.Context(), req.URL)
	}}
	return h
}

func (h *WebhookHandler) Execute(ctx context.Context, j job.Job) (json.RawMessage, error) {
	var payload webhookPayload
	if err := json.Unmarshal(j.Payload, &payload); err != nil {
		return nil, &ExecutionError{Code: "INVALID_PAYLOAD", Message: "webhook payload must be valid JSON", Retryable: false}
	}
	parsed, err := url.Parse(payload.URL)
	if err != nil {
		return nil, &ExecutionError{Code: "INVALID_URL", Message: "webhook URL is invalid", Retryable: false}
	}
	if err = h.validateURL(ctx, parsed); err != nil {
		return nil, &ExecutionError{Code: "UNSAFE_WEBHOOK_TARGET", Message: err.Error(), Retryable: false}
	}
	method := strings.ToUpper(strings.TrimSpace(payload.Method))
	if method == "" {
		method = http.MethodPost
	}
	switch method {
	case http.MethodGet, http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
	default:
		return nil, &ExecutionError{Code: "INVALID_METHOD", Message: "unsupported webhook method", Retryable: false}
	}
	var body io.Reader
	if len(payload.Body) > 0 {
		body = strings.NewReader(string(payload.Body))
	}
	req, err := http.NewRequestWithContext(ctx, method, parsed.String(), body)
	if err != nil {
		return nil, &ExecutionError{Code: "REQUEST_BUILD_FAILED", Message: err.Error(), Retryable: false}
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Scheduler-Job-ID", j.ID.String())
	req.Header.Set("X-Scheduler-Attempt", strconv.Itoa(j.AttemptNumber))
	req.Header.Set("Idempotency-Key", j.ID.String())
	for key, value := range payload.Headers {
		canonical := http.CanonicalHeaderKey(key)
		if isReservedHeader(canonical) {
			continue
		}
		req.Header.Set(canonical, value)
	}
	started := time.Now()
	resp, err := h.client.Do(req)
	if err != nil {
		return nil, &ExecutionError{Code: "WEBHOOK_NETWORK_ERROR", Message: err.Error(), Retryable: true}
	}
	defer resp.Body.Close()
	excerpt, readErr := io.ReadAll(io.LimitReader(resp.Body, 4097))
	if readErr != nil {
		return nil, &ExecutionError{Code: "WEBHOOK_READ_ERROR", Message: readErr.Error(), Retryable: true}
	}
	truncated := len(excerpt) > 4096
	if truncated {
		excerpt = excerpt[:4096]
	}
	metadata, _ := json.Marshal(map[string]any{"status_code": resp.StatusCode, "duration_ms": time.Since(started).Milliseconds(), "response_excerpt": string(excerpt), "response_truncated": truncated})
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return metadata, nil
	}
	retryable := resp.StatusCode == http.StatusRequestTimeout || resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500
	message := fmt.Sprintf("webhook returned HTTP %d", resp.StatusCode)
	return nil, &ExecutionError{Code: "WEBHOOK_HTTP_ERROR", Message: message, Retryable: retryable, RetryAfter: parseRetryAfter(resp.Header.Get("Retry-After")), Metadata: metadata}
}

func (h *WebhookHandler) validateURL(ctx context.Context, target *url.URL) error {
	if target.Scheme != "http" && target.Scheme != "https" {
		return errors.New("webhook URL must use http or https")
	}
	if target.User != nil {
		return errors.New("webhook URL must not contain credentials")
	}
	if target.Hostname() == "" {
		return errors.New("webhook URL requires a host")
	}
	return h.validateHost(ctx, target.Hostname())
}
func (h *WebhookHandler) validateHost(ctx context.Context, host string) error {
	for _, allowed := range h.options.AllowedHosts {
		if strings.EqualFold(strings.TrimSpace(allowed), host) {
			return nil
		}
	}
	addresses, err := net.DefaultResolver.LookupIPAddr(ctx, host)
	if err != nil {
		return fmt.Errorf("resolve webhook host: %w", err)
	}
	if h.options.AllowPrivateNetworks {
		return nil
	}
	for _, address := range addresses {
		ip := address.IP
		if ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalMulticast() || ip.IsLinkLocalUnicast() || ip.IsUnspecified() {
			return errors.New("webhook target resolves to a private or local address")
		}
	}
	return nil
}
func isReservedHeader(key string) bool {
	switch key {
	case "Host", "Content-Length", "X-Scheduler-Job-Id", "X-Scheduler-Attempt", "Idempotency-Key":
		return true
	}
	return false
}
func parseRetryAfter(value string) time.Duration {
	seconds, err := strconv.Atoi(strings.TrimSpace(value))
	if err != nil || seconds < 0 {
		return 0
	}
	duration := time.Duration(seconds) * time.Second
	if duration > 15*time.Minute {
		return 15 * time.Minute
	}
	return duration
}

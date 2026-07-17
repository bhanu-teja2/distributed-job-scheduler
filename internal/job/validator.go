package job

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

var supportedJobTypes = map[string]struct{}{
	"CALL_WEBHOOK": {},
}

func validateCreate(req CreateRequest) error {
	if strings.TrimSpace(req.Name) == "" {
		return fmt.Errorf("name is required")
	}
	if _, ok := supportedJobTypes[req.JobType]; !ok {
		return fmt.Errorf("unsupported job type")
	}
	if len(req.Payload) == 0 || !json.Valid(req.Payload) || string(req.Payload) == "null" {
		return fmt.Errorf("payload must be valid JSON")
	}
	if req.RunAt.IsZero() {
		return fmt.Errorf("run_at is required")
	}
	if req.Priority < 0 || req.Priority > 10 {
		return fmt.Errorf("priority must be between 0 and 10")
	}
	if req.MaxRetries < 0 {
		return fmt.Errorf("max_retries must not be negative")
	}
	if req.RetryBackoffSeconds < 0 {
		return fmt.Errorf("retry_backoff_seconds must not be negative")
	}
	if req.TimeoutSeconds < 0 {
		return fmt.Errorf("timeout_seconds must not be negative")
	}
	return nil
}

func initialStatus(runAt time.Time, now time.Time) Status {
	if runAt.After(now) {
		return StatusScheduled
	}
	return StatusPending
}

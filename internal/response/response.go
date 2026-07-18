package response

import (
	"encoding/json"
	"net/http"
)

// ErrorBody is the stable machine-readable API error payload.
type ErrorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// Envelope is the response shape returned by every JSON API endpoint.
type Envelope struct {
	Success   bool       `json:"success"`
	Data      any        `json:"data"`
	Error     *ErrorBody `json:"error"`
	RequestID string     `json:"request_id"`
}

// JSON writes a successful response envelope.
func JSON(w http.ResponseWriter, r *http.Request, status int, data any) {
	write(w, status, Envelope{Success: true, Data: data, Error: nil, RequestID: RequestID(r)})
}

// Error writes an unsuccessful response envelope with a stable error code.
func Error(w http.ResponseWriter, r *http.Request, status int, code, message string) {
	write(w, status, Envelope{Success: false, Data: nil, Error: &ErrorBody{Code: code, Message: message}, RequestID: RequestID(r)})
}

// RequestID returns the request correlation ID installed by middleware.
func RequestID(r *http.Request) string {
	requestID, _ := r.Context().Value(requestIDKey{}).(string)
	return requestID
}

type requestIDKey struct{}

// ContextKey exposes the private request-ID context key to middleware without
// making its concrete type part of the package API.
func ContextKey() any {
	return requestIDKey{}
}

func write(w http.ResponseWriter, status int, body Envelope) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

package response

import (
	"encoding/json"
	"net/http"
)

type ErrorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type Envelope struct {
	Success   bool       `json:"success"`
	Data      any        `json:"data"`
	Error     *ErrorBody `json:"error"`
	RequestID string     `json:"request_id"`
}

func JSON(w http.ResponseWriter, r *http.Request, status int, data any) {
	write(w, status, Envelope{Success: true, Data: data, Error: nil, RequestID: RequestID(r)})
}

func Error(w http.ResponseWriter, r *http.Request, status int, code, message string) {
	write(w, status, Envelope{Success: false, Data: nil, Error: &ErrorBody{Code: code, Message: message}, RequestID: RequestID(r)})
}

func RequestID(r *http.Request) string {
	requestID, _ := r.Context().Value(requestIDKey{}).(string)
	return requestID
}

type requestIDKey struct{}

func ContextKey() any {
	return requestIDKey{}
}

func write(w http.ResponseWriter, status int, body Envelope) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

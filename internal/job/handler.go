package job

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/bhanuteja/distributed-job-scheduler/internal/auth"
	appErrors "github.com/bhanuteja/distributed-job-scheduler/internal/errors"
	"github.com/bhanuteja/distributed-job-scheduler/internal/response"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
)

type Handler struct {
	service *Service
}

func NewHandler(service *Service) *Handler {
	return &Handler{service: service}
}

func (h *Handler) Routes() chi.Router {
	r := chi.NewRouter()
	r.Post("/", auth.Require(auth.RoleOperator, h.create))
	r.Get("/", h.list)
	r.Get("/{jobID}", h.get)
	r.Get("/{jobID}/attempts", h.attempts)
	r.Get("/{jobID}/events", h.events)
	r.Post("/{jobID}/cancel", auth.Require(auth.RoleOperator, h.cancel))
	r.Post("/{jobID}/pause", auth.Require(auth.RoleOperator, h.pause))
	r.Post("/{jobID}/resume", auth.Require(auth.RoleOperator, h.resume))
	r.Post("/{jobID}/retry", auth.Require(auth.RoleOperator, h.retry))
	return r
}

func (h *Handler) DeadLetterRoutes() chi.Router {
	r := chi.NewRouter()
	r.Get("/", h.deadLetters)
	r.Post("/{deadLetterID}/requeue", auth.Require(auth.RoleOperator, h.requeueDeadLetter))
	return r
}

func (h *Handler) create(w http.ResponseWriter, r *http.Request) {
	var req CreateRequest
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		response.Error(w, r, http.StatusBadRequest, "INVALID_JSON", "request body must be valid JSON")
		return
	}
	req.IdempotencyKey = r.Header.Get("Idempotency-Key")
	resp, err := h.service.Create(r.Context(), req)
	if err != nil {
		writeError(w, r, err)
		return
	}
	status := http.StatusCreated
	if resp.Replayed {
		status = http.StatusOK
	}
	response.JSON(w, r, status, resp)
}

func (h *Handler) list(w http.ResponseWriter, r *http.Request) {
	page, err := queryInt(r, "page", 1)
	if err != nil {
		response.Error(w, r, http.StatusBadRequest, "INVALID_INPUT", err.Error())
		return
	}
	pageSize, err := queryInt(r, "page_size", 20)
	if err != nil {
		response.Error(w, r, http.StatusBadRequest, "INVALID_INPUT", err.Error())
		return
	}
	filter := ListFilter{
		Status:   Status(r.URL.Query().Get("status")),
		JobType:  r.URL.Query().Get("job_type"),
		Page:     page,
		PageSize: pageSize,
		Sort:     r.URL.Query().Get("sort"),
		Order:    r.URL.Query().Get("order"),
	}
	if value := r.URL.Query().Get("created_after"); value != "" {
		parsed, err := time.Parse(time.RFC3339, value)
		if err != nil {
			response.Error(w, r, http.StatusBadRequest, "INVALID_INPUT", "created_after must be RFC3339")
			return
		}
		filter.CreatedAfter = &parsed
	}
	if value := r.URL.Query().Get("created_before"); value != "" {
		parsed, err := time.Parse(time.RFC3339, value)
		if err != nil {
			response.Error(w, r, http.StatusBadRequest, "INVALID_INPUT", "created_before must be RFC3339")
			return
		}
		filter.CreatedBefore = &parsed
	}
	jobPage, err := h.service.List(r.Context(), filter)
	if err != nil {
		writeError(w, r, err)
		return
	}
	response.JSON(w, r, http.StatusOK, jobPage)
}

func (h *Handler) events(w http.ResponseWriter, r *http.Request) {
	id, ok := parseUUID(w, r, "jobID")
	if !ok {
		return
	}
	items, err := h.service.Events(r.Context(), id)
	if err != nil {
		writeError(w, r, err)
		return
	}
	response.JSON(w, r, http.StatusOK, items)
}

func (h *Handler) get(w http.ResponseWriter, r *http.Request) {
	id, ok := parseUUID(w, r, "jobID")
	if !ok {
		return
	}
	j, err := h.service.Get(r.Context(), id)
	if err != nil {
		writeError(w, r, err)
		return
	}
	response.JSON(w, r, http.StatusOK, j)
}

func (h *Handler) attempts(w http.ResponseWriter, r *http.Request) {
	id, ok := parseUUID(w, r, "jobID")
	if !ok {
		return
	}
	attempts, err := h.service.Attempts(r.Context(), id)
	if err != nil {
		writeError(w, r, err)
		return
	}
	response.JSON(w, r, http.StatusOK, attempts)
}

func (h *Handler) cancel(w http.ResponseWriter, r *http.Request) {
	h.transition(w, r, h.service.Cancel)
}

func (h *Handler) pause(w http.ResponseWriter, r *http.Request) {
	h.transition(w, r, h.service.Pause)
}

func (h *Handler) resume(w http.ResponseWriter, r *http.Request) {
	h.transition(w, r, h.service.Resume)
}

func (h *Handler) retry(w http.ResponseWriter, r *http.Request) {
	h.transition(w, r, h.service.Retry)
}

func (h *Handler) transition(w http.ResponseWriter, r *http.Request, fn func(context.Context, uuid.UUID) error) {
	id, ok := parseUUID(w, r, "jobID")
	if !ok {
		return
	}
	if err := fn(r.Context(), id); err != nil {
		writeError(w, r, err)
		return
	}
	response.JSON(w, r, http.StatusOK, map[string]string{"job_id": id.String()})
}

func (h *Handler) deadLetters(w http.ResponseWriter, r *http.Request) {
	page, parseErr := queryInt(r, "page", 1)
	if parseErr != nil {
		response.Error(w, r, http.StatusBadRequest, "INVALID_INPUT", parseErr.Error())
		return
	}
	pageSize, parseErr := queryInt(r, "page_size", 20)
	if parseErr != nil {
		response.Error(w, r, http.StatusBadRequest, "INVALID_INPUT", parseErr.Error())
		return
	}
	items, err := h.service.ListDeadLetters(r.Context(), page, pageSize)
	if err != nil {
		writeError(w, r, err)
		return
	}
	response.JSON(w, r, http.StatusOK, items)
}

func (h *Handler) requeueDeadLetter(w http.ResponseWriter, r *http.Request) {
	id, ok := parseUUID(w, r, "deadLetterID")
	if !ok {
		return
	}
	j, err := h.service.RequeueDeadLetter(r.Context(), id)
	if err != nil {
		writeError(w, r, err)
		return
	}
	response.JSON(w, r, http.StatusCreated, j)
}

func queryInt(r *http.Request, key string, fallback int) (int, error) {
	value := r.URL.Query().Get(key)
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer", key)
	}
	if parsed < 1 {
		return 0, fmt.Errorf("%s must be at least 1", key)
	}
	return parsed, nil
}

func parseUUID(w http.ResponseWriter, r *http.Request, param string) (uuid.UUID, bool) {
	value := chi.URLParam(r, param)
	id, err := uuid.Parse(value)
	if err != nil {
		response.Error(w, r, http.StatusBadRequest, "INVALID_UUID", param+" must be a valid UUID")
		return uuid.Nil, false
	}
	return id, true
}

func writeError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, appErrors.ErrInvalidInput):
		response.Error(w, r, http.StatusBadRequest, "INVALID_INPUT", err.Error())
	case errors.Is(err, appErrors.ErrInvalidTransition):
		response.Error(w, r, http.StatusConflict, "INVALID_TRANSITION", err.Error())
	case errors.Is(err, appErrors.ErrConflict):
		response.Error(w, r, http.StatusConflict, "CONFLICT", err.Error())
	case errors.Is(err, appErrors.ErrIdempotency):
		response.Error(w, r, http.StatusConflict, "IDEMPOTENCY_CONFLICT", err.Error())
	case errors.Is(err, appErrors.ErrNotFound):
		response.Error(w, r, http.StatusNotFound, "NOT_FOUND", "resource not found")
	default:
		response.Error(w, r, http.StatusInternalServerError, "INTERNAL_ERROR", "internal server error")
	}
}

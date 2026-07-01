package job

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

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
	r.Post("/", h.create)
	r.Get("/", h.list)
	r.Get("/{jobID}", h.get)
	r.Get("/{jobID}/attempts", h.attempts)
	return r
}

func (h *Handler) DeadLetterRoutes() chi.Router {
	r := chi.NewRouter()
	r.Get("/", h.deadLetters)
	r.Post("/{deadLetterID}/requeue", h.requeueDeadLetter)
	return r
}

func (h *Handler) create(w http.ResponseWriter, r *http.Request) {
	var req CreateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		response.Error(w, r, http.StatusBadRequest, "INVALID_JSON", "request body must be valid JSON")
		return
	}
	resp, err := h.service.Create(r.Context(), req)
	if err != nil {
		writeError(w, r, err)
		return
	}
	response.JSON(w, r, http.StatusCreated, resp)
}

func (h *Handler) list(w http.ResponseWriter, r *http.Request) {
	filter := ListFilter{
		Status:   Status(r.URL.Query().Get("status")),
		JobType:  r.URL.Query().Get("job_type"),
		Page:     queryInt(r, "page", 1),
		PageSize: queryInt(r, "page_size", 20),
	}
	page, err := h.service.List(r.Context(), filter)
	if err != nil {
		writeError(w, r, err)
		return
	}
	response.JSON(w, r, http.StatusOK, page)
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

func (h *Handler) deadLetters(w http.ResponseWriter, r *http.Request) {
	items, err := h.service.ListDeadLetters(r.Context(), queryInt(r, "page", 1), queryInt(r, "page_size", 20))
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

func queryInt(r *http.Request, key string, fallback int) int {
	value := r.URL.Query().Get(key)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
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
	case errors.Is(err, appErrors.ErrNotFound):
		response.Error(w, r, http.StatusNotFound, "NOT_FOUND", "resource not found")
	default:
		response.Error(w, r, http.StatusInternalServerError, "INTERNAL_ERROR", "internal server error")
	}
}

package httpapi

import (
	"encoding/json"
	"net/http"

	"github.com/lucky-talkin/QuickTicket/services/waiting-room/internal/admission"
	"github.com/lucky-talkin/QuickTicket/services/waiting-room/internal/queue"
)

type Handler struct {
	queue *queue.Service
	tokens *admission.Service
}

func New(q *queue.Service, tokens *admission.Service) *Handler {
	return &Handler{queue: q, tokens: tokens}
}

type input struct {
	EventID int64 `json:"event_id"`
	UserID int64 `json:"user_id"`
}

func (h *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("/queue/join", h.method(http.MethodPost, h.join))
	mux.HandleFunc("/queue/status", h.method(http.MethodPost, h.status))
}

func (h *Handler) method(method string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != method { fail(w, http.StatusMethodNotAllowed, "method not allowed"); return }
		next(w, r)
	}
}

func (h *Handler) join(w http.ResponseWriter, r *http.Request) {
	in, ok := decode(w, r); if !ok { return }
	result, err := h.queue.Join(r.Context(), in.EventID, in.UserID)
	if err != nil { fail(w, http.StatusServiceUnavailable, err.Error()); return }
	h.respond(w, r, in, result)
}

func (h *Handler) status(w http.ResponseWriter, r *http.Request) {
	in, ok := decode(w, r); if !ok { return }
	result, err := h.queue.Status(r.Context(), in.EventID, in.UserID)
	if err != nil { fail(w, http.StatusServiceUnavailable, err.Error()); return }
	if !result.Admitted && result.Position < 0 { fail(w, http.StatusNotFound, "user is not in this event queue"); return }
	h.respond(w, r, in, result)
}

func (h *Handler) respond(w http.ResponseWriter, r *http.Request, in input, result queue.Result) {
	body := map[string]any{"event_id": in.EventID, "user_id": in.UserID, "status": "waiting", "position": result.Position}
	if result.Admitted {
		token, err := h.tokens.GetOrCreate(r.Context(), in.EventID, in.UserID)
		if err != nil { fail(w, http.StatusServiceUnavailable, err.Error()); return }
		body["status"] = "admitted"
		body["admission_token"] = token
		body["admission_expires_in_seconds"] = h.queue.AdmissionTTLSeconds()
		delete(body, "position")
	}
	write(w, http.StatusOK, body)
}

func decode(w http.ResponseWriter, r *http.Request) (input, bool) {
	var in input
	if json.NewDecoder(r.Body).Decode(&in) != nil || in.EventID < 1 || in.UserID < 1 {
		fail(w, http.StatusBadRequest, "event_id and user_id required")
		return input{}, false
	}
	return in, true
}

func write(w http.ResponseWriter, code int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(value)
}

func fail(w http.ResponseWriter, code int, message string) {
	write(w, code, map[string]string{"error": message})
}

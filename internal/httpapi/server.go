// Package httpapi exposes a Broker over HTTP.
//
// It is deliberately thin: it decodes JSON, calls one queue method, and encodes
// the result. Every ordering and durability decision lives in package queue, so
// this file has nothing interesting to get wrong.
package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/juhokim/queuemaxxing/internal/queue"
)

// maxBodyBytes caps a request body so one client cannot exhaust memory.
const maxBodyBytes = 1 << 20

type Server struct {
	broker *queue.Broker
	mux    *http.ServeMux
}

func New(b *queue.Broker) *Server {
	s := &Server{broker: b, mux: http.NewServeMux()}

	// Go's ServeMux has matched on method and path variables since 1.22, which
	// is why this project needs no router dependency.
	s.mux.HandleFunc("POST /queues", s.createQueue)
	s.mux.HandleFunc("GET /queues", s.listQueues)
	s.mux.HandleFunc("GET /queues/{name}", s.getQueue)
	s.mux.HandleFunc("DELETE /queues/{name}", s.deleteQueue)
	s.mux.HandleFunc("POST /queues/{name}/messages", s.send)
	s.mux.HandleFunc("POST /queues/{name}/messages/pop", s.pop)
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.mux.ServeHTTP(w, r) }

type createRequest struct {
	Name         string      `json:"name"`
	Order        queue.Order `json:"order"`
	Priority     bool        `json:"priority"`
	DelaySeconds int         `json:"delay_seconds"`
}

func (s *Server) createQueue(w http.ResponseWriter, r *http.Request) {
	var req createRequest
	if !decode(w, r, &req) {
		return
	}
	if req.Order == "" {
		req.Order = queue.FIFO
	}
	q, err := s.broker.Create(req.Name, queue.Policy{
		Order:        req.Order,
		Priority:     req.Priority,
		DelaySeconds: req.DelaySeconds,
	})
	if err != nil {
		if errors.Is(err, queue.ErrExists) {
			fail(w, http.StatusConflict, err)
			return
		}
		fail(w, http.StatusBadRequest, err)
		return
	}
	respond(w, http.StatusCreated, q.Stats())
}

func (s *Server) listQueues(w http.ResponseWriter, r *http.Request) {
	respond(w, http.StatusOK, s.broker.List())
}

func (s *Server) getQueue(w http.ResponseWriter, r *http.Request) {
	q, err := s.broker.Get(r.PathValue("name"))
	if err != nil {
		fail(w, http.StatusNotFound, err)
		return
	}
	respond(w, http.StatusOK, q.Stats())
}

func (s *Server) deleteQueue(w http.ResponseWriter, r *http.Request) {
	if err := s.broker.Delete(r.PathValue("name")); err != nil {
		fail(w, http.StatusNotFound, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

type sendRequest struct {
	Body     string `json:"body"`
	Priority int    `json:"priority"`
	// DelaySeconds overrides the queue's default for this one message, in the
	// spirit of an SQS message timer. Absent means "use the queue default",
	// which is why it is a pointer: 0 has to stay distinguishable from unset.
	DelaySeconds *int `json:"delay_seconds"`
}

func (s *Server) send(w http.ResponseWriter, r *http.Request) {
	q, err := s.broker.Get(r.PathValue("name"))
	if err != nil {
		fail(w, http.StatusNotFound, err)
		return
	}
	var req sendRequest
	if !decode(w, r, &req) {
		return
	}
	m, err := q.Enqueue(req.Body, req.Priority, req.DelaySeconds)
	if err != nil {
		fail(w, http.StatusBadRequest, err)
		return
	}
	// The message is on disk by the time Enqueue returns, so 201 is a promise
	// the server can keep across a crash.
	respond(w, http.StatusCreated, m)
}

func (s *Server) pop(w http.ResponseWriter, r *http.Request) {
	q, err := s.broker.Get(r.PathValue("name"))
	if err != nil {
		fail(w, http.StatusNotFound, err)
		return
	}
	m, err := q.Dequeue()
	if err != nil {
		fail(w, http.StatusInternalServerError, err)
		return
	}
	if m == nil {
		// Nothing deliverable. Distinct from 404: the queue exists, and it may
		// well hold messages that are still inside their delay.
		w.WriteHeader(http.StatusNoContent)
		return
	}
	respond(w, http.StatusOK, m)
}

func decode(w http.ResponseWriter, r *http.Request, dst any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		fail(w, http.StatusBadRequest, err)
		return false
	}
	return true
}

func respond(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func fail(w http.ResponseWriter, code int, err error) {
	respond(w, code, map[string]string{"error": err.Error()})
}

package handler

import (
	"net/http"
	"time"

	"github.com/UllasSG/Uptime-status-checker/internal/config"
	"github.com/UllasSG/Uptime-status-checker/internal/database"
)

type Server struct {
	cfg   config.Config
	store *database.Store
}

func NewServer(cfg config.Config, store *database.Store) *Server {
	return &Server{cfg: cfg, store: store}
}

func (s *Server) Health(w http.ResponseWriter, r *http.Request) {
	respondWithJSON(w, http.StatusOK, map[string]string{"message": "Hello World"})
}

// GET /status/{name}
func (s *Server) GetStatus(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	status, err := s.store.GetStatus(r.Context(), name)
	if err != nil {
		respondWithError(w, http.StatusInternalServerError, err.Error())
		return
	}
	respondWithJSON(w, http.StatusOK, status)
}



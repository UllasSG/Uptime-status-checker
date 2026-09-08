package handler

import (
	"net/http"

	"github.com/UllasSG/Uptime-status-checker/internal/config"
)

type Server struct {
	cfg config.Config
	// store *store.Store
	// db    *db.DB
}

func NewServer(cfg config.Config) *Server {
	return &Server{cfg: cfg}
}

func (s *Server) Health(w http.ResponseWriter, r *http.Request) {
	respondWithJSON(w, http.StatusOK, map[string]string{"message": "Hello World"})
}

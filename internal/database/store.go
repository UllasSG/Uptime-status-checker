package database

import (
	"context"
	"database/sql"
	"time"

	checker "github.com/UllasSG/Uptime-status-checker/internal/Checker"
)

type Store struct {
	db *sql.DB
}

func NewStore(db *sql.DB) *Store {
	return &Store{
		db: db,
	}
}

const schema = `
CREATE TABLE IF NOT EXISTS results (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    target      TEXT      NOT NULL,
    up          INTEGER   NOT NULL,
    status_code INTEGER   NOT NULL,
    latency_ms  INTEGER   NOT NULL,
    err         TEXT      NOT NULL DEFAULT '',
    checked_at  TIMESTAMP NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_results_target_time
    ON results (target, checked_at DESC);
`

func (s *Store) InitDB(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, schema)
	return err

}

func (s *Store) SaveResults(ctx context.Context, batch []checker.JobResult) error {
	if len(batch) == 0 {
		return nil
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx,
		`INSERT INTO results (target, up, status_code, latency_ms, err, checked_at)
         VALUES (?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, r := range batch {
		if _, err := stmt.ExecContext(ctx,
			r.Target.Name,
			r.Up,
			r.StatusCode,
			r.Latency.Milliseconds(),
			r.Err,
			r.CheckedAt,
		); err != nil {
			return err
		}
	}

	return tx.Commit()
}

type Status struct {
	Name       string    `json:"name"`
	Up         bool      `json:"up"`
	StatusCode int       `json:"status_code"`
	LatencyMs  int64     `json:"latency_ms"`
	Err        string    `json:"err,omitempty"`
	CheckedAt  time.Time `json:"checked_at"`
}

func (s *Store) GetStatus(ctx context.Context, targetName string) (Status, error) {
	var res Status
	query := `SELECT target, up, status_code, latency_ms, err, checked_at
           FROM results
          WHERE target = ?
          ORDER BY checked_at DESC
          LIMIT 1`

	err := s.db.QueryRowContext(ctx, query, targetName).Scan(&res.Name, &res.Up, &res.StatusCode, &res.LatencyMs, &res.Err, &res.CheckedAt)
	return res, err
}

func (s *Store) GetHistory(ctx context.Context, targetName string, since time.Time) ([]Status, error) {
	var history []Status
	query := `SELECT target, up, status_code, latency_ms, err, checked_at
	FROM results
   WHERE target = ? AND checked_at >= ?
   ORDER BY checked_at DESC`

	queryResult, err := s.db.QueryContext(ctx, query, targetName, since)
	if err != nil {
		return history, err
	}
	defer queryResult.Close()

	for queryResult.Next() {
		var res Status
		err = queryResult.Scan(&res.Name, &res.Up, &res.StatusCode, &res.LatencyMs, &res.Err, &res.CheckedAt)
		if err != nil {
			return nil, err
		}
		history = append(history, res)
	}

	return history, queryResult.Err()

}

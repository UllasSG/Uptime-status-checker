# Setting up the `Store` (DB layer + dependency injection)

A step-by-step guide to wiring a single database layer into the uptime checker.
You write the code — this doc tells you **what** goes where and **why**.

## The idea in one picture

```
Scheduler → WorkerPool → Checker → JobResult
                                      │  worker calls Enqueue()
                                      ▼
                        ResultQueue.ScheduleBatches()  ──batch every N s──▶ Store.SaveResults()   ← the ONLY writer
                                                                            │
Handlers ── Store.GetStatus / GetHistory ─────────────────────────────▶ same *sql.DB pool   ← readers
```

- **`Store`** wraps `*sql.DB` and owns *all* SQL. Nothing else imports `database/sql`.
- **One writer:** only `ResultQueue.ScheduleBatches` calls `SaveResults`. That's what makes "single writer" true.
- **DI = constructor injection.** `main.go` is the *composition root*: build each dependency once, pass it down. No framework needed.

Prereqs already done: `go-sqlite3` installed, DSN uses `?_journal_mode=WAL&_busy_timeout=5000`.

---

## Step 1 — Design the schema

One table of check results, plus an index for the two queries you'll run
(latest-per-target and history-since).

```sql
CREATE TABLE IF NOT EXISTS results (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    target      TEXT      NOT NULL,          -- config.Target.Name
    up          INTEGER   NOT NULL,          -- 0 / 1
    status_code INTEGER   NOT NULL,
    latency_ms  INTEGER   NOT NULL,
    err         TEXT      NOT NULL DEFAULT '',
    checked_at  TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_results_target_time
    ON results (target, checked_at DESC);
```

> Keep it minimal now. You can add a `targets` table later if you want to
> store config in the DB; for now `target` is just the name string.

---

## Step 2 — Create `internal/database/store.go`

Struct + constructor + a one-shot schema initializer.

```go
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
    return &Store{db: db}
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

// Init creates tables/indexes if they don't exist. Call once at startup.
func (s *Store) Init(ctx context.Context) error {
    _, err := s.db.ExecContext(ctx, schema)
    return err
}
```

---

## Step 3 — Writer method: `SaveResults` (batch insert in one transaction)

This is the **only** method that writes. Batching many inserts into a single
transaction means one fsync per flush instead of one per row.

```go
// Add to store.go

func (s *Store) SaveResults(ctx context.Context, batch []checker.JobResult) error {
    if len(batch) == 0 {
        return nil
    }

    tx, err := s.db.BeginTx(ctx, nil)
    if err != nil {
        return err
    }
    defer tx.Rollback() // no-op once Commit succeeds

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
            r.Up,                    // go-sqlite3 stores bool as 0/1
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
```

Field reference (`checker.JobResult`): `Target config.Target`, `Up bool`,
`Err string`, `StatusCode int`, `Latency time.Duration`, `CheckedAt time.Time`.

---

## Step 4 — Reader methods: `GetStatus`, `GetHistory`

These are called by handlers and are safe to run concurrently with the writer
(that's what WAL buys you).

```go
// Add to store.go

// Status is the read-model returned to the API.
type Status struct {
    Name       string    `json:"name"`
    Up         bool      `json:"up"`
    StatusCode int       `json:"status_code"`
    LatencyMs  int64     `json:"latency_ms"`
    Err        string    `json:"err,omitempty"`
    CheckedAt  time.Time `json:"checked_at"`
}

// GetStatus returns the most recent result for one target.
func (s *Store) GetStatus(ctx context.Context, name string) (Status, error) {
    var st Status
    err := s.db.QueryRowContext(ctx,
        `SELECT target, up, status_code, latency_ms, err, checked_at
           FROM results
          WHERE target = ?
          ORDER BY checked_at DESC
          LIMIT 1`, name).
        Scan(&st.Name, &st.Up, &st.StatusCode, &st.LatencyMs, &st.Err, &st.CheckedAt)
    return st, err // caller can check errors.Is(err, sql.ErrNoRows)
}

// GetHistory returns results for a target since `since` (newest first).
func (s *Store) GetHistory(ctx context.Context, name string, since time.Time) ([]Status, error) {
    rows, err := s.db.QueryContext(ctx,
        `SELECT target, up, status_code, latency_ms, err, checked_at
           FROM results
          WHERE target = ? AND checked_at >= ?
          ORDER BY checked_at DESC`, name, since)
    if err != nil {
        return nil, err
    }
    defer rows.Close()

    var out []Status
    for rows.Next() {
        var st Status
        if err := rows.Scan(&st.Name, &st.Up, &st.StatusCode,
            &st.LatencyMs, &st.Err, &st.CheckedAt); err != nil {
            return nil, err
        }
        out = append(out, st)
    }
    return out, rows.Err()
}
```

---

## Step 5 — Rework `internal/database/queue.go`

Two changes: the queue now **owns its channel** and holds a `*Store`, and it
gains a `ScheduleBatches` method — the single writer goroutine that, every
`flushInterval`, grabs whatever's buffered and writes it as one batch.

```go
package database

import (
    "context"
    "log"
    "time"

    checker "github.com/UllasSG/Uptime-status-checker/internal/Checker"
)

type ResultQueue struct {
    results       chan checker.JobResult
    store         *Store
    flushInterval time.Duration
}

func NewResultQueue(store *Store, buffer int, flushInterval time.Duration) *ResultQueue {
    return &ResultQueue{
        results:       make(chan checker.JobResult, buffer),
        store:         store,
        flushInterval: flushInterval,
    }
}

// Enqueue is called by workers (many goroutines).
func (rq *ResultQueue) Enqueue(result checker.JobResult) {
    rq.results <- result
}

// ScheduleBatches is the SINGLE writer goroutine. Start once:
// `go rq.ScheduleBatches(ctx)`. On each tick it takes everything currently
// buffered and persists it in one batch.
func (rq *ResultQueue) ScheduleBatches(ctx context.Context) {
    ticker := time.NewTicker(rq.flushInterval)
    defer ticker.Stop()

    for {
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
            n := len(rq.results)
            if n == 0 {
                continue
            }
            batch := make([]checker.JobResult, 0, n)
            for range n {
                batch = append(batch, <-rq.results)
            }
            if err := rq.store.SaveResults(ctx, batch); err != nil {
                log.Printf("resultqueue: save %d results: %v", n, err)
            }
        }
    }
}
```

**Why the counted `for range n` loop is safe:** `len()` reports exactly how many
items are buffered, and `ScheduleBatches` is the *only* reader — so nobody can
steal them between the `len()` and the receives, and `n` receives are guaranteed
not to block. No `select`/`default`, no labels to drain the channel.

> ⚠️ This is also a **bug fix**: right now nothing drains the result channel,
> so it fills up and workers block. `ScheduleBatches` is the consumer that was
> missing.
>
> Notes:
> - The worker side (`scheduler`) already calls `resultQueue.Enqueue(*result)` —
>   no change needed there.
> - Each tick takes a *snapshot*; results enqueued mid-`SaveResults` wait for
>   the next tick. Fine as long as `buffer` comfortably exceeds per-interval
>   volume.
> - Don't range a channel to drain it (`for r := range rq.results`) — that only
>   ends when the channel is *closed*, so it would block forever here.

---

## Step 6 — Update `internal/handler/handler.go`

Give `Server` the store and add the read endpoints.

```go
package handler

import (
    "net/http"

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
```

> **Testing seam (optional, do later):** to unit-test handlers without a real
> DB, declare a small interface *in the handler package* and take that instead
> of `*database.Store`:
> ```go
> type StatusReader interface {
>     GetStatus(ctx context.Context, name string) (database.Status, error)
>     GetHistory(ctx context.Context, name string, since time.Time) ([]database.Status, error)
> }
> ```
> `*database.Store` already satisfies it, so `main.go` doesn't change.

---

## Step 7 — Wire it up in `cmd/api/main.go`

Your DB is already opened (`db`). Add the store, run schema init, start the
queue's writer loop, and inject the store into the queue and the server.

Replace the current block:

```go
srv := handler.NewServer(cfg)

jobs := make(chan scheduler.Job, 100)
client := &http.Client{}
checkerHttpClient := checker.NewChecker(client)
resultQueue := database.NewResultQueue(make(chan checker.JobResult, 1000))
```

with:

```go
// Build the single DB gateway (composition root).
store := database.NewStore(db)
if err := store.Init(ctx); err != nil {
    log.Fatalf("Failed to init schema: %v", err)
}

srv := handler.NewServer(cfg, store)

jobs := make(chan scheduler.Job, 100)
client := &http.Client{}
checkerHttpClient := checker.NewChecker(client)

// Workers Enqueue; ScheduleBatches is the single writer that flushes every 5s.
resultQueue := database.NewResultQueue(store, 1000, 5*time.Second)
go resultQueue.ScheduleBatches(ctx)
```

Then add the read route next to the health route:

```go
mux.HandleFunc("GET /healthz", srv.Health)
mux.HandleFunc("GET /status/{name}", srv.GetStatus)
```

After this, `checker.JobResult` is no longer referenced directly in `main.go`
(the channel moved into the queue), but the `checker` import stays — you still
use `checker.NewChecker`. `go build` will tell you if anything's off.

---

## Step 8 — Verify

```bash
# 1. Compiles
go build ./...

# 2. Run it
go run ./cmd/api

# 3. In another terminal, after a check interval or two:
curl localhost:8080/status/google | jq
#   → latest recorded status for the "google" target

# 4. Confirm WAL is active and rows are landing
sqlite3 uptime.db 'PRAGMA journal_mode;'          # → wal
sqlite3 uptime.db 'SELECT target, up, status_code, checked_at FROM results ORDER BY checked_at DESC LIMIT 5;'
```

Add the WAL sidecar files to `.gitignore` so they don't get committed:

```
uptime.db
uptime.db-wal
uptime.db-shm
```

---

## Checklist

- [ ] Step 1: schema decided
- [ ] Step 2: `store.go` — `Store`, `NewStore`, `Init`
- [ ] Step 3: `SaveResults` (batch tx) — the only writer
- [ ] Step 4: `GetStatus`, `GetHistory`, `Status`
- [ ] Step 5: `queue.go` — owns channel + `Store`, adds `ScheduleBatches`
- [ ] Step 6: `handler.go` — `Server` takes `*Store`, adds `GetStatus`
- [ ] Step 7: `main.go` — `NewStore` + `Init` + `go resultQueue.ScheduleBatches(ctx)` + route
- [ ] Step 8: `go build ./...`, run, curl, check rows + `.gitignore`
```

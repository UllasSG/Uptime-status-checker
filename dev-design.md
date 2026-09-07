# Uptime Status Checker — Design

Date: 2026-09-06 · Requirements: [`req.md`](./req.md)

## 1. Purpose

One long-running Go process. It polls configured HTTP(S) endpoints on a schedule, records every check durably, serves current status and history over HTTP, and logs a notification on each up↔down transition.

## 2. Design decisions

| Decision | Choice | Why |
|----------|--------|-----|
| Persistence | one row **per check** | history (4.4), uptime % (4.3), and time-window views all become SQL queries |
| Write path | worker → buffered channel → one writer goroutine → `INSERT` | durable, off the check hot-path, no external infra |
| Database | **SQLite** via `modernc.org/sqlite`, WAL mode | embedded, single-file, survives restart; pure-Go ⇒ static binary |
| Alerting | **log-only** behind a `Notifier` interface | service is internal; detection is complete, delivery swaps in later |
| Config | **JSON** file | targets + settings change without recompiling |

Kafka/NATS rejected: breaks single-machine constraint, far over scale (~100 targets ≈ a few writes/sec), duplicates SQLite's durability.

## 3. Architecture

```
config.json ──► Config Loader ──► []Target
                                     │
              per-target Ticker (Scheduler) ──► Job ──► [bounded queue] ──► Worker Pool (N)
                                                                                 │ runs
                                                                             Checker (HTTP client)
                                                                                 │ Result
                                        ┌────────────────────────────────────────┴─────────┐
                                        ▼                                                    ▼
                                 State Store (in-mem)                                DB Writer goroutine
                             current up/down, consec fails,                        resultCh ─► INSERT/check
                             last change, recent ring buffer                            │
                                        │ emits StateChange event                       ▼
                                        ▼                                       SQLite (uptime.db, WAL)
                                    Notifier (log v1)                          + Retention Pruner

              HTTP API Server ──reads──► State Store (current)  +  SQLite (history / uptime %)
```

Data flows one way. Each box talks to the next through a channel or interface, so each is tested alone.

## 4. Components

**Config Loader** — `Load(path) (Config, error)`
Parses and validates `config.json`, returning one error that lists every problem (4.1).
Rules: `name` unique + non-empty, `url` valid absolute, `interval_secs>0`, `timeout_secs>0`, `expected_status` non-empty, `failure_threshold>=1`, `workers>=1`, `retention_hrs>0`.
`timeout_secs` may exceed `interval_secs` — overlapping ticks are skipped (see Scheduler).

**Scheduler** — `Start(ctx)`
One `time.Ticker` goroutine per target; each tick enqueues `Job{Target}`.
If that target's previous check is still running, the tick is skipped and logged — bounds the queue.

**Job queue** — bounded `chan Job`
Bounded so a burst can't grow memory without limit.

**Worker pool** — `Start(ctx, jobs)`
`N` workers, one job each; concurrency capped at `N` no matter how many targets.
Per-job `recover()` turns a panic into a logged failed check, never a crash.

**Checker** — `Check(ctx, Target) Result`
One request with a per-target `context.WithTimeout`. Outcome is classified:
- `up` — reached, status ∈ `expected_status`
- `down_status` — reached, status ∉ `expected_status`
- `down_unreachable` — connection/DNS/TLS error, no response
- `down_timeout` — deadline exceeded, request abandoned (4.2)

Shares one `*http.Client`.

**State Store** — `Apply(Result) (*StateChange)` · `Snapshot()` · `SnapshotOne(name)`
Single source of truth for current status: state, consecutive fails, last check/result, last-change time, recent-results ring buffer.
Applies the failure threshold — down only after N consecutive fails (4.5) — and emits `StateChange` on a real transition.
Mutex-guarded, so a read never sees a half-updated target.

**DB Writer** — `Enqueue(Result)` · `Run(ctx)` · `Close()`
Owns the SQLite connection; one goroutine ranges `resultCh` doing one `INSERT` per result.
WAL + `synchronous=NORMAL`. Crash loss = unflushed `resultCh` (~ms); graceful `Close()` drains fully ⇒ zero loss (4.6).

**Retention Pruner** — `Run(ctx)`
Periodic `DELETE FROM results WHERE started_at < now - retention` to bound storage (4.4).

**Notifier** — `Notify(StateChange)`
Interface; v1 = `LogNotifier`, logging target, new state, time, and reason (4.5).
Delivery failures are logged, never interrupt checking.

**HTTP API Server** — `http.Server`
Reads State Store (current) and SQLite (history). See §7.

**Main / Lifecycle**
Loads config, opens + migrates DB, wires components, launches goroutines, handles signals. See §6.

## 5. Data model

### 5.1 Config (JSON)

```json
{
  "workers": 8,
  "db_path": "uptime.db",
  "retention_hrs": 720,
  "targets": [
    {
      "name": "example-api",
      "url": "https://api.example.com/health",
      "interval_secs": 30,
      "timeout_secs": 5,
      "expected_status": [200],
      "failure_threshold": 3
    }
  ]
}
```

`interval_secs`, `timeout_secs`, and `retention_hrs` are plain integers (seconds, seconds, and hours respectively). `expected_status` defaults to `[200]`, `failure_threshold` to `3`.

The process takes two CLI flags, not config fields: `-config` (path to this JSON file) and `-addr` (HTTP listen address, e.g. `:8080`). The listen address is an operational concern, so it lives on the flag rather than in the file.

### 5.2 Result (one per check → one row)

| Field | Type | Notes |
|-------|------|-------|
| `target` | text | target name |
| `started_at` | integer (unix ms) | when the check began |
| `outcome` | text | `up` \| `down_status` \| `down_unreachable` \| `down_timeout` |
| `status_code` | integer, nullable | present when a response arrived |
| `duration_ms` | integer | time taken |
| `error` | text, nullable | failure detail |

### 5.3 SQLite schema

```sql
CREATE TABLE IF NOT EXISTS results (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  target      TEXT    NOT NULL,
  started_at  INTEGER NOT NULL,      -- unix ms
  outcome     TEXT    NOT NULL,
  status_code INTEGER,
  duration_ms INTEGER NOT NULL,
  error       TEXT
);
CREATE INDEX IF NOT EXISTS idx_results_target_time ON results (target, started_at);
```

**Uptime %:** `SELECT COUNT(*) FILTER (WHERE outcome='up') * 100.0 / COUNT(*) FROM results WHERE target=? AND started_at >= ?`

**History:** `SELECT ... FROM results WHERE target=? ORDER BY started_at DESC LIMIT ?`

## 6. Concurrency & lifecycle

**Ownership** — State Store is the only writer of current status; DB Writer is the only writer of SQLite. No mutable state has two writers.

**Startup** — load config → open + migrate DB → build components → start DB Writer, workers, pruner, HTTP server → start tickers last.

**Restart** — history is durable, served from SQLite at once (acceptance 6). In-memory state starts `unknown`, repopulates on each target's first check.

**Shutdown** (`SIGINT`/`SIGTERM`) — cancel context → tickers stop → workers finish or abandon within a grace timeout → drain + close `resultCh` → close DB → exit. No data loss (4.6).

**Resilience** — per-worker `recover()`; one check or notifier failure never propagates.

## 7. HTTP API

| Method | Path | Returns |
|--------|------|---------|
| GET | `/api/status` | all targets: name, state, last check, last result, last change (4.3) |
| GET | `/api/status/{name}` | one target |
| GET | `/api/history/{name}?window=24h` | recent results + uptime % over the window (4.3, 4.4) |
| GET | `/` | HTML summary page (4.3) |
| GET | `/healthz` | checker liveness |

Machine-readable responses are JSON.

## 8. Logging & observability

Structured logs for: startup, shutdown, config-load result, each state change, each failed check (with classified reason), each skipped tick. The logs alone explain why any target is down (§5, 4.6).

## 9. Testing strategy

| Component | Test |
|-----------|------|
| Config Loader | table-driven valid/invalid cases (missing url, duplicate names, …) |
| Checker | `httptest.Server` for 200 / bad code / hang / conn-refused → assert each outcome (acceptance 1, 4) |
| State Store | result sequences → assert threshold transitions + one `StateChange` each (acceptance 2, 3); run `-race` |
| DB Writer | enqueue → rows persisted; drain-on-close loses nothing (acceptance 7) |
| Integration | one unresponsive target → others keep checking (acceptance 5); restart keeps history (acceptance 6) |

## 10. Out of scope (v1)

Non-HTTP protocols; auth / multi-tenancy; network notification channels — the `Notifier` seam adds them later without touching detection.

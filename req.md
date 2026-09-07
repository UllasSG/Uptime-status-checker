# Uptime Checker — Requirements

## 1. Purpose

A long-running service that periodically checks whether a list of HTTP endpoints is reachable and healthy, records the outcome of each check, and makes the current status available to a user.

## 2. Scope

In scope: checking HTTP and HTTPS endpoints, storing check results, reporting status, notifying on status changes.

Out of scope: checking non-HTTP protocols, user accounts and authentication, a multi-tenant hosted service.

## 3. Definitions

- **Target** — an endpoint to be checked, together with its check settings.
- **Check** — a single attempt to contact a target.
- **Result** — the outcome of one check: up or down, with supporting detail.
- **Status** — the current up/down state of a target, derived from its recent results.
- **Incident** — a period during which a target is continuously down.

## 4. Functional Requirements

### 4.1 Targets

- The system must accept a list of targets at startup.
- Each target must have a URL and a human-read Each target must have a check interval.
- Each target must have a timeout after which a check is abandoned.
- Each target must define what counts as a healthy response.
- The system must reject a target list that is malformed or incomplete, and report why.

### 4.2 Checking

- The system must check each target repeatedly at its configured interval.
- Each target must be checked independently; one target's behaviour must not delay or block checks of any other target.
- A check must be abandoned once its timeout is exceeded, and recorded as a failure.
- A check must record: the target, the time the check started, whether it succeeded, the response status code where one was received, the time taken, and the reason for failure where applicable.
- The system must distinguish between different failure reasons — for example a target that could not be reached at all versus one that responded with an unexpected status.
- The system must continue checking all other targets when any individual check fails.

### 4.3 Status Reporting

- The system must expose the current status of every target over HTTP.
- The status of a target must include: its name, its current up/down state, the time of the most recent check, the result of that check, and the time the target last changed state.
- The system must expose the results in a machine-readable format.
- The system must provide a human-readable summary page.
- The system must report uptime as a percentage over a defined recent time window.

### 4.4 History

- The system must retain the results of past checks.
- Retained results must survive a restart of the service.
- The system must expose the recent resu history for a given target.
- The system must limit how long results are retained, and remove results older than that limit.

### 4.5 Alerting

- The system must notify when a target changes state from up to down, and again when it returns to up.
- A notification must not be sent for every failed check — only for a change of state.
- A target must be considered down only after a configurable number of consecutive failed checks.
- A notification must identify the target, the new state, the time of the change, and the reason for a failure.
- A failure to deliver a notification must not stop the service or interrupt checking.

### 4.6 Licycle

- The system must run continuously until it is asked to stop.
- On being asked to stop, the system must finish or abandon in-flight checks in a bounded amount of time, stop cleanly, and not leave stored data in an inconsistent state.
- The system must log its activity, including startup, shutdown, failed checks, and state changes.

## 5. Non-Functional Requirements

- **Configuration** — All targets and settings must be configurable without changing code.
- **Scale** — The system must support at least 100 targets on a single instance.
- **Accuracy** — A check must begin within a small, bounded delay of its scheduled time.
- **Resource use** — Resource consumption must remain stable over days of continuous operation; nothing may grow without bound.
- **Correctness under concurrency** — Concurrent checks and concurrent status requests must never produce a corrupted or partially-updated view of a target's s*Resilience** — An unexpected failure while checking one target must not terminate the service.
- **Observability** — It must be possible to determine, from the system's own output, why any given target is considered down.

## 6. Constraints

- The service runs as a single process on one machine.
- The service is trusted internally and is not exposed to the public internet.

## 7. Acceptance Criteria

1. Given a configured target that responds healthily, the system reports it as up.
2. Given a target that stops responding, the system reports it as down within the configured failure threshold and sends one notification.
3. Given a target that recovers, the system reports it as up again and sends one further notification.
4. Given a target that never responds within its timeout, checks are abandoned at the timeout and recorded as failures.
5. Given one unresponsive target among many, all other targets continue to be checked on schedule.
6. Given a restart, previousecorded results are still available.
7. Given a shutdown request, the process exits within a bounded time without loss of recorded results.

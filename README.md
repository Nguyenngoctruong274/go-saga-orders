# go-saga-orders

[![ci](https://github.com/Nguyenngoctruong274/go-saga-orders/actions/workflows/ci.yml/badge.svg)](https://github.com/Nguyenngoctruong274/go-saga-orders/actions/workflows/ci.yml)
![Go](https://img.shields.io/badge/Go-1.24-00ADD8?logo=go&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-4169E1?logo=postgresql&logoColor=white)
![Kafka](https://img.shields.io/badge/Kafka-Redpanda-E53935?logo=apachekafka&logoColor=white)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

An order saga across three services in Go. Placing an order has to reserve
stock in one service and take payment in another, and either both happen or
neither does. There is no distributed transaction to lean on: each service
owns its database, they talk over Kafka, and any of them can crash
mid-flight. This project implements the saga pattern that makes the order
end up consistent anyway, with compensation when a step fails, exactly-once
effects over at-least-once delivery, and a visible timeline of what happened
to every order.

**Contents:** [Why](#why) · [Architecture](#architecture) ·
[Run it](#quick-start) · [Saga lifecycle](#saga-lifecycle) ·
[Configuration](#configuration) · [Roadmap](#roadmap)

Built in public over ten days, one tested increment a day
([roadmap](#roadmap)). Day 2: the saga domain, a deterministic state
machine with fenced attempts, deadlines and compensation, fuzzed in CI.

## Why

Every e-commerce or ride-hailing backend has this problem and most solve it
by accident, with a `payment_status` column nobody trusts. The questions
this repository answers with code and tests:

- **Orchestration.** `ordersd` runs an explicit saga per order: reserve
  stock, authorize payment, confirm. Each step is a command on Kafka and a
  reply back; the orchestrator persists where every saga is before acting.
- **Compensation.** When payment is declined after stock was reserved, the
  reservation is released; when a step times out, the saga compensates
  what already happened. Compensations are commands too, and they retry.
- **Exactly-once effects.** Kafka delivers at least once. Every participant
  keeps an inbox of processed message ids in the same transaction as its
  effect, so a redelivered `reserve` reserves nothing twice.
- **Crash safety.** State changes and outgoing messages are written in one
  transaction (an outbox), then relayed; a daemon killed between the two
  resumes without losing or duplicating a command.
- **Choreography compared.** Day 9 runs the same participants in an
  event-driven choreography and measures what the orchestrator bought.
- **Visible.** Every order has a saga timeline: steps, attempts, replies,
  compensations, with timestamps; Prometheus metrics and alerts.

## Architecture

```mermaid
flowchart LR
    C[Client] -->|POST /v1/orders| O[ordersd<br/>API + saga orchestrator]
    O --> ODB[(orders DB<br/>orders · sagas · steps · outbox)]
    O -->|commands| K[(Kafka / Redpanda)]
    K -->|replies| O
    K -->|reserve · release| I[inventoryd]
    K -->|authorize · void| P[paymentd]
    I --> IDB[(inventory DB<br/>stock · reservations · inbox)]
    P --> PDB[(payment DB<br/>authorizations · inbox)]
    I -->|replies| K
    P -->|replies| K
```

Three daemons from one module and one image. They share nothing but
Kafka: each has its own PostgreSQL database, and every cross-service
effect is a message.

| Package | Responsibility |
|---|---|
| `cmd/ordersd` | Orders API and the saga orchestrator |
| `cmd/inventoryd` | Stock and reservations; answers `reserve` and `release` |
| `cmd/paymentd` | Payment authorizations; answers `authorize` and `void` |
| `internal/config` | Environment configuration with validation, shared by the three daemons |
| `internal/httpapi` | Probes (`/healthz`, `/readyz`), request ids; the `/v1` API from day 8 |
| `internal/service` | Shared lifecycle: tagged JSON logger, HTTP server, graceful shutdown that fails readiness first |
| `internal/saga` | Domain: saga definitions, the instance state machine with fenced attempts, deadlines, retries and compensation, and the timeline; no I/O |

Packages still to come, by roadmap day: `internal/message` and
`internal/kafka` (envelopes, topics, client; day 3), `internal/postgres`
and `internal/outbox` (day 4), `internal/orchestrator` (day 5),
`internal/participant` (day 6), `internal/metrics` (day 9).

## Quick start

```sh
make up              # PostgreSQL (orders, inventory, payment DBs) + Redpanda
make run-orders      # :8080
make run-inventory   # :8081, in another shell
make run-payment     # :8082, in another shell

curl -s localhost:8080/healthz   # {"status":"ok"}
curl -s localhost:8080/readyz    # {"status":"ready"}
```

`make test` runs the unit tests with the race detector; `make lint` runs
golangci-lint; `make build` puts the three binaries in `bin/`. The Docker
image holds all three daemons; pick one with the entrypoint
(`docker run --entrypoint /paymentd ...`).

### Shutdown

On `SIGTERM` or `SIGINT` a daemon fails `/readyz` immediately, keeps
serving for `SAGA_SHUTDOWN_DRAIN` so the load balancer can move traffic
away, then closes the listener and gives in-flight requests up to
`SAGA_SHUTDOWN_TIMEOUT` to finish. `/healthz` stays green throughout, so
the orchestrator does not restart a pod that is deliberately stopping.

## Saga lifecycle

`internal/saga` is the heart of the project and has no I/O: every
transition takes the current time and returns the next command to send,
and the orchestrator (day 5) stores the saga and moves the messages. That
makes the whole state machine unit-testable and fuzzable.

A **definition** is an ordered list of steps. Each step names an action
command (`inventory.reserve`), an optional compensation (`inventory.release`),
a per-attempt timeout and an attempt budget. The order saga is:

| Step | Action | Compensation | Notes |
|---|---|---|---|
| `reserve` | `inventory.reserve` | `inventory.release` | |
| `pay` | `payment.authorize` | `payment.void` | |
| `confirm` | `order.confirm` | none | the pivot: local, cannot be undone, so it goes last |

```mermaid
stateDiagram-v2
    [*] --> pending
    pending --> running: Start
    pending --> compensated: Abort
    running --> running: reply succeeded, next step<br/>reply retry / timeout, attempts left
    running --> completed: last step succeeded
    running --> compensating: reply failed · attempts exhausted · Abort
    compensating --> compensating: compensation succeeded, previous step<br/>retry / timeout, attempts left
    compensating --> compensated: nothing left to undo
    compensating --> failed: compensation refused or exhausted
    completed --> [*]
    compensated --> [*]
    failed --> [*]
```

The rules the tests and the fuzzer pin down:

- **One thing in flight.** A running saga has exactly one step running; a
  compensating saga exactly one compensation. Everything after it is
  pending (forward) or already undone (backward).
- **Fenced attempts.** Every command carries the step's attempt number,
  which increases on every send, action or compensation. A reply must
  quote the saga, the step and the live attempt; anything else is
  `ErrStaleReply` and changes nothing. A late reply to a timed-out attempt
  cannot resurrect a step.
- **Three outcomes.** `succeeded` advances. `failed` is the participant's
  final answer (out of stock, card declined): it did nothing, so the step
  is not compensated, and the saga turns around. `retry` means it did
  nothing *this time*: the command is re-sent until the budget is spent,
  then treated as failed.
- **Timeouts are not failures.** A lost reply is retried like `retry`.
  When the budget is spent, the step's outcome is *unknown*: it may have
  taken effect, so it is compensated along with the steps before it.
  Participants' compensations must therefore tolerate undoing something
  that never happened (day 6).
- **Compensations get their own budget** and the same timeout. One that is
  refused, or keeps timing out, moves the saga to `failed`: nothing more
  happens automatically, an operator inspects and replays it (day 7).
- **Pivots.** A step without a compensation stands if a later step fails;
  definitions put such steps last.
- **Abort** of a pending saga compensates it on the spot; of a running one
  marks the step in flight aborted (outcome unknown, so undone too) and
  starts compensating. A saga already compensating or finished is left
  alone.
- **Version and timeline.** Each accepted transition bumps `Version`, for
  optimistic locking when the saga is stored, and appends an event
  (`step_sent`, `step_failed`, `compensation_sent`, `compensated`, ...)
  that becomes the order's timeline in the API.

`FuzzSagaInvariants` feeds a four-step definition random replies, timeouts
and aborts, including stale attempts, wrong steps, other sagas and unknown
outcomes, and checks all of the above after every operation; CI runs it
for 20 seconds on each push.

## Configuration

All three daemons read the same variables; each deployment sets its own
database. Boolean-free and fail-fast: a misconfigured daemon lists every
invalid variable at once and exits.

| Variable | Default | Description |
|---|---|---|
| `SAGA_HTTP_ADDR` | `:8080` ordersd, `:8081` inventoryd, `:8082` paymentd | HTTP listen address |
| `SAGA_DATABASE_URL` | required | `postgres://` URL of this daemon's own database |
| `SAGA_DB_MAX_CONNS` | `20` | Maximum open database connections |
| `SAGA_KAFKA_BROKERS` | required | Comma-separated bootstrap brokers, `host:port` |
| `SAGA_LOG_LEVEL` | `info` | `debug`, `info`, `warn` or `error` |
| `SAGA_SHUTDOWN_TIMEOUT` | `15s` | Time for in-flight requests to finish after the listener closes |
| `SAGA_SHUTDOWN_DRAIN` | `0s` | Time to keep serving with `/readyz` failing before the listener closes |

## Roadmap

- [x] **Day 1**: Scaffold: three daemons, config, probes, slog, graceful shutdown, Docker, compose with PostgreSQL and Redpanda, CI
- [x] **Day 2**: Saga domain: steps with action and compensation, state machine with fenced transitions, deadlines; fuzzing
- [ ] **Day 3**: Message contracts: versioned JSON envelopes with saga, correlation and causation ids; topics and headers; Kafka client over franz-go, tested against an in-memory broker
- [ ] **Day 4**: Schema and store: orders, sagas, steps, outbox; saga creation and step transitions in one transaction; outbox relay to Kafka, ordered per saga
- [ ] **Day 5**: Orchestrator: consume replies, advance or compensate, inbox for idempotent replies, recovery of in-flight sagas after a crash
- [ ] **Day 6**: Participants: inventoryd (reserve, release) and paymentd (authorize, void) with inbox deduplication and fault injection; end-to-end happy path and compensation
- [ ] **Day 7**: Timeouts and retries: step deadlines, backoff for retriable replies, timeout triggers compensation, dead-lettered sagas, operator replay
- [ ] **Day 8**: API: `POST /v1/orders`, `GET /v1/orders/{id}` with the saga timeline, `GET /v1/sagas`, abort and replay; end-to-end tests over Kafka and PostgreSQL
- [ ] **Day 9**: Choreography mode with the same participants; orchestration vs choreography compared; metrics and alerts
- [ ] **Day 10**: Docker stack (three daemons, Redpanda, PostgreSQL, Prometheus), benchmarks, design decisions, v1.0.0

## License

MIT, see [LICENSE](LICENSE).

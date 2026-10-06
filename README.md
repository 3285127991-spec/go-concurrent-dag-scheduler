# dagscheduler

A concurrency-safe, in-memory DAG task scheduler written in Go. Backend
library only, plus a minimal CLI demo and tests. No third-party
dependencies.

## Features

- Tasks with unique non-empty string IDs, `Pending` -> `Completed` lifecycle.
- `AddDependency(task, dependsOn)`: `task` waits for `dependsOn`. Both tasks
  must exist; self-dependencies, duplicates and cycles are rejected, and a
  failed call leaves the graph unchanged.
- Cycle detection and the dependency write are one atomic operation under a
  single `sync.RWMutex`, so concurrent use cannot create cycles or
  inconsistent state.
- `Ready()` returns Pending tasks whose direct dependencies are all
  Completed, sorted lexicographically by ID.
- `Snapshot()` returns a stable copy of tasks, statuses and dependencies,
  fully isolated from internal state.

## API

| Method | Description |
| --- | --- |
| `New() *Scheduler` | Create an empty scheduler. |
| `AddTask(id string) error` | Add a Pending task; fails on empty or duplicate ID. |
| `AddDependency(task, dependsOn string) error` | Add an edge; fails on missing tasks, self-dependency, duplicate, or cycle. |
| `Complete(id string) error` | Complete a Ready task; fails with `ErrNotReady` / `ErrAlreadyCompleted`. |
| `Get(id string) (Task, bool)` | Copy of one task (ID, status, sorted dependencies). |
| `Ready() []string` | IDs of ready tasks, sorted. |
| `Snapshot() []Task` | Stable, isolated copy of all tasks, sorted by ID. |
| `Len() int` | Number of tasks. |

Errors are sentinel values (`ErrTaskNotFound`, `ErrCycle`, ...) usable with
`errors.Is`.

## Layout

- `scheduler.go` - the library (package `dagscheduler`).
- `scheduler_test.go` - unit tests plus a basic concurrency test.
- `cmd/demo/main.go` - minimal CLI demo.

## Run

```sh
go run ./cmd/demo      # build graph, Ready, reject cycle, complete chain
go test ./...          # unit tests
go test -race ./...    # verify concurrency safety
```

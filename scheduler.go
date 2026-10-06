// Package dagscheduler provides a concurrency-safe, in-memory DAG task
// scheduler. Tasks are identified by unique non-empty string IDs and form a
// directed acyclic graph through explicit dependencies.
package dagscheduler

import (
	"errors"
	"fmt"
	"sort"
	"sync"
)

// Status describes the lifecycle state of a task.
type Status string

const (
	// Pending means the task has been added but not yet completed.
	Pending Status = "Pending"
	// Completed means the task has been completed via Complete.
	Completed Status = "Completed"
)

var (
	// ErrEmptyID is returned when a task ID is empty.
	ErrEmptyID = errors.New("dagscheduler: task ID must not be empty")
	// ErrTaskExists is returned when adding a task with a duplicate ID.
	ErrTaskExists = errors.New("dagscheduler: task already exists")
	// ErrTaskNotFound is returned when referencing an unknown task ID.
	ErrTaskNotFound = errors.New("dagscheduler: task not found")
	// ErrSelfDependency is returned when a task would depend on itself.
	ErrSelfDependency = errors.New("dagscheduler: task cannot depend on itself")
	// ErrDependencyExists is returned when the dependency already exists.
	ErrDependencyExists = errors.New("dagscheduler: dependency already exists")
	// ErrCycle is returned when a dependency would introduce a cycle.
	ErrCycle = errors.New("dagscheduler: dependency would create a cycle")
	// ErrNotReady is returned when completing a task with unfinished dependencies.
	ErrNotReady = errors.New("dagscheduler: task has unfinished dependencies")
	// ErrAlreadyCompleted is returned when completing a completed task.
	ErrAlreadyCompleted = errors.New("dagscheduler: task already completed")
)

// TaskInfo is an immutable view of a single task.
type TaskInfo struct {
	ID        string
	Status    Status
	DependsOn []string // sorted IDs of direct dependencies
}

// Snapshot is a stable deep copy of the scheduler state, isolated from the
// scheduler it was taken from.
type Snapshot struct {
	Tasks []TaskInfo // sorted by ID
}

type task struct {
	status Status
	deps   map[string]struct{}
}

// Scheduler is a concurrency-safe in-memory DAG of tasks.
type Scheduler struct {
	mu    sync.RWMutex
	tasks map[string]*task
}

// NewScheduler returns an empty Scheduler.
func NewScheduler() *Scheduler {
	return &Scheduler{tasks: make(map[string]*task)}
}

// AddTask registers a new Pending task with the given unique non-empty ID.
func (s *Scheduler) AddTask(id string) error {
	if id == "" {
		return ErrEmptyID
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.tasks[id]; ok {
		return fmt.Errorf("%w: %q", ErrTaskExists, id)
	}
	s.tasks[id] = &task{status: Pending, deps: make(map[string]struct{})}
	return nil
}

// AddDependency records that the task identified by taskID must wait for the
// task identified by dependsOn to complete. Both tasks must exist; self
// dependencies, duplicate dependencies and cycles are rejected. On failure
// the graph is left unchanged.
func (s *Scheduler) AddDependency(taskID, dependsOn string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.tasks[taskID]
	if !ok {
		return fmt.Errorf("%w: %q", ErrTaskNotFound, taskID)
	}
	if _, ok := s.tasks[dependsOn]; !ok {
		return fmt.Errorf("%w: %q", ErrTaskNotFound, dependsOn)
	}
	if taskID == dependsOn {
		return ErrSelfDependency
	}
	if _, ok := t.deps[dependsOn]; ok {
		return fmt.Errorf("%w: %q -> %q", ErrDependencyExists, taskID, dependsOn)
	}
	if s.reachesLocked(dependsOn, taskID) {
		return fmt.Errorf("%w: %q -> %q", ErrCycle, taskID, dependsOn)
	}
	t.deps[dependsOn] = struct{}{}
	return nil
}

// reachesLocked reports whether following dependency edges from "from" can
// reach "to". It must be called with s.mu held, so cycle detection and the
// dependency write happen as one atomic operation.
func (s *Scheduler) reachesLocked(from, to string) bool {
	visited := make(map[string]struct{})
	stack := []string{from}
	for len(stack) > 0 {
		id := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if id == to {
			return true
		}
		if _, ok := visited[id]; ok {
			continue
		}
		visited[id] = struct{}{}
		for dep := range s.tasks[id].deps {
			stack = append(stack, dep)
		}
	}
	return false
}

// Complete marks a Ready task as Completed. It fails with ErrNotReady if any
// direct dependency is not yet Completed, and with ErrAlreadyCompleted if the
// task was already completed.
func (s *Scheduler) Complete(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.tasks[id]
	if !ok {
		return fmt.Errorf("%w: %q", ErrTaskNotFound, id)
	}
	if t.status == Completed {
		return fmt.Errorf("%w: %q", ErrAlreadyCompleted, id)
	}
	for dep := range t.deps {
		if s.tasks[dep].status != Completed {
			return fmt.Errorf("%w: %q waits on %q", ErrNotReady, id, dep)
		}
	}
	t.status = Completed
	return nil
}

// Get returns an immutable view of the task with the given ID.
func (s *Scheduler) Get(id string) (TaskInfo, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	t, ok := s.tasks[id]
	if !ok {
		return TaskInfo{}, false
	}
	return TaskInfo{ID: id, Status: t.status, DependsOn: sortedKeys(t.deps)}, true
}

// Ready returns the sorted IDs of all Pending tasks whose direct dependencies
// are all Completed.
func (s *Scheduler) Ready() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ready := make([]string, 0, len(s.tasks))
	for id, t := range s.tasks {
		if t.status == Pending && s.readyLocked(t) {
			ready = append(ready, id)
		}
	}
	sort.Strings(ready)
	return ready
}

// readyLocked reports whether every direct dependency of t is Completed.
func (s *Scheduler) readyLocked(t *task) bool {
	for dep := range t.deps {
		if s.tasks[dep].status != Completed {
			return false
		}
	}
	return true
}

// Snapshot returns a stable deep copy of all tasks, their statuses and their
// dependencies. Mutating the snapshot does not affect the scheduler, and
// later scheduler changes do not affect the snapshot.
func (s *Scheduler) Snapshot() Snapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	snap := Snapshot{Tasks: make([]TaskInfo, 0, len(s.tasks))}
	for id, t := range s.tasks {
		snap.Tasks = append(snap.Tasks, TaskInfo{
			ID:        id,
			Status:    t.status,
			DependsOn: sortedKeys(t.deps),
		})
	}
	sort.Slice(snap.Tasks, func(i, j int) bool { return snap.Tasks[i].ID < snap.Tasks[j].ID })
	return snap
}

// Len returns the number of tasks in the scheduler.
func (s *Scheduler) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.tasks)
}

func sortedKeys(m map[string]struct{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

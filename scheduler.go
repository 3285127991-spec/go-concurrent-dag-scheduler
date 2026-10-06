// Package dagscheduler provides a concurrency-safe, in-memory DAG task
// scheduler. Tasks are identified by unique non-empty string IDs and move
// from Pending to Completed. A task becomes Ready when it is Pending and
// all of its direct dependencies have completed.
package dagscheduler

import (
	"errors"
	"sort"
	"sync"
)

// Status describes the lifecycle state of a task.
type Status string

const (
	StatusPending   Status = "Pending"
	StatusCompleted Status = "Completed"
)

var (
	ErrEmptyID             = errors.New("dagscheduler: task id must not be empty")
	ErrTaskExists          = errors.New("dagscheduler: task already exists")
	ErrTaskNotFound        = errors.New("dagscheduler: task not found")
	ErrSelfDependency      = errors.New("dagscheduler: task cannot depend on itself")
	ErrDuplicateDependency = errors.New("dagscheduler: dependency already exists")
	ErrCycle               = errors.New("dagscheduler: dependency would create a cycle")
	ErrNotReady            = errors.New("dagscheduler: task is not ready")
	ErrAlreadyCompleted    = errors.New("dagscheduler: task is already completed")
)

// Task is a stable copy of a single task and its direct dependencies.
type Task struct {
	ID        string
	Status    Status
	DependsOn []string
}

type node struct {
	status    Status
	dependsOn map[string]struct{}
}

// Scheduler is a concurrency-safe in-memory DAG of tasks.
// The zero value is not usable; call New.
type Scheduler struct {
	mu    sync.RWMutex
	tasks map[string]*node
}

// New returns an empty Scheduler.
func New() *Scheduler {
	return &Scheduler{tasks: make(map[string]*node)}
}

// AddTask registers a new Pending task with a unique non-empty id.
func (s *Scheduler) AddTask(id string) error {
	if id == "" {
		return ErrEmptyID
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.tasks[id]; ok {
		return ErrTaskExists
	}
	s.tasks[id] = &node{status: StatusPending, dependsOn: make(map[string]struct{})}
	return nil
}

// AddDependency records that task must wait for dependsOn to complete.
// Both tasks must exist; self-dependencies, duplicates and cycles are
// rejected. The cycle check and the write happen atomically under the
// write lock, so a failed call leaves the graph unchanged.
func (s *Scheduler) AddDependency(task, dependsOn string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	taskNode, ok := s.tasks[task]
	if !ok {
		return ErrTaskNotFound
	}
	if _, ok := s.tasks[dependsOn]; !ok {
		return ErrTaskNotFound
	}
	if task == dependsOn {
		return ErrSelfDependency
	}
	if _, ok := taskNode.dependsOn[dependsOn]; ok {
		return ErrDuplicateDependency
	}
	if s.reaches(dependsOn, task) {
		return ErrCycle
	}
	taskNode.dependsOn[dependsOn] = struct{}{}
	return nil
}

// reaches reports whether target is reachable from start by following
// dependency edges. The caller must hold the lock.
func (s *Scheduler) reaches(start, target string) bool {
	visited := make(map[string]struct{})
	stack := []string{start}
	for len(stack) > 0 {
		cur := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if cur == target {
			return true
		}
		if _, ok := visited[cur]; ok {
			continue
		}
		visited[cur] = struct{}{}
		for dep := range s.tasks[cur].dependsOn {
			stack = append(stack, dep)
		}
	}
	return false
}

// Complete marks a Ready task as Completed. It fails with ErrNotReady if
// any direct dependency is not yet completed, and with
// ErrAlreadyCompleted on repeated completion.
func (s *Scheduler) Complete(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	n, ok := s.tasks[id]
	if !ok {
		return ErrTaskNotFound
	}
	if n.status == StatusCompleted {
		return ErrAlreadyCompleted
	}
	if !s.depsCompleted(n) {
		return ErrNotReady
	}
	n.status = StatusCompleted
	return nil
}

// Get returns a copy of the task with the given id.
func (s *Scheduler) Get(id string) (Task, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	n, ok := s.tasks[id]
	if !ok {
		return Task{}, false
	}
	return Task{ID: id, Status: n.status, DependsOn: sortedDeps(n)}, true
}

// Ready returns the IDs of Pending tasks whose direct dependencies are all
// Completed, sorted lexicographically.
func (s *Scheduler) Ready() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ready := make([]string, 0)
	for id, n := range s.tasks {
		if n.status == StatusPending && s.depsCompleted(n) {
			ready = append(ready, id)
		}
	}
	sort.Strings(ready)
	return ready
}

// Snapshot returns a stable copy of all tasks, sorted by ID. The result is
// fully detached from internal state and safe to mutate.
func (s *Scheduler) Snapshot() []Task {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Task, 0, len(s.tasks))
	for id, n := range s.tasks {
		out = append(out, Task{ID: id, Status: n.status, DependsOn: sortedDeps(n)})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// Len returns the number of tasks in the scheduler.
func (s *Scheduler) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.tasks)
}

// depsCompleted reports whether every direct dependency of n is Completed.
// The caller must hold the lock.
func (s *Scheduler) depsCompleted(n *node) bool {
	for dep := range n.dependsOn {
		if s.tasks[dep].status != StatusCompleted {
			return false
		}
	}
	return true
}

// sortedDeps returns the dependency IDs of n in lexicographic order.
// The caller must hold the lock.
func sortedDeps(n *node) []string {
	deps := make([]string, 0, len(n.dependsOn))
	for dep := range n.dependsOn {
		deps = append(deps, dep)
	}
	sort.Strings(deps)
	return deps
}

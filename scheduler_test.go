package dagscheduler

import (
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
)

func mustAdd(t *testing.T, s *Scheduler, ids ...string) {
	t.Helper()
	for _, id := range ids {
		if err := s.AddTask(id); err != nil {
			t.Fatalf("AddTask(%q): %v", id, err)
		}
	}
}

func mustDep(t *testing.T, s *Scheduler, task, dependsOn string) {
	t.Helper()
	if err := s.AddDependency(task, dependsOn); err != nil {
		t.Fatalf("AddDependency(%q, %q): %v", task, dependsOn, err)
	}
}

func TestAddTaskAndGet(t *testing.T) {
	s := New()
	mustAdd(t, s, "a")
	if got := s.Len(); got != 1 {
		t.Fatalf("Len() = %d, want 1", got)
	}
	task, ok := s.Get("a")
	if !ok {
		t.Fatal("Get(a) not found")
	}
	if task.ID != "a" || task.Status != StatusPending || len(task.DependsOn) != 0 {
		t.Fatalf("unexpected task: %+v", task)
	}
	if _, ok := s.Get("missing"); ok {
		t.Fatal("Get(missing) should report not found")
	}
}

func TestAddTaskRejectsEmptyAndDuplicate(t *testing.T) {
	s := New()
	if err := s.AddTask(""); !errors.Is(err, ErrEmptyID) {
		t.Fatalf("AddTask(empty) = %v, want ErrEmptyID", err)
	}
	mustAdd(t, s, "a")
	if err := s.AddTask("a"); !errors.Is(err, ErrTaskExists) {
		t.Fatalf("duplicate AddTask = %v, want ErrTaskExists", err)
	}
	if got := s.Len(); got != 1 {
		t.Fatalf("Len() = %d, want 1", got)
	}
}

func TestAddDependency(t *testing.T) {
	s := New()
	mustAdd(t, s, "a", "b")
	mustDep(t, s, "b", "a")
	task, _ := s.Get("b")
	if !reflect.DeepEqual(task.DependsOn, []string{"a"}) {
		t.Fatalf("DependsOn = %v, want [a]", task.DependsOn)
	}
	if got := s.Ready(); !reflect.DeepEqual(got, []string{"a"}) {
		t.Fatalf("Ready() = %v, want [a]", got)
	}
}

func TestAddDependencyErrors(t *testing.T) {
	s := New()
	mustAdd(t, s, "a", "b")
	if err := s.AddDependency("a", "missing"); !errors.Is(err, ErrTaskNotFound) {
		t.Fatalf("AddDependency(a, missing) = %v, want ErrTaskNotFound", err)
	}
	if err := s.AddDependency("missing", "a"); !errors.Is(err, ErrTaskNotFound) {
		t.Fatalf("AddDependency(missing, a) = %v, want ErrTaskNotFound", err)
	}
	if err := s.AddDependency("a", "a"); !errors.Is(err, ErrSelfDependency) {
		t.Fatalf("AddDependency(a, a) = %v, want ErrSelfDependency", err)
	}
	mustDep(t, s, "b", "a")
	if err := s.AddDependency("b", "a"); !errors.Is(err, ErrDuplicateDependency) {
		t.Fatalf("duplicate AddDependency = %v, want ErrDuplicateDependency", err)
	}
	if got := s.Len(); got != 2 {
		t.Fatalf("Len() = %d, want 2", got)
	}
}

func TestReadyOrder(t *testing.T) {
	s := New()
	mustAdd(t, s, "delta", "alpha", "charlie", "bravo")
	mustDep(t, s, "alpha", "bravo")
	want := []string{"bravo", "charlie", "delta"}
	if got := s.Ready(); !reflect.DeepEqual(got, want) {
		t.Fatalf("Ready() = %v, want %v", got, want)
	}
}

func TestComplete(t *testing.T) {
	s := New()
	mustAdd(t, s, "a", "b")
	mustDep(t, s, "b", "a")
	if err := s.Complete("a"); err != nil {
		t.Fatalf("Complete(a): %v", err)
	}
	task, _ := s.Get("a")
	if task.Status != StatusCompleted {
		t.Fatalf("status = %v, want %v", task.Status, StatusCompleted)
	}
	if got := s.Ready(); !reflect.DeepEqual(got, []string{"b"}) {
		t.Fatalf("Ready() = %v, want [b]", got)
	}
}

func TestCompleteRejectsNotReadyAndDuplicate(t *testing.T) {
	s := New()
	mustAdd(t, s, "a", "b")
	mustDep(t, s, "b", "a")
	if err := s.Complete("b"); !errors.Is(err, ErrNotReady) {
		t.Fatalf("Complete(b) = %v, want ErrNotReady", err)
	}
	if err := s.Complete("missing"); !errors.Is(err, ErrTaskNotFound) {
		t.Fatalf("Complete(missing) = %v, want ErrTaskNotFound", err)
	}
	if err := s.Complete("a"); err != nil {
		t.Fatalf("Complete(a): %v", err)
	}
	if err := s.Complete("a"); !errors.Is(err, ErrAlreadyCompleted) {
		t.Fatalf("repeated Complete(a) = %v, want ErrAlreadyCompleted", err)
	}
}

func TestCycleDetectionKeepsGraphUnchanged(t *testing.T) {
	s := New()
	mustAdd(t, s, "a", "b", "c")
	mustDep(t, s, "b", "a")
	mustDep(t, s, "c", "b")

	if err := s.AddDependency("a", "c"); !errors.Is(err, ErrCycle) {
		t.Fatalf("AddDependency(a, c) = %v, want ErrCycle", err)
	}
	if err := s.AddDependency("a", "b"); !errors.Is(err, ErrCycle) {
		t.Fatalf("AddDependency(a, b) = %v, want ErrCycle", err)
	}

	if got := s.Len(); got != 3 {
		t.Fatalf("Len() = %d, want 3", got)
	}
	if got := s.Ready(); !reflect.DeepEqual(got, []string{"a"}) {
		t.Fatalf("Ready() = %v, want [a]", got)
	}
	a, _ := s.Get("a")
	if len(a.DependsOn) != 0 {
		t.Fatalf("a gained dependencies after failed calls: %v", a.DependsOn)
	}
}

func TestSnapshotIsolation(t *testing.T) {
	s := New()
	mustAdd(t, s, "b", "a")
	mustDep(t, s, "b", "a")

	snap := s.Snapshot()
	if len(snap) != 2 || snap[0].ID != "a" || snap[1].ID != "b" {
		t.Fatalf("Snapshot() not sorted by ID: %v", snap)
	}

	snap[0].Status = StatusCompleted
	snap[1].DependsOn[0] = "mutated"
	snap[1].DependsOn = append(snap[1].DependsOn, "extra")

	a, _ := s.Get("a")
	if a.Status != StatusPending {
		t.Fatalf("internal status mutated: %v", a.Status)
	}
	b, _ := s.Get("b")
	if !reflect.DeepEqual(b.DependsOn, []string{"a"}) {
		t.Fatalf("internal dependencies mutated: %v", b.DependsOn)
	}
}

func TestConcurrentMixedOps(t *testing.T) {
	s := New()
	const workers = 8
	const perWorker = 20

	var wg sync.WaitGroup
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func(w int) {
			defer wg.Done()
			prev := ""
			for i := 0; i < perWorker; i++ {
				id := fmt.Sprintf("w%d-t%02d", w, i)
				if err := s.AddTask(id); err != nil {
					t.Errorf("AddTask(%q): %v", id, err)
				}
				if prev != "" {
					if err := s.AddDependency(id, prev); err != nil {
						t.Errorf("AddDependency(%q, %q): %v", id, prev, err)
					}
					if err := s.Complete(prev); err != nil {
						t.Errorf("Complete(%q): %v", prev, err)
					}
				}
				_ = s.Ready()
				_ = s.Snapshot()
				_ = s.Len()
				prev = id
			}
			if err := s.Complete(prev); err != nil {
				t.Errorf("Complete(%q): %v", prev, err)
			}
		}(w)
	}
	wg.Wait()

	if got := s.Len(); got != workers*perWorker {
		t.Fatalf("Len() = %d, want %d", got, workers*perWorker)
	}
	if got := s.Ready(); len(got) != 0 {
		t.Fatalf("Ready() = %v, want empty", got)
	}
}

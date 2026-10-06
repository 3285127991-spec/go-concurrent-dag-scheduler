package dagscheduler

import (
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
)

func mustAddTasks(t *testing.T, s *Scheduler, ids ...string) {
	t.Helper()
	for _, id := range ids {
		if err := s.AddTask(id); err != nil {
			t.Fatalf("AddTask(%q): %v", id, err)
		}
	}
}

func mustAddDep(t *testing.T, s *Scheduler, taskID, dependsOn string) {
	t.Helper()
	if err := s.AddDependency(taskID, dependsOn); err != nil {
		t.Fatalf("AddDependency(%q, %q): %v", taskID, dependsOn, err)
	}
}

func TestAddTaskAndGet(t *testing.T) {
	s := NewScheduler()
	if err := s.AddTask("a"); err != nil {
		t.Fatalf("AddTask: %v", err)
	}
	if got := s.Len(); got != 1 {
		t.Fatalf("Len = %d, want 1", got)
	}
	info, ok := s.Get("a")
	if !ok {
		t.Fatal("Get(a) not found")
	}
	if info.Status != Pending {
		t.Errorf("Status = %q, want %q", info.Status, Pending)
	}
	if len(info.DependsOn) != 0 {
		t.Errorf("DependsOn = %v, want empty", info.DependsOn)
	}
	if _, ok := s.Get("missing"); ok {
		t.Error("Get(missing) = true, want false")
	}
}

func TestAddTaskInvalidID(t *testing.T) {
	s := NewScheduler()
	if err := s.AddTask(""); !errors.Is(err, ErrEmptyID) {
		t.Errorf("AddTask(empty) = %v, want ErrEmptyID", err)
	}
	mustAddTasks(t, s, "a")
	if err := s.AddTask("a"); !errors.Is(err, ErrTaskExists) {
		t.Errorf("AddTask(dup) = %v, want ErrTaskExists", err)
	}
	if got := s.Len(); got != 1 {
		t.Errorf("Len = %d, want 1", got)
	}
}

func TestAddDependency(t *testing.T) {
	s := NewScheduler()
	mustAddTasks(t, s, "a", "b")
	mustAddDep(t, s, "b", "a")

	info, _ := s.Get("b")
	if want := []string{"a"}; !reflect.DeepEqual(info.DependsOn, want) {
		t.Errorf("DependsOn = %v, want %v", info.DependsOn, want)
	}
	if got, want := s.Ready(), []string{"a"}; !reflect.DeepEqual(got, want) {
		t.Errorf("Ready = %v, want %v", got, want)
	}
}

func TestAddDependencyUnknownTask(t *testing.T) {
	s := NewScheduler()
	mustAddTasks(t, s, "a", "b")

	if err := s.AddDependency("ghost", "a"); !errors.Is(err, ErrTaskNotFound) {
		t.Errorf("AddDependency(ghost, a) = %v, want ErrTaskNotFound", err)
	}
	if err := s.AddDependency("a", "ghost"); !errors.Is(err, ErrTaskNotFound) {
		t.Errorf("AddDependency(a, ghost) = %v, want ErrTaskNotFound", err)
	}
	if got := s.Len(); got != 2 {
		t.Errorf("Len = %d, want 2", got)
	}
	if got, want := s.Ready(), []string{"a", "b"}; !reflect.DeepEqual(got, want) {
		t.Errorf("Ready = %v, want %v", got, want)
	}
}

func TestSelfAndDuplicateDependency(t *testing.T) {
	s := NewScheduler()
	mustAddTasks(t, s, "a", "b")

	if err := s.AddDependency("a", "a"); !errors.Is(err, ErrSelfDependency) {
		t.Errorf("AddDependency(a, a) = %v, want ErrSelfDependency", err)
	}
	mustAddDep(t, s, "b", "a")
	if err := s.AddDependency("b", "a"); !errors.Is(err, ErrDependencyExists) {
		t.Errorf("AddDependency(b, a) again = %v, want ErrDependencyExists", err)
	}
	info, _ := s.Get("b")
	if want := []string{"a"}; !reflect.DeepEqual(info.DependsOn, want) {
		t.Errorf("DependsOn = %v, want %v", info.DependsOn, want)
	}
}

func TestCycleDetection(t *testing.T) {
	s := NewScheduler()
	mustAddTasks(t, s, "a", "b", "c")
	mustAddDep(t, s, "b", "a")
	mustAddDep(t, s, "c", "b")

	// Direct 2-cycle.
	if err := s.AddDependency("a", "b"); !errors.Is(err, ErrCycle) {
		t.Errorf("AddDependency(a, b) = %v, want ErrCycle", err)
	}
	// Indirect 3-cycle.
	if err := s.AddDependency("a", "c"); !errors.Is(err, ErrCycle) {
		t.Errorf("AddDependency(a, c) = %v, want ErrCycle", err)
	}

	// The failed attempts must not have modified the graph.
	if got := s.Len(); got != 3 {
		t.Errorf("Len = %d, want 3", got)
	}
	info, _ := s.Get("a")
	if len(info.DependsOn) != 0 {
		t.Errorf("a.DependsOn = %v, want empty", info.DependsOn)
	}
	if got, want := s.Ready(), []string{"a"}; !reflect.DeepEqual(got, want) {
		t.Errorf("Ready = %v, want %v", got, want)
	}
}

func TestReadyOrder(t *testing.T) {
	s := NewScheduler()
	mustAddTasks(t, s, "delta", "alpha", "charlie", "bravo")

	want := []string{"alpha", "bravo", "charlie", "delta"}
	if got := s.Ready(); !reflect.DeepEqual(got, want) {
		t.Errorf("Ready = %v, want %v", got, want)
	}
	if err := s.Complete("bravo"); err != nil {
		t.Fatalf("Complete(bravo): %v", err)
	}
	want = []string{"alpha", "charlie", "delta"}
	if got := s.Ready(); !reflect.DeepEqual(got, want) {
		t.Errorf("Ready = %v, want %v", got, want)
	}
}

func TestCompleteUnblocksDependents(t *testing.T) {
	s := NewScheduler()
	mustAddTasks(t, s, "a", "b")
	mustAddDep(t, s, "b", "a")

	if err := s.Complete("a"); err != nil {
		t.Fatalf("Complete(a): %v", err)
	}
	info, _ := s.Get("a")
	if info.Status != Completed {
		t.Errorf("a.Status = %q, want %q", info.Status, Completed)
	}
	if got, want := s.Ready(), []string{"b"}; !reflect.DeepEqual(got, want) {
		t.Errorf("Ready = %v, want %v", got, want)
	}
	if err := s.Complete("b"); err != nil {
		t.Fatalf("Complete(b): %v", err)
	}
	if got := s.Ready(); len(got) != 0 {
		t.Errorf("Ready = %v, want empty", got)
	}
}

func TestCompleteNotReady(t *testing.T) {
	s := NewScheduler()
	mustAddTasks(t, s, "a", "b")
	mustAddDep(t, s, "b", "a")

	if err := s.Complete("b"); !errors.Is(err, ErrNotReady) {
		t.Errorf("Complete(b) = %v, want ErrNotReady", err)
	}
	info, _ := s.Get("b")
	if info.Status != Pending {
		t.Errorf("b.Status = %q, want %q", info.Status, Pending)
	}
}

func TestCompleteDuplicateAndMissing(t *testing.T) {
	s := NewScheduler()
	mustAddTasks(t, s, "a")

	if err := s.Complete("a"); err != nil {
		t.Fatalf("Complete(a): %v", err)
	}
	if err := s.Complete("a"); !errors.Is(err, ErrAlreadyCompleted) {
		t.Errorf("Complete(a) again = %v, want ErrAlreadyCompleted", err)
	}
	if err := s.Complete("ghost"); !errors.Is(err, ErrTaskNotFound) {
		t.Errorf("Complete(ghost) = %v, want ErrTaskNotFound", err)
	}
}

func TestSnapshotIsolation(t *testing.T) {
	s := NewScheduler()
	mustAddTasks(t, s, "a", "b", "c")
	mustAddDep(t, s, "b", "a")
	if err := s.Complete("a"); err != nil {
		t.Fatalf("Complete(a): %v", err)
	}

	snap := s.Snapshot()
	want := Snapshot{Tasks: []TaskInfo{
		{ID: "a", Status: Completed, DependsOn: []string{}},
		{ID: "b", Status: Pending, DependsOn: []string{"a"}},
		{ID: "c", Status: Pending, DependsOn: []string{}},
	}}
	if !reflect.DeepEqual(snap, want) {
		t.Fatalf("Snapshot = %+v, want %+v", snap, want)
	}

	// Mutating the snapshot must not affect the scheduler.
	snap.Tasks[0].Status = Pending
	snap.Tasks[1].DependsOn[0] = "hacked"
	if info, _ := s.Get("a"); info.Status != Completed {
		t.Errorf("a.Status = %q, want %q", info.Status, Completed)
	}
	if info, _ := s.Get("b"); !reflect.DeepEqual(info.DependsOn, []string{"a"}) {
		t.Errorf("b.DependsOn = %v, want [a]", info.DependsOn)
	}

	// Later scheduler changes must not leak into the snapshot.
	mustAddTasks(t, s, "d")
	if err := s.Complete("b"); err != nil {
		t.Fatalf("Complete(b): %v", err)
	}
	if len(snap.Tasks) != 3 {
		t.Errorf("snapshot has %d tasks, want 3", len(snap.Tasks))
	}
	if snap.Tasks[1].Status != Pending {
		t.Errorf("snapshot b.Status = %q, want %q", snap.Tasks[1].Status, Pending)
	}
}

func TestConcurrentAccess(t *testing.T) {
	s := NewScheduler()
	if err := s.AddTask("root"); err != nil {
		t.Fatalf("AddTask(root): %v", err)
	}

	const workers = 16
	ids := make([]string, workers)
	for i := range ids {
		ids[i] = fmt.Sprintf("task-%02d", i)
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 3*workers)
	run := func(fn func() error) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := fn(); err != nil {
				errCh <- err
			}
		}()
	}

	// Concurrently add tasks, then concurrently attach them to root.
	for _, id := range ids {
		id := id
		run(func() error { return s.AddTask(id) })
	}
	wg.Wait()
	for _, id := range ids {
		id := id
		run(func() error { return s.AddDependency(id, "root") })
	}
	wg.Wait()

	if err := s.Complete("root"); err != nil {
		t.Fatalf("Complete(root): %v", err)
	}

	// Concurrently complete tasks while readers query the scheduler.
	for _, id := range ids {
		id := id
		run(func() error { return s.Complete(id) })
	}
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 25; j++ {
				_ = s.Ready()
				_ = s.Len()
				_, _ = s.Get("root")
				_ = s.Snapshot()
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Errorf("concurrent operation failed: %v", err)
	}

	if got, want := s.Len(), workers+1; got != want {
		t.Errorf("Len = %d, want %d", got, want)
	}
	if got := s.Ready(); len(got) != 0 {
		t.Errorf("Ready = %v, want empty", got)
	}
	for _, id := range ids {
		if info, ok := s.Get(id); !ok || info.Status != Completed {
			t.Errorf("Get(%q) = %+v, %v; want Completed", id, info, ok)
		}
	}
}

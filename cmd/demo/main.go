// Command demo is a minimal walkthrough of the dagscheduler library using a
// small built-in build pipeline: fetch -> build -> {test, package} -> deploy.
package main

import (
	"fmt"
	"log"

	"dagscheduler"
)

func main() {
	s := dagscheduler.NewScheduler()

	// Build the graph.
	must(s.AddTask("fetch"))
	must(s.AddTask("build"))
	must(s.AddTask("test"))
	must(s.AddTask("package"))
	must(s.AddTask("deploy"))
	must(s.AddDependency("build", "fetch"))
	must(s.AddDependency("test", "build"))
	must(s.AddDependency("package", "build"))
	must(s.AddDependency("deploy", "test"))
	must(s.AddDependency("deploy", "package"))
	fmt.Printf("graph built: %d tasks, ready=%v\n", s.Len(), s.Ready())

	// An illegal cycle is rejected and the graph stays unchanged.
	if err := s.AddDependency("fetch", "deploy"); err != nil {
		fmt.Printf("cycle rejected: %v (ready still %v)\n", err, s.Ready())
	}

	// Complete tasks as they become ready.
	complete(s, "fetch")
	complete(s, "build")
	complete(s, "test")
	complete(s, "package")
	complete(s, "deploy")

	// Final state.
	fmt.Println("final snapshot:")
	for _, task := range s.Snapshot().Tasks {
		fmt.Printf("  %-8s %-9s dependsOn=%v\n", task.ID, task.Status, task.DependsOn)
	}
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func complete(s *dagscheduler.Scheduler, id string) {
	if err := s.Complete(id); err != nil {
		log.Fatalf("complete %s: %v", id, err)
	}
	fmt.Printf("completed %-8s ready=%v\n", id, s.Ready())
}

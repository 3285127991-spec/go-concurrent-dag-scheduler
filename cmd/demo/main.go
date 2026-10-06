// Command demo is a minimal walkthrough of the dagscheduler library:
// build a graph, inspect Ready tasks, reject a cycle, and complete the
// chain so downstream tasks become ready.
package main

import (
	"fmt"

	"dagscheduler"
)

func main() {
	s := dagscheduler.New()

	must(s.AddTask("fetch"))
	must(s.AddTask("parse"))
	must(s.AddTask("analyze"))
	must(s.AddTask("report"))
	must(s.AddDependency("parse", "fetch"))
	must(s.AddDependency("analyze", "parse"))
	must(s.AddDependency("report", "analyze"))
	fmt.Println("built graph: fetch -> parse -> analyze -> report, tasks:", s.Len())

	fmt.Println("ready:", s.Ready())

	if err := s.AddDependency("fetch", "report"); err != nil {
		fmt.Println("cycle rejected:", err)
	}
	if err := s.Complete("report"); err != nil {
		fmt.Println("not-ready rejected:", err)
	}

	must(s.Complete("fetch"))
	fmt.Println("completed fetch,   ready:", s.Ready())
	must(s.Complete("parse"))
	fmt.Println("completed parse,   ready:", s.Ready())
	must(s.Complete("analyze"))
	fmt.Println("completed analyze, ready:", s.Ready())
	must(s.Complete("report"))
	fmt.Println("completed report,  ready:", s.Ready())

	fmt.Println("snapshot:", s.Snapshot())
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

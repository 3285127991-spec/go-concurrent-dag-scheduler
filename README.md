# dagscheduler

并发安全的内存 DAG 任务调度器（纯 Go 标准库，无第三方依赖）。包含后端库、最小 CLI 演示和自动化测试。

## 功能
- 任务以唯一非空字符串 ID 标识，初始状态为 `Pending`，可经 `Complete` 变为 `Completed`。
- `AddDependency(task, dependsOn)` 表示 `task` 必须等待 `dependsOn` 完成；两个任务必须已存在，禁止自依赖、重复依赖和成环，失败时原图保持不变。
- `Ready()` 返回所有直接依赖均已完成的 `Pending` 任务 ID，按字典序稳定排序。
- `Complete` 只能完成 Ready 任务，重复完成或未就绪返回明确错误。
- `Snapshot()` 返回任务、状态和依赖的稳定深拷贝，与内部状态完全隔离。
- 单个 `sync.RWMutex` 保护全部状态，环检测与依赖写入在同一临界区内原子完成，并发下不会产生环或不一致状态。

## API
```go
s := dagscheduler.NewScheduler()
err := s.AddTask("build")               // 注册 Pending 任务
err = s.AddDependency("build", "fetch") // build 等待 fetch 完成
info, ok := s.Get("build")              // 查询单个任务（副本）
ready := s.Ready()                      // 可立即完成的任务 ID，字典序
err = s.Complete("fetch")               // 完成 Ready 任务
snap := s.Snapshot()                    // 稳定快照，与内部状态隔离
n := s.Len()                            // 任务总数
```

错误通过哨兵错误值区分，可用 `errors.Is` 判断：`ErrEmptyID`、`ErrTaskExists`、`ErrTaskNotFound`、`ErrSelfDependency`、`ErrDependencyExists`、`ErrCycle`、`ErrNotReady`、`ErrAlreadyCompleted`。

## 运行演示
```
go run ./cmd/demo
```
演示建图、Ready 查询、非法成环被拒绝，以及依次完成依赖后后续任务变为 Ready 的全过程。

## 运行测试
```
go test -race ./...
```

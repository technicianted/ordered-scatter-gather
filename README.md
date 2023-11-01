Ordered Scatter-Gather
---

A simple implementation of a scatter-gather pattern that preserves order of scattering. It guarantees that gathered results will always be read in the same order they were added regardless of parallel excution times. Additionally:

* Optionally restricting maximum input backlog work.
* Optionally restricting maximum output backlog.

### Usage

```go
// create a new ordered scatter-gather with 4 workers
// and unlimited backlogs for a work function that
// returns an int.
osg := 	NewOrderedScatterGather[int](4, 0, 0)
defer sg.Drain()
// add some work
// if backlog had a constraint, call will block until
// backlog space is available.
sg.AddWork(context.Background(), func() int {
    return 123
})
// get results
r := <-sg.OutputChannel()
```

# Contributing

Thank you for your interest!

All types of contributions are encouraged and valued.


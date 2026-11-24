package trailbeamsqs

import "sync/atomic"

// The process's call-order counter (contract section 2a). One per process: every goroutine shares it, so
// "A returned before B was issued" holds across goroutines, as the contract argues for threads.
var order = struct {
	scope string
	next  atomic.Int64
}{scope: newUUID()}

// tick returns the next value of the counter: strictly increasing and safe from any goroutine.
func tick() int64 { return order.next.Add(1) }

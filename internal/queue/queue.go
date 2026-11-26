// Package queue is a system-agnostic workload for append-only logs:
// producers append unique values, readers poll, and the checker decides
// whether the log kept its promises.
package queue

import (
	"context"
	"errors"
)

// Node identifies one replica a client can talk to.
type Node struct {
	Name string
	URL  string
}

// Record is one entry of the log as observed by a reader.
type Record struct {
	Offset uint64
	Value  string
}

// ErrRejected marks an error the system guarantees left no effect.
// Clients wrap definite failures with it; every other error is treated as
// indeterminate, because the write may have committed before the reply was lost.
var ErrRejected = errors.New("rejected")

// Client is what a target system must implement to be tested.
type Client interface {
	// Append writes value and returns the offset the system acknowledged.
	Append(ctx context.Context, node Node, value string) (uint64, error)
	// Read returns up to count records starting at offset from node's view.
	Read(ctx context.Context, node Node, offset uint64, count int) ([]Record, error)
}

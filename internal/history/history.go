// Package history records what every client and the nemesis did, in order,
// so a checker can judge the run after the fact.
package history

import (
	"encoding/json"
	"os"
	"sync"
	"time"
)

// Kind is the outcome phase of an operation.
type Kind string

const (
	Invoke Kind = "invoke"
	// Ok means the system acknowledged the operation.
	Ok Kind = "ok"
	// Fail means the operation definitely did not take effect.
	Fail Kind = "fail"
	// Info means the outcome is unknown: it may or may not have taken effect.
	Info Kind = "info"
)

// Op is one line of history.
type Op struct {
	Index   int           `json:"index"`
	Time    time.Duration `json:"time_ns"`
	Process string        `json:"process"`
	Kind    Kind          `json:"kind"`
	F       string        `json:"f"`
	Node    string        `json:"node,omitempty"`
	Value   string        `json:"value,omitempty"`
	Offset  *uint64       `json:"offset,omitempty"`
	Count   int           `json:"count,omitempty"`
	Error   string        `json:"error,omitempty"`
}

// Recorder is safe for concurrent use.
type Recorder struct {
	mu    sync.Mutex
	start time.Time
	ops   []Op
}

func NewRecorder() *Recorder {
	return &Recorder{start: time.Now()}
}

// Append stamps the op with its index and elapsed time and stores it.
func (r *Recorder) Append(op Op) Op {
	r.mu.Lock()
	defer r.mu.Unlock()
	op.Index = len(r.ops)
	op.Time = time.Since(r.start)
	r.ops = append(r.ops, op)
	return op
}

// Ops returns a copy of the history so far.
func (r *Recorder) Ops() []Op {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]Op(nil), r.ops...)
}

// WriteJSONL writes one op per line, the format Jepsen-style tooling expects.
func (r *Recorder) WriteJSONL(path string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(f)
	for _, op := range r.Ops() {
		if err := enc.Encode(op); err != nil {
			f.Close()
			return err
		}
	}
	return f.Close()
}

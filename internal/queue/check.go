package queue

import (
	"fmt"
	"sort"

	"github.com/aashish-thapa/faultline/internal/history"
)

// AnomalyKind names a broken promise. Each kind is a separate claim a log makes.
type AnomalyKind string

const (
	// LostWrite: the system acknowledged an append that no final log contains.
	LostWrite AnomalyKind = "lost-write"
	// AckOffsetMismatch: the acknowledged offset holds a different value.
	AckOffsetMismatch AnomalyKind = "ack-offset-mismatch"
	// Duplicate: one append appears at more than one offset.
	Duplicate AnomalyKind = "duplicate"
	// Phantom: a value appears that was never attempted, or whose append definitely failed.
	Phantom AnomalyKind = "phantom"
	// DirtyRead: a reader saw a record at an offset the final log disagrees with.
	DirtyRead AnomalyKind = "dirty-read"
	// Divergence: two nodes' final logs hold different values at the same offset.
	Divergence AnomalyKind = "divergence"
)

type Anomaly struct {
	Kind   AnomalyKind `json:"kind"`
	Value  string      `json:"value,omitempty"`
	Offset *uint64     `json:"offset,omitempty"`
	Detail string      `json:"detail"`
}

type Summary struct {
	Attempted     int            `json:"attempted"`
	Acknowledged  int            `json:"acknowledged"`
	Failed        int            `json:"failed"`
	Indeterminate int            `json:"indeterminate"`
	FinalLength   map[string]int `json:"final_length"`
	Reads         int            `json:"reads"`
}

type Result struct {
	Valid     bool                `json:"valid"`
	Summary   Summary             `json:"summary"`
	Anomalies []Anomaly           `json:"anomalies"`
	Counts    map[AnomalyKind]int `json:"counts"`
}

// Check compares what clients were told against what the nodes ended up storing.
func Check(ops []history.Op, observed []Observation, finals map[string][]Record) Result {
	var res Result
	res.Counts = map[AnomalyKind]int{}
	add := func(a Anomaly) {
		res.Anomalies = append(res.Anomalies, a)
		res.Counts[a.Kind]++
	}

	acked := map[string]uint64{}
	outcome := map[string]history.Kind{}
	for _, op := range ops {
		if op.F != "append" {
			continue
		}
		switch op.Kind {
		case history.Invoke:
			res.Summary.Attempted++
		case history.Ok:
			res.Summary.Acknowledged++
			acked[op.Value] = *op.Offset
		case history.Fail:
			res.Summary.Failed++
		case history.Info:
			res.Summary.Indeterminate++
		}
		if op.Kind != history.Invoke {
			outcome[op.Value] = op.Kind
		}
	}

	canonical, divergences := mergeFinals(finals)
	for _, a := range divergences {
		add(a)
	}
	res.Summary.FinalLength = map[string]int{}
	for node, log := range finals {
		res.Summary.FinalLength[node] = len(log)
	}

	positions := map[string][]uint64{}
	for offset, value := range canonical {
		positions[value] = append(positions[value], offset)
	}
	for value, offs := range positions {
		sort.Slice(offs, func(i, j int) bool { return offs[i] < offs[j] })
		if len(offs) > 1 {
			add(Anomaly{Kind: Duplicate, Value: value, Detail: fmt.Sprintf("stored at offsets %v", offs)})
		}
		kind, attempted := outcome[value]
		switch {
		case !attempted:
			add(Anomaly{Kind: Phantom, Value: value, Detail: "value was never sent by any producer"})
		case kind == history.Fail:
			add(Anomaly{Kind: Phantom, Value: value, Offset: &offs[0], Detail: "append returned a definite failure but the value was stored"})
		}
	}

	for value, offset := range acked {
		stored, ok := canonical[offset]
		switch {
		case len(positions[value]) == 0:
			add(Anomaly{Kind: LostWrite, Value: value, Offset: ptr(offset), Detail: "acknowledged but missing from every node's final log"})
		case !ok || stored != value:
			add(Anomaly{Kind: AckOffsetMismatch, Value: value, Offset: ptr(offset),
				Detail: fmt.Sprintf("acknowledged at offset %d, final log holds %q there; value found at %v", offset, stored, positions[value])})
		}
	}

	for _, obs := range observed {
		res.Summary.Reads++
		for _, r := range obs.Records {
			if stored, ok := canonical[r.Offset]; !ok || stored != r.Value {
				add(Anomaly{Kind: DirtyRead, Value: r.Value, Offset: ptr(r.Offset),
					Detail: fmt.Sprintf("%s returned %q at offset %d; final log holds %q", obs.Node, r.Value, r.Offset, stored)})
			}
		}
	}

	sort.SliceStable(res.Anomalies, func(i, j int) bool { return res.Anomalies[i].Kind < res.Anomalies[j].Kind })
	res.Valid = len(res.Anomalies) == 0
	return res
}

// mergeFinals unions every node's log by offset and reports offsets where nodes disagree.
// A shorter log is not divergence: a lagging replica may simply not have caught up.
func mergeFinals(finals map[string][]Record) (map[uint64]string, []Anomaly) {
	canonical := map[uint64]string{}
	holder := map[uint64]string{}
	var anomalies []Anomaly
	nodes := make([]string, 0, len(finals))
	for n := range finals {
		nodes = append(nodes, n)
	}
	sort.Strings(nodes)
	for _, node := range nodes {
		for _, r := range finals[node] {
			prev, seen := canonical[r.Offset]
			if !seen {
				canonical[r.Offset], holder[r.Offset] = r.Value, node
				continue
			}
			if prev != r.Value {
				anomalies = append(anomalies, Anomaly{Kind: Divergence, Offset: ptr(r.Offset),
					Detail: fmt.Sprintf("%s holds %q, %s holds %q", holder[r.Offset], prev, node, r.Value)})
			}
		}
	}
	return canonical, anomalies
}

func ptr(v uint64) *uint64 { return &v }

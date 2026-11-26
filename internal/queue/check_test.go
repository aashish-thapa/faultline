package queue

import (
	"testing"

	"github.com/aashish-thapa/faultline/internal/history"
)

func appendOps(value string, kind history.Kind, offset uint64) []history.Op {
	done := history.Op{F: "append", Kind: kind, Value: value}
	if kind == history.Ok {
		done.Offset = &offset
	}
	return []history.Op{{F: "append", Kind: history.Invoke, Value: value}, done}
}

func ops(groups ...[]history.Op) []history.Op {
	var out []history.Op
	for _, g := range groups {
		out = append(out, g...)
	}
	return out
}

func TestCheck(t *testing.T) {
	tests := []struct {
		name     string
		history  []history.Op
		observed []Observation
		finals   map[string][]Record
		want     map[AnomalyKind]int
	}{
		{
			name:    "clean run is valid",
			history: ops(appendOps("a", history.Ok, 0), appendOps("b", history.Ok, 1)),
			finals:  map[string][]Record{"n0": {{0, "a"}, {1, "b"}}, "n1": {{0, "a"}}},
			want:    map[AnomalyKind]int{},
		},
		{
			name:    "indeterminate write may or may not appear",
			history: ops(appendOps("a", history.Ok, 0), appendOps("b", history.Info, 0)),
			finals:  map[string][]Record{"n0": {{0, "a"}}},
			want:    map[AnomalyKind]int{},
		},
		{
			name:    "acknowledged write missing everywhere is lost",
			history: ops(appendOps("a", history.Ok, 0), appendOps("b", history.Ok, 1)),
			finals:  map[string][]Record{"n0": {{0, "a"}}, "n1": {{0, "a"}}},
			want:    map[AnomalyKind]int{LostWrite: 1},
		},
		{
			name:    "acknowledged offset reused by another value",
			history: ops(appendOps("a", history.Ok, 0), appendOps("b", history.Ok, 0)),
			finals:  map[string][]Record{"n0": {{0, "a"}, {1, "b"}}},
			want:    map[AnomalyKind]int{AckOffsetMismatch: 1},
		},
		{
			name:    "definitely failed write that got stored is a phantom",
			history: ops(appendOps("a", history.Fail, 0)),
			finals:  map[string][]Record{"n0": {{0, "a"}}},
			want:    map[AnomalyKind]int{Phantom: 1},
		},
		{
			name:    "value stored twice is a duplicate",
			history: ops(appendOps("a", history.Ok, 0)),
			finals:  map[string][]Record{"n0": {{0, "a"}, {1, "a"}}},
			want:    map[AnomalyKind]int{Duplicate: 1},
		},
		{
			name:    "nodes disagreeing at an offset diverge",
			history: ops(appendOps("a", history.Info, 0), appendOps("b", history.Info, 0)),
			finals:  map[string][]Record{"n0": {{0, "a"}}, "n1": {{0, "b"}}},
			want:    map[AnomalyKind]int{Divergence: 1},
		},
		{
			name:     "read of a record the final log lacks is dirty",
			history:  ops(appendOps("a", history.Info, 0)),
			observed: []Observation{{Node: "n0", Records: []Record{{0, "a"}}}},
			finals:   map[string][]Record{"n0": {}},
			want:     map[AnomalyKind]int{DirtyRead: 1},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Check(tt.history, tt.observed, tt.finals)
			if len(got.Counts) != len(tt.want) {
				t.Fatalf("counts = %v, want %v; anomalies %+v", got.Counts, tt.want, got.Anomalies)
			}
			for kind, n := range tt.want {
				if got.Counts[kind] != n {
					t.Errorf("%s = %d, want %d; anomalies %+v", kind, got.Counts[kind], n, got.Anomalies)
				}
			}
			if got.Valid != (len(tt.want) == 0) {
				t.Errorf("valid = %v", got.Valid)
			}
		})
	}
}

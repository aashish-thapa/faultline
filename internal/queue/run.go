package queue

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/aashish-thapa/faultline/internal/history"
)

// Config shapes the load generated during the run.
type Config struct {
	Producers int
	Readers   int
	// OpDelay is the pause each worker takes between operations.
	OpDelay   time.Duration
	OpTimeout time.Duration
	ReadBatch int
}

// Observation is a read that returned during the run, kept for the checker.
type Observation struct {
	Node    string
	Records []Record
}

// Generate runs producers and readers against random nodes until ctx ends.
func Generate(ctx context.Context, cfg Config, client Client, nodes []Node, rec *history.Recorder) []Observation {
	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		observed []Observation
	)
	for p := range cfg.Producers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			produce(ctx, cfg, client, nodes, rec, fmt.Sprintf("producer-%d", p))
		}()
	}
	for r := range cfg.Readers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			obs := consume(ctx, cfg, client, nodes, rec, fmt.Sprintf("reader-%d", r))
			mu.Lock()
			observed = append(observed, obs...)
			mu.Unlock()
		}()
	}
	wg.Wait()
	return observed
}

func produce(ctx context.Context, cfg Config, client Client, nodes []Node, rec *history.Recorder, process string) {
	for seq := 0; ctx.Err() == nil; seq++ {
		node := nodes[rand.IntN(len(nodes))]
		value := fmt.Sprintf("%s/%d", process, seq)
		rec.Append(history.Op{Process: process, Kind: history.Invoke, F: "append", Node: node.Name, Value: value})

		opCtx, cancel := context.WithTimeout(context.Background(), cfg.OpTimeout)
		offset, err := client.Append(opCtx, node, value)
		cancel()

		done := history.Op{Process: process, F: "append", Node: node.Name, Value: value}
		switch {
		case err == nil:
			done.Kind, done.Offset = history.Ok, &offset
		case errors.Is(err, ErrRejected):
			done.Kind, done.Error = history.Fail, err.Error()
		default:
			done.Kind, done.Error = history.Info, err.Error()
		}
		rec.Append(done)
		sleep(ctx, cfg.OpDelay)
	}
}

func consume(ctx context.Context, cfg Config, client Client, nodes []Node, rec *history.Recorder, process string) []Observation {
	var (
		observed []Observation
		next     uint64
	)
	for ctx.Err() == nil {
		node := nodes[rand.IntN(len(nodes))]
		rec.Append(history.Op{Process: process, Kind: history.Invoke, F: "read", Node: node.Name, Offset: &next})

		opCtx, cancel := context.WithTimeout(context.Background(), cfg.OpTimeout)
		records, err := client.Read(opCtx, node, next, cfg.ReadBatch)
		cancel()

		done := history.Op{Process: process, F: "read", Node: node.Name, Offset: &next}
		if err != nil {
			done.Kind, done.Error = history.Fail, err.Error()
		} else {
			done.Kind, done.Count = history.Ok, len(records)
			observed = append(observed, Observation{Node: node.Name, Records: records})
			if n := len(records); n > 0 {
				next = records[n-1].Offset + 1
			}
		}
		rec.Append(done)
		sleep(ctx, cfg.OpDelay)
	}
	return observed
}

// ReadAll pages through node's entire log.
func ReadAll(ctx context.Context, client Client, node Node, batch int) ([]Record, error) {
	var (
		all  []Record
		next uint64
	)
	for {
		records, err := client.Read(ctx, node, next, batch)
		if err != nil {
			return all, fmt.Errorf("read %s at offset %d: %w", node.Name, next, err)
		}
		if len(records) == 0 {
			return all, nil
		}
		all = append(all, records...)
		next = records[len(records)-1].Offset + 1
	}
}

func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}

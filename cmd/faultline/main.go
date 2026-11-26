// Command faultline runs a fault-injection test against a 3-node Apache Iggy cluster
// and checks the resulting history for broken guarantees.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/aashish-thapa/faultline/internal/docker"
	"github.com/aashish-thapa/faultline/internal/history"
	"github.com/aashish-thapa/faultline/internal/nemesis"
	"github.com/aashish-thapa/faultline/internal/queue"
)

type options struct {
	duration   time.Duration
	healthy    time.Duration
	faulty     time.Duration
	recovery   time.Duration
	fault      string
	durability string
	producers  int
	readers    int
	opDelay    time.Duration
	outDir     string
	producer   string
	driver     string
	attempt    time.Duration
	target     string
}

func main() {
	var o options
	flag.DurationVar(&o.duration, "duration", 60*time.Second, "how long to generate load")
	flag.DurationVar(&o.healthy, "healthy", 10*time.Second, "time between faults")
	flag.DurationVar(&o.faulty, "faulty", 10*time.Second, "how long each fault lasts")
	flag.DurationVar(&o.recovery, "recovery", 20*time.Second, "time to let the cluster settle before final reads")
	flag.StringVar(&o.fault, "nemesis", "partition", "none | kill | kill-majority | pause | partition | bridge | storage | mix")
	flag.StringVar(&o.durability, "durability", "replicated", "replicated | persisted")
	flag.IntVar(&o.producers, "producers", 5, "concurrent producers")
	flag.IntVar(&o.readers, "readers", 2, "concurrent readers")
	flag.DurationVar(&o.opDelay, "op-delay", 20*time.Millisecond, "pause between each worker's operations")
	flag.StringVar(&o.outDir, "out", "runs", "directory for run artifacts")
	flag.StringVar(&o.target, "target", "iggy", "iggy | automq")
	flag.StringVar(&o.producer, "producer", "http", "iggy: http | idempotent; kafka: idempotent | plain")
	flag.DurationVar(&o.attempt, "attempt-timeout", 10*time.Second, "idempotent producer: per-send timeout before resending from a fresh session")
	flag.StringVar(&o.driver, "driver", "drivers/iggy-producer/target/release/iggy-producer", "idempotent producer driver binary")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	if err := run(context.Background(), o, logger); err != nil {
		logger.Error("run failed", "error", err)
		os.Exit(1)
	}
}

const sidecarImage = "faultline/netns"

func pickNemesis(name string, t target) (nemesis.Nemesis, error) {
	switch name {
	case "none":
		return nemesis.None{}, nil
	case "kill":
		return nemesis.Kill{}, nil
	case "kill-majority":
		return nemesis.KillMajority{}, nil
	case "pause":
		return nemesis.Pause{}, nil
	case "partition":
		return nemesis.Partition{SidecarImage: sidecarImage}, nil
	case "bridge":
		return nemesis.Bridge{SidecarImage: sidecarImage}, nil
	case "mix":
		mix := nemesis.Mix{nemesis.Kill{}, nemesis.Pause{}, nemesis.Partition{SidecarImage: sidecarImage}, nemesis.Bridge{SidecarImage: sidecarImage}}
		if t.storage != nil {
			mix = append(mix, cutStorage(t))
		}
		return mix, nil
	case "storage":
		if t.storage == nil {
			return nil, fmt.Errorf("target has no shared storage to cut")
		}
		return cutStorage(t), nil
	}
	return nil, fmt.Errorf("unknown nemesis %q", name)
}

func cutStorage(t target) nemesis.Nemesis {
	return nemesis.CutDependency{SidecarImage: sidecarImage, Dependency: t.storage.name, DependencyIP: t.storage.ip}
}

func run(ctx context.Context, o options, logger *slog.Logger) error {
	t, ok := targets[o.target]
	if !ok {
		return fmt.Errorf("unknown target %q", o.target)
	}
	nem, err := pickNemesis(o.fault, t)
	if err != nil {
		return err
	}
	runID := fmt.Sprintf("%s-%s-%s-%s-%s", time.Now().UTC().Format("20060102T150405Z"), o.target, o.fault, o.durability, o.producer)
	dir := filepath.Join(o.outDir, runID)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}

	sess, err := t.open(ctx, o, runID, t.nodes[0], logger)
	if err != nil {
		return err
	}
	defer sess.close()
	logger.Info("topic ready, generating load", "run", runID, "nemesis", nem.Name(), "durability", o.durability)

	rec := history.NewRecorder()
	loadCtx, stop := context.WithTimeout(ctx, o.duration)
	defer stop()
	nemesisDone := make(chan struct{})
	go func() {
		nemesis.Schedule(loadCtx, nem, t.members, o.healthy, o.faulty, rec)
		close(nemesisDone)
	}()
	observed := queue.Generate(loadCtx, queue.Config{
		Producers: o.producers, Readers: o.readers, OpDelay: o.opDelay, OpTimeout: 10 * time.Second, ReadBatch: 100,
	}, sess.client, t.nodes, rec)
	<-nemesisDone

	restoreCluster(ctx, t.members, logger)
	logger.Info("load finished, letting cluster recover", "wait", o.recovery)
	time.Sleep(o.recovery)

	finals := map[string][]queue.Record{}
	for _, node := range t.nodes {
		log, err := readAllWithRetry(ctx, sess.final, node)
		if err != nil {
			logger.Warn("final read incomplete", "node", node.Name, "error", err)
		}
		finals[node.Name] = log
	}

	result := queue.Check(rec.Ops(), observed, finals)
	if err := rec.WriteJSONL(filepath.Join(dir, "history.jsonl")); err != nil {
		return err
	}
	if err := writeJSON(filepath.Join(dir, "result.json"), result); err != nil {
		return err
	}
	printSummary(result, dir)
	return nil
}

// restoreCluster undoes anything a crashed heal may have left behind.
func restoreCluster(ctx context.Context, members []nemesis.Member, logger *slog.Logger) {
	for _, m := range members {
		if state, err := docker.Run(ctx, "inspect", "-f", "{{.State.Status}}", m.Container); err == nil {
			switch state {
			case "paused":
				_, err = docker.Run(ctx, "unpause", m.Container)
			case "exited", "created":
				_, err = docker.Run(ctx, "start", m.Container)
			}
			if err != nil {
				logger.Warn("restore failed", "container", m.Container, "error", err)
			}
		}
		if _, err := docker.InNetns(ctx, sidecarImage, m.Container, "iptables", "-F", "INPUT"); err != nil {
			logger.Warn("flush iptables failed", "container", m.Container, "error", err)
		}
	}
}

func readAllWithRetry(ctx context.Context, client queue.Client, node queue.Node) ([]queue.Record, error) {
	var lastErr error
	for range 10 {
		readCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
		log, err := queue.ReadAll(readCtx, client, node, 1000)
		cancel()
		if err == nil {
			return log, nil
		}
		lastErr = err
		time.Sleep(3 * time.Second)
	}
	return nil, lastErr
}

func writeJSON(path string, v any) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

func printSummary(r queue.Result, dir string) {
	s := r.Summary
	fmt.Printf("\nattempted %d, acknowledged %d, failed %d, indeterminate %d, reads %d\n",
		s.Attempted, s.Acknowledged, s.Failed, s.Indeterminate, s.Reads)
	names := make([]string, 0, len(s.FinalLength))
	for n := range s.FinalLength {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		fmt.Printf("  final log %s: %d records\n", n, s.FinalLength[n])
	}
	if r.Valid {
		fmt.Printf("VALID: no anomalies. artifacts in %s\n", dir)
		return
	}
	fmt.Printf("INVALID: %v\n", r.Counts)
	for i, a := range r.Anomalies {
		if i == 10 {
			fmt.Printf("  ... %d more in %s/result.json\n", len(r.Anomalies)-10, dir)
			break
		}
		fmt.Printf("  %s %s: %s\n", a.Kind, a.Value, a.Detail)
	}
}

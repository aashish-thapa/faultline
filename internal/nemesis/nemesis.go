// Package nemesis injects faults into a running cluster.
package nemesis

import (
	"context"
	"fmt"
	"math/rand/v2"
	"strings"
	"time"

	"github.com/aashish-thapa/faultline/internal/docker"
	"github.com/aashish-thapa/faultline/internal/history"
)

// Member is one cluster replica as the nemesis sees it.
type Member struct {
	Container string
	// PeerIP is the address other replicas use to reach this one.
	PeerIP string
}

// Fault is an injected failure that can be undone.
type Fault interface {
	Describe() string
	Heal(ctx context.Context) error
}

// Nemesis decides which fault to inject next.
type Nemesis interface {
	Name() string
	Inject(ctx context.Context, members []Member) (Fault, error)
}

// Schedule alternates healthy and faulty periods until ctx ends, then heals.
func Schedule(ctx context.Context, n Nemesis, members []Member, healthy, faulty time.Duration, rec *history.Recorder) {
	for {
		if !wait(ctx, healthy) {
			return
		}
		fault, err := n.Inject(context.Background(), members)
		if err != nil {
			rec.Append(history.Op{Process: "nemesis", Kind: history.Fail, F: n.Name(), Error: err.Error()})
			continue
		}
		rec.Append(history.Op{Process: "nemesis", Kind: history.Ok, F: "start-" + n.Name(), Value: fault.Describe()})
		wait(ctx, faulty)
		if err := fault.Heal(context.Background()); err != nil {
			rec.Append(history.Op{Process: "nemesis", Kind: history.Fail, F: "heal-" + n.Name(), Error: err.Error()})
		} else {
			rec.Append(history.Op{Process: "nemesis", Kind: history.Ok, F: "heal-" + n.Name(), Value: fault.Describe()})
		}
		if ctx.Err() != nil {
			return
		}
	}
}

func wait(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}

func pick(members []Member) Member { return members[rand.IntN(len(members))] }

// Kill SIGKILLs one random replica and restarts it on heal.
type Kill struct{}

func (Kill) Name() string { return "kill" }

func (Kill) Inject(ctx context.Context, members []Member) (Fault, error) {
	m := pick(members)
	if _, err := docker.Run(ctx, "kill", "--signal", "KILL", m.Container); err != nil {
		return nil, err
	}
	return restart{targets: []Member{m}}, nil
}

// KillMajority SIGKILLs a quorum at once; only meaningful with persisted durability.
type KillMajority struct{}

func (KillMajority) Name() string { return "kill-majority" }

func (KillMajority) Inject(ctx context.Context, members []Member) (Fault, error) {
	shuffled := append([]Member(nil), members...)
	rand.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
	victims := shuffled[:len(shuffled)/2+1]
	for _, m := range victims {
		if _, err := docker.Run(ctx, "kill", "--signal", "KILL", m.Container); err != nil {
			return restart{targets: victims}, err
		}
	}
	return restart{targets: victims}, nil
}

type restart struct{ targets []Member }

func (r restart) Describe() string { return "killed " + names(r.targets) }

func (r restart) Heal(ctx context.Context) error {
	for _, m := range r.targets {
		if _, err := docker.Run(ctx, "start", m.Container); err != nil {
			return err
		}
	}
	return nil
}

// Pause freezes one random replica with SIGSTOP semantics, simulating a long GC or VM stall.
type Pause struct{}

func (Pause) Name() string { return "pause" }

func (Pause) Inject(ctx context.Context, members []Member) (Fault, error) {
	m := pick(members)
	if _, err := docker.Run(ctx, "pause", m.Container); err != nil {
		return nil, err
	}
	return unpause{m}, nil
}

type unpause struct{ m Member }

func (u unpause) Describe() string { return "paused " + u.m.Container }

func (u unpause) Heal(ctx context.Context) error {
	_, err := docker.Run(ctx, "unpause", u.m.Container)
	return err
}

// Partition cuts one random replica off from its peers while clients can still reach it.
// That is the dangerous case: an isolated node that still accepts requests.
type Partition struct {
	SidecarImage string
}

func (Partition) Name() string { return "partition" }

func (p Partition) Inject(ctx context.Context, members []Member) (Fault, error) {
	isolated := pick(members)
	var cut []rule
	for _, peer := range members {
		if peer.Container == isolated.Container {
			continue
		}
		cut = append(cut, rule{isolated.Container, peer.PeerIP}, rule{peer.Container, isolated.PeerIP})
	}
	f := partitionFault{sidecar: p.SidecarImage, isolated: isolated, rules: cut}
	for _, r := range cut {
		if err := r.apply(ctx, p.SidecarImage, "-A"); err != nil {
			_ = f.Heal(ctx)
			return nil, err
		}
	}
	return f, nil
}

type rule struct{ container, dropFrom string }

func (r rule) apply(ctx context.Context, sidecar, action string) error {
	_, err := docker.InNetns(ctx, sidecar, r.container, "iptables", action, "INPUT", "-s", r.dropFrom, "-j", "DROP")
	return err
}

type partitionFault struct {
	sidecar  string
	isolated Member
	rules    []rule
	describe string
}

func (f partitionFault) Describe() string {
	if f.describe != "" {
		return f.describe
	}
	return "isolated " + f.isolated.Container + " from peers"
}

func (f partitionFault) Heal(ctx context.Context) error {
	var errs []string
	for _, r := range f.rules {
		if err := r.apply(ctx, f.sidecar, "-D"); err != nil && !strings.Contains(err.Error(), "Bad rule") {
			errs = append(errs, err.Error())
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("heal partition: %s", strings.Join(errs, "; "))
	}
	return nil
}

func names(ms []Member) string {
	out := make([]string, len(ms))
	for i, m := range ms {
		out[i] = m.Container
	}
	return strings.Join(out, ",")
}

// None injects nothing; it is the control run.
type None struct{}

func (None) Name() string { return "none" }

func (None) Inject(context.Context, []Member) (Fault, error) { return noop{}, nil }

type noop struct{}

func (noop) Describe() string           { return "no fault" }
func (noop) Heal(context.Context) error { return nil }

// Bridge splits the cluster so two replicas cannot see each other but both
// still see a third. Non-transitive connectivity is a classic trigger for
// two nodes each believing they lead.
type Bridge struct {
	SidecarImage string
}

func (Bridge) Name() string { return "bridge" }

func (b Bridge) Inject(ctx context.Context, members []Member) (Fault, error) {
	if len(members) < 3 {
		return nil, fmt.Errorf("bridge needs at least 3 members, have %d", len(members))
	}
	shuffled := append([]Member(nil), members...)
	rand.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
	bridge, left, right := shuffled[0], shuffled[1], shuffled[2]
	cut := []rule{{left.Container, right.PeerIP}, {right.Container, left.PeerIP}}
	f := partitionFault{sidecar: b.SidecarImage, isolated: bridge, rules: cut,
		describe: fmt.Sprintf("bridge via %s: %s cannot reach %s", bridge.Container, left.Container, right.Container)}
	for _, r := range cut {
		if err := r.apply(ctx, b.SidecarImage, "-A"); err != nil {
			_ = f.Heal(ctx)
			return nil, err
		}
	}
	return f, nil
}

// Mix picks a different nemesis each time.
type Mix []Nemesis

func (Mix) Name() string { return "mix" }

func (m Mix) Inject(ctx context.Context, members []Member) (Fault, error) {
	return m[rand.IntN(len(m))].Inject(ctx, members)
}

// CutDependency isolates one random replica from a shared dependency, such
// as the object store a diskless broker writes its log to, while it stays
// reachable by clients and peers.
type CutDependency struct {
	SidecarImage string
	Dependency   string
	DependencyIP string
}

func (c CutDependency) Name() string { return "cut-" + c.Dependency }

func (c CutDependency) Inject(ctx context.Context, members []Member) (Fault, error) {
	m := pick(members)
	cut := []rule{{m.Container, c.DependencyIP}}
	f := partitionFault{sidecar: c.SidecarImage, isolated: m, rules: cut,
		describe: fmt.Sprintf("cut %s off from %s", m.Container, c.Dependency)}
	if err := cut[0].apply(ctx, c.SidecarImage, "-A"); err != nil {
		return nil, err
	}
	return f, nil
}

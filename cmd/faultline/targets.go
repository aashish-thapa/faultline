package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/aashish-thapa/faultline/internal/iggy"
	"github.com/aashish-thapa/faultline/internal/kafka"
	"github.com/aashish-thapa/faultline/internal/nemesis"
	"github.com/aashish-thapa/faultline/internal/queue"
)

// session is one run's clients: load goes through client, the verdict's
// final reads through final.
type session struct {
	client queue.Client
	final  queue.Client
	close  func()
}

// target describes one system under test; each mirrors a compose file in targets/.
type target struct {
	nodes   []queue.Node
	members []nemesis.Member
	// storage is a shared dependency the replicas write through, if any.
	storage *dependency
	open    func(ctx context.Context, o options, runID string, seed queue.Node, logger *slog.Logger) (session, error)
}

type dependency struct{ name, ip string }

var targets = map[string]target{
	"iggy": {
		nodes: []queue.Node{
			{Name: "iggy-node-0", URL: "http://127.0.0.1:3000"},
			{Name: "iggy-node-1", URL: "http://127.0.0.1:3001"},
			{Name: "iggy-node-2", URL: "http://127.0.0.1:3002"},
		},
		members: []nemesis.Member{
			{Container: "iggy-node-0", PeerIP: "10.29.0.10"},
			{Container: "iggy-node-1", PeerIP: "10.29.0.11"},
			{Container: "iggy-node-2", PeerIP: "10.29.0.12"},
		},
		open: openIggy,
	},
	"automq": {
		nodes: []queue.Node{
			{Name: "automq-server1", URL: "127.0.0.1:19091"},
			{Name: "automq-server2", URL: "127.0.0.1:19092"},
			{Name: "automq-server3", URL: "127.0.0.1:19093"},
		},
		members: []nemesis.Member{
			{Container: "automq-server1", PeerIP: "10.6.0.11"},
			{Container: "automq-server2", PeerIP: "10.6.0.12"},
			{Container: "automq-server3", PeerIP: "10.6.0.13"},
		},
		storage: &dependency{name: "minio", ip: "10.6.0.5"},
		open:    openKafka,
	},
}

var iggyTCPAddrs = []string{"127.0.0.1:8090", "127.0.0.1:8091", "127.0.0.1:8092"}

func openIggy(ctx context.Context, o options, runID string, seed queue.Node, logger *slog.Logger) (session, error) {
	stream := "fl-" + runID
	httpClient := iggy.NewClient(&http.Client{}, iggy.Credentials{Username: "iggy", Password: "iggy"}, stream, "log")
	if err := httpClient.Setup(ctx, seed, iggy.Durability(o.durability)); err != nil {
		return session{}, fmt.Errorf("setup: %w", err)
	}
	switch o.producer {
	case "http":
		return session{client: httpClient, final: httpClient, close: func() {}}, nil
	case "idempotent":
		idem, err := iggy.NewIdempotentClient(o.driver, iggyTCPAddrs, stream, "log", o.producers, o.attempt, httpClient)
		if err != nil {
			return session{}, err
		}
		return session{client: idem, final: httpClient, close: func() {
			logger.Info("idempotent producer resends", "acknowledged_after_resend", idem.Resent())
			idem.Close()
		}}, nil
	}
	return session{}, fmt.Errorf("unknown producer %q for iggy", o.producer)
}

func openKafka(ctx context.Context, o options, runID string, seed queue.Node, _ *slog.Logger) (session, error) {
	opts := []kgo.Opt{kgo.RequiredAcks(kgo.AllISRAcks())}
	switch o.producer {
	case "idempotent":
	case "plain":
		opts = append(opts, kgo.DisableIdempotentWrite())
	default:
		return session{}, fmt.Errorf("unknown producer %q for kafka (idempotent | plain)", o.producer)
	}
	topic := "fl-" + runID
	client := kafka.NewClient(topic, opts...)
	if err := client.Setup(ctx, seed, 1, nil); err != nil {
		return session{}, fmt.Errorf("setup: %w", err)
	}
	return session{client: client, final: client, close: client.Close}, nil
}

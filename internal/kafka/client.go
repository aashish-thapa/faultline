// Package kafka adapts any Kafka-protocol broker to the queue workload.
package kafka

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/aashish-thapa/faultline/internal/queue"
)

// Client produces through one long-lived producer per bootstrap node and
// reads with a short-lived consumer per poll, so a read never inherits
// state from an earlier one.
type Client struct {
	topic string
	opts  []kgo.Opt

	mu        sync.Mutex
	producers map[string]*kgo.Client
}

var _ queue.Client = (*Client)(nil)

// NewClient takes producer options such as acks and idempotence; the
// partitioner and topic are fixed so every append targets partition 0.
func NewClient(topic string, producerOpts ...kgo.Opt) *Client {
	return &Client{topic: topic, opts: producerOpts, producers: map[string]*kgo.Client{}}
}

// Setup creates a single-partition topic.
func (c *Client) Setup(ctx context.Context, node queue.Node, replicationFactor int16, configs map[string]*string) error {
	cl, err := kgo.NewClient(kgo.SeedBrokers(node.URL))
	if err != nil {
		return err
	}
	defer cl.Close()
	resp, err := kadm.NewClient(cl).CreateTopic(ctx, 1, replicationFactor, configs, c.topic)
	if err != nil {
		return fmt.Errorf("create topic: %w", err)
	}
	return resp.Err
}

func (c *Client) Append(ctx context.Context, node queue.Node, value string) (uint64, error) {
	producer, err := c.producer(node)
	if err != nil {
		return 0, fmt.Errorf("%w: %w", queue.ErrRejected, err)
	}
	rec := &kgo.Record{Topic: c.topic, Partition: 0, Value: []byte(value)}
	if err := producer.ProduceSync(ctx, rec).FirstErr(); err != nil {
		// Kafka clients retry internally, so any error leaves the outcome unknown.
		return 0, err
	}
	if rec.Offset < 0 {
		return 0, fmt.Errorf("acknowledged with negative offset %d", rec.Offset)
	}
	return uint64(rec.Offset), nil
}

func (c *Client) Read(ctx context.Context, node queue.Node, offset uint64, count int) ([]queue.Record, error) {
	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(node.URL),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{c.topic: {0: kgo.NewOffset().At(int64(offset))}}),
		kgo.FetchMaxWait(500*time.Millisecond),
	)
	if err != nil {
		return nil, err
	}
	defer consumer.Close()

	end, err := c.highWatermark(ctx, consumer)
	if err != nil {
		return nil, err
	}
	var records []queue.Record
	for len(records) < count && int64(offset)+int64(len(records)) < end {
		fetches := consumer.PollFetches(ctx)
		if err := ctx.Err(); err != nil {
			return records, err
		}
		var fetchErr error
		fetches.EachError(func(_ string, _ int32, err error) { fetchErr = errors.Join(fetchErr, err) })
		if fetchErr != nil {
			return records, fetchErr
		}
		fetches.EachRecord(func(r *kgo.Record) {
			records = append(records, queue.Record{Offset: uint64(r.Offset), Value: string(r.Value)})
		})
	}
	if len(records) > count {
		records = records[:count]
	}
	return records, nil
}

// highWatermark bounds a read so it returns instead of waiting for new data.
func (c *Client) highWatermark(ctx context.Context, cl *kgo.Client) (int64, error) {
	ends, err := kadm.NewClient(cl).ListEndOffsets(ctx, c.topic)
	if err != nil {
		return 0, err
	}
	o, ok := ends.Lookup(c.topic, 0)
	if !ok {
		return 0, fmt.Errorf("no end offset for %s/0", c.topic)
	}
	return o.Offset, o.Err
}

func (c *Client) producer(node queue.Node) (*kgo.Client, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if p, ok := c.producers[node.Name]; ok {
		return p, nil
	}
	opts := append([]kgo.Opt{
		kgo.SeedBrokers(node.URL),
		kgo.DefaultProduceTopic(c.topic),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	}, c.opts...)
	p, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	c.producers[node.Name] = p
	return p, nil
}

func (c *Client) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, p := range c.producers {
		p.Close()
	}
}

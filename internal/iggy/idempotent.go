package iggy

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/aashish-thapa/faultline/internal/queue"
)

// IdempotentClient appends through Rust SDK idempotent producers, one driver
// process per concurrent producer, and reads through the HTTP client.
// Each driver is serial, so any free driver may carry any producer's append.
type IdempotentClient struct {
	drivers chan *driver
	reader  queue.Client
	all     []*driver
	resent  atomic.Int64
}

var _ queue.Client = (*IdempotentClient)(nil)

// NewIdempotentClient starts n drivers, spreading their bootstrap addresses over tcpAddrs.
func NewIdempotentClient(binary string, tcpAddrs []string, stream, topic string, n int, attemptTimeout time.Duration, reader queue.Client) (*IdempotentClient, error) {
	c := &IdempotentClient{drivers: make(chan *driver, n), reader: reader}
	for i := range n {
		d, err := startDriver(binary, tcpAddrs[i%len(tcpAddrs)], stream, topic, attemptTimeout)
		if err != nil {
			c.Close()
			return nil, err
		}
		c.all = append(c.all, d)
		c.drivers <- d
	}
	return c, nil
}

// Append ignores ctx's deadline: the driver bounds its own retries, and
// abandoning a driver mid-send would leave its reply unread.
func (c *IdempotentClient) Append(_ context.Context, _ queue.Node, value string) (uint64, error) {
	d := <-c.drivers
	defer func() { c.drivers <- d }()
	offset, attempts, err := d.append(value)
	if err == nil && attempts > 1 {
		c.resent.Add(1)
	}
	return offset, err
}

// Resent counts acknowledged appends that needed more than one send,
// i.e. the ones whose success depended on server-side deduplication.
func (c *IdempotentClient) Resent() int64 { return c.resent.Load() }

func (c *IdempotentClient) Read(ctx context.Context, node queue.Node, offset uint64, count int) ([]queue.Record, error) {
	return c.reader.Read(ctx, node, offset, count)
}

func (c *IdempotentClient) Close() {
	for _, d := range c.all {
		d.close()
	}
}

type driver struct {
	cmd *exec.Cmd
	in  io.WriteCloser
	out *bufio.Scanner
}

func startDriver(binary, addr, stream, topic string, attemptTimeout time.Duration) (*driver, error) {
	cmd := exec.Command(binary, addr, stream, topic, strconv.FormatInt(attemptTimeout.Milliseconds(), 10))
	in, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	out, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start driver: %w", err)
	}
	return &driver{cmd: cmd, in: in, out: bufio.NewScanner(out)}, nil
}

var errDriverGone = errors.New("driver exited")

func (d *driver) append(value string) (offset uint64, attempts int, err error) {
	line, err := json.Marshal(map[string]string{"value": value})
	if err != nil {
		return 0, 0, fmt.Errorf("%w: %w", queue.ErrRejected, err)
	}
	if _, err := d.in.Write(append(line, '\n')); err != nil {
		return 0, 0, fmt.Errorf("%w: %w", queue.ErrRejected, err)
	}
	if !d.out.Scan() {
		return 0, 0, errDriverGone
	}
	var outcome struct {
		Status   string `json:"status"`
		Offset   uint64 `json:"offset"`
		Attempts int    `json:"attempts"`
		Error    string `json:"error"`
	}
	if err := json.Unmarshal(d.out.Bytes(), &outcome); err != nil {
		return 0, 0, fmt.Errorf("decode driver outcome: %w", err)
	}
	switch outcome.Status {
	case "ok":
		return outcome.Offset, outcome.Attempts, nil
	case "fail":
		return 0, outcome.Attempts, fmt.Errorf("%w: %s", queue.ErrRejected, outcome.Error)
	default:
		return 0, outcome.Attempts, fmt.Errorf("unknown outcome after %d attempts: %s", outcome.Attempts, outcome.Error)
	}
}

func (d *driver) close() {
	d.in.Close()
	_ = d.cmd.Wait()
}

// Package iggy adapts Apache Iggy's HTTP API to the queue workload.
package iggy

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"
	"syscall"

	"github.com/aashish-thapa/faultline/internal/queue"
)

// Credentials are the root user the cluster was booted with.
type Credentials struct {
	Username string
	Password string
}

// Client talks to any node over HTTP; Iggy forwards to the partition primary.
type Client struct {
	http   *http.Client
	creds  Credentials
	stream string
	topic  string

	mu     sync.Mutex
	tokens map[string]string
}

var _ queue.Client = (*Client)(nil)

func NewClient(httpClient *http.Client, creds Credentials, stream, topic string) *Client {
	return &Client{http: httpClient, creds: creds, stream: stream, topic: topic, tokens: map[string]string{}}
}

// Durability is the per-topic acknowledgement policy Iggy documents.
type Durability string

const (
	Replicated Durability = "replicated"
	Persisted  Durability = "persisted"
)

// Setup creates the stream and a single-partition topic for the run.
func (c *Client) Setup(ctx context.Context, node queue.Node, durability Durability) error {
	if _, err := c.do(ctx, node, http.MethodPost, "/streams", map[string]any{"name": c.stream}); err != nil {
		return fmt.Errorf("create stream: %w", err)
	}
	topic := map[string]any{
		"name":             c.topic,
		"partitions_count": 1,
		"options": map[string]any{
			"compression_algorithm": "none",
			"message_expiry":        "none",
			"max_topic_size":        "unlimited",
			"durability":            string(durability),
		},
	}
	body, err := c.do(ctx, node, http.MethodPost, "/streams/"+c.stream+"/topics", topic)
	if err != nil {
		return fmt.Errorf("create topic: %w", err)
	}
	var created struct {
		Options struct {
			Durability struct {
				Value string `json:"value"`
			} `json:"durability"`
		} `json:"options"`
	}
	if err := json.Unmarshal(body, &created); err != nil {
		return fmt.Errorf("decode topic: %w", err)
	}
	if got := created.Options.Durability.Value; got != string(durability) {
		return fmt.Errorf("topic durability is %q, wanted %q", got, durability)
	}
	return nil
}

func (c *Client) Append(ctx context.Context, node queue.Node, value string) (uint64, error) {
	req := map[string]any{
		"partitioning": map[string]any{"kind": "partition_id", "value": base64.StdEncoding.EncodeToString([]byte{0, 0, 0, 0})},
		"messages":     []map[string]any{{"id": 0, "payload": base64.StdEncoding.EncodeToString([]byte(value))}},
	}
	body, err := c.do(ctx, node, http.MethodPost, c.topicPath()+"/messages", req)
	if err != nil {
		return 0, err
	}
	var resp struct {
		Confirmations []struct {
			BaseOffset uint64 `json:"base_offset"`
		} `json:"confirmations"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return 0, fmt.Errorf("decode confirmation: %w", err)
	}
	if len(resp.Confirmations) != 1 {
		return 0, fmt.Errorf("acknowledged with %d confirmations: %s", len(resp.Confirmations), body)
	}
	return resp.Confirmations[0].BaseOffset, nil
}

func (c *Client) Read(ctx context.Context, node queue.Node, offset uint64, count int) ([]queue.Record, error) {
	q := url.Values{
		"consumer_id":  {"1"},
		"partition_id": {"0"},
		"kind":         {"offset"},
		"value":        {fmt.Sprint(offset)},
		"count":        {fmt.Sprint(count)},
		"auto_commit":  {"false"},
	}
	body, err := c.do(ctx, node, http.MethodGet, c.topicPath()+"/messages?"+q.Encode(), nil)
	if err != nil {
		return nil, err
	}
	var resp struct {
		Messages []struct {
			Header struct {
				Offset uint64 `json:"offset"`
			} `json:"header"`
			Payload string `json:"payload"`
		} `json:"messages"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("decode poll: %w", err)
	}
	records := make([]queue.Record, 0, len(resp.Messages))
	for _, m := range resp.Messages {
		payload, err := base64.StdEncoding.DecodeString(m.Payload)
		if err != nil {
			return nil, fmt.Errorf("decode payload at offset %d: %w", m.Header.Offset, err)
		}
		records = append(records, queue.Record{Offset: m.Header.Offset, Value: string(payload)})
	}
	return records, nil
}

func (c *Client) topicPath() string { return "/streams/" + c.stream + "/topics/" + c.topic }

// do sends one request, logging in on first use and once more after a 401.
func (c *Client) do(ctx context.Context, node queue.Node, method, path string, payload any) ([]byte, error) {
	for attempt := 0; ; attempt++ {
		token, err := c.token(ctx, node, attempt > 0)
		if err != nil {
			// The operation itself was never sent.
			return nil, fmt.Errorf("%w: %w", queue.ErrRejected, err)
		}
		status, body, err := c.send(ctx, node, method, path, token, payload)
		if errors.Is(err, syscall.ECONNREFUSED) {
			return nil, fmt.Errorf("%w: %w", queue.ErrRejected, err)
		}
		if err != nil {
			return nil, err
		}
		if status == http.StatusUnauthorized && attempt == 0 {
			continue
		}
		if status >= 200 && status < 300 {
			return body, nil
		}
		return nil, classify(status, body)
	}
}

// classify maps Iggy's documented statuses to definite or unknown outcomes.
// 504 is documented as "the write may still commit"; 4xx, 429 and 503 are
// documented as refusals or retryable admission failures, i.e. not applied.
func classify(status int, body []byte) error {
	err := fmt.Errorf("http %d: %s", status, bytes.TrimSpace(body))
	if status == http.StatusServiceUnavailable || (status >= 400 && status < 500) {
		return fmt.Errorf("%w: %w", queue.ErrRejected, err)
	}
	return err
}

func (c *Client) send(ctx context.Context, node queue.Node, method, path, token string, payload any) (int, []byte, error) {
	var reader io.Reader
	if payload != nil {
		encoded, err := json.Marshal(payload)
		if err != nil {
			return 0, nil, err
		}
		reader = bytes.NewReader(encoded)
	}
	req, err := http.NewRequestWithContext(ctx, method, node.URL+path, reader)
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	return resp.StatusCode, body, err
}

func (c *Client) token(ctx context.Context, node queue.Node, refresh bool) (string, error) {
	c.mu.Lock()
	cached, ok := c.tokens[node.Name]
	c.mu.Unlock()
	if ok && !refresh {
		return cached, nil
	}
	status, body, err := c.send(ctx, node, http.MethodPost, "/users/login", "", map[string]string{
		"username": c.creds.Username, "password": c.creds.Password,
	})
	if err != nil {
		return "", fmt.Errorf("login %s: %w", node.Name, err)
	}
	if status != http.StatusOK {
		return "", fmt.Errorf("login %s: %w", node.Name, classify(status, body))
	}
	var resp struct {
		AccessToken struct {
			Token string `json:"token"`
		} `json:"access_token"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return "", fmt.Errorf("decode login: %w", err)
	}
	c.mu.Lock()
	c.tokens[node.Name] = resp.AccessToken.Token
	c.mu.Unlock()
	return resp.AccessToken.Token, nil
}

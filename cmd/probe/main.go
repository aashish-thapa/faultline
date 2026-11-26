// Command probe writes to one Kafka partition at a steady rate and prints
// each outcome with its latency, to measure how long a fault stalls writes.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	brokers := flag.String("brokers", "127.0.0.1:19091", "bootstrap broker")
	topic := flag.String("topic", "", "topic to write to (partition 0)")
	interval := flag.Duration("interval", 200*time.Millisecond, "time between writes")
	duration := flag.Duration("duration", time.Minute, "how long to probe")
	timeout := flag.Duration("timeout", 5*time.Second, "per-write timeout")
	flag.Parse()

	cl, err := kgo.NewClient(kgo.SeedBrokers(*brokers), kgo.DefaultProduceTopic(*topic),
		kgo.RecordPartitioner(kgo.ManualPartitioner()), kgo.RequiredAcks(kgo.AllISRAcks()))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer cl.Close()

	start := time.Now()
	for i := 0; time.Since(start) < *duration; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), *timeout)
		sent := time.Now()
		rec := &kgo.Record{Partition: 0, Value: fmt.Appendf(nil, "probe-%d", i)}
		err := cl.ProduceSync(ctx, rec).FirstErr()
		cancel()
		at := sent.Sub(start).Seconds()
		if err != nil {
			fmt.Printf("%7.1fs  FAIL  %v\n", at, err)
		} else {
			fmt.Printf("%7.1fs  ok    offset=%d latency=%s\n", at, rec.Offset, time.Since(sent).Round(time.Millisecond))
		}
		time.Sleep(*interval)
	}
}

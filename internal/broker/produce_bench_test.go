// Benchmarks measuring produce throughput against a real Artemis broker
// (Testcontainers). Run via `make bench`; skipped in -short and never run by
// plain `go test` (benchmarks need -bench).
//
// BenchmarkProduce answers "do parallel workers help?": each worker is a
// sender on its own session over ONE shared connection, so settlement
// round-trips overlap. BenchmarkProduceMultiConn answers "do separate
// connections beat sessions?": same worker counts, but each worker gets a
// whole connection of its own.
package broker

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Azure/go-amqp"
)

var benchWorkerCounts = []int{1, 2, 4, 8, 16}

// benchQueue returns a unique queue name per sub-benchmark run so message
// depth from earlier runs doesn't skew later ones.
var benchQueueSeq atomic.Int64

func benchQueue(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, benchQueueSeq.Add(1))
}

func BenchmarkProduce(b *testing.B) {
	if testing.Short() {
		b.Skip("needs broker")
	}
	props := startArtemis(b)

	for _, w := range benchWorkerCounts {
		b.Run(fmt.Sprintf("workers=%d", w), func(b *testing.B) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()
			c, err := Connect(ctx, props)
			if err != nil {
				b.Fatalf("connect: %v", err)
			}
			defer c.Close(ctx)

			msgs := GenerateMessages(b.N, 256, nil)
			queue := benchQueue("bench-produce")

			b.ResetTimer()
			sent, err := c.Produce(ctx, queue, msgs, 0, w, nil)
			b.StopTimer()
			if err != nil {
				b.Fatalf("produce: %v", err)
			}
			if sent != b.N {
				b.Fatalf("sent = %d, want %d", sent, b.N)
			}
			b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "msgs/s")
		})
	}
}

func BenchmarkProduceMultiConn(b *testing.B) {
	if testing.Short() {
		b.Skip("needs broker")
	}
	props := startArtemis(b)

	for _, w := range benchWorkerCounts {
		b.Run(fmt.Sprintf("conns=%d", w), func(b *testing.B) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()

			clients := make([]*Client, w)
			for i := range clients {
				c, err := Connect(ctx, props)
				if err != nil {
					b.Fatalf("connect %d: %v", i, err)
				}
				defer c.Close(ctx)
				clients[i] = c
			}

			msgs := GenerateMessages(b.N, 256, nil)
			queue := benchQueue("bench-produce-multiconn")

			// Split messages across clients; each client sends its slice
			// sequentially over its own connection (workers=1), so the only
			// parallelism measured is connection-level.
			b.ResetTimer()
			var wg sync.WaitGroup
			var sent atomic.Int64
			errs := make([]error, w)
			for i, c := range clients {
				lo := i * len(msgs) / w
				hi := (i + 1) * len(msgs) / w
				if lo == hi {
					continue
				}
				wg.Add(1)
				go func(i int, c *Client, part []*amqp.Message) {
					defer wg.Done()
					n, err := c.Produce(ctx, queue, part, 0, 1, nil)
					sent.Add(int64(n))
					errs[i] = err
				}(i, c, msgs[lo:hi])
			}
			wg.Wait()
			b.StopTimer()
			for i, err := range errs {
				if err != nil {
					b.Fatalf("produce conn %d: %v", i, err)
				}
			}
			if got := int(sent.Load()); got != b.N {
				b.Fatalf("sent = %d, want %d", got, b.N)
			}
			b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "msgs/s")
		})
	}
}

// isolane/worker/go/consumer.go
//
// Redis Streams XREADGROUP consumer — Go production implementation.
//
// The Go worker replaces the Python consumer for production throughput.
// The Python engine (batch.py, dbt) is called as a subprocess per batch.
// This gives us Go's goroutine-per-namespace concurrency model with
// Python's rich data transformation ecosystem.

package worker

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	DefaultBlockMs    = 2000
	DefaultBatchCount = 10
	NSScanInterval    = 60 * time.Second
)

// Consumer is the main Redis Streams consumer.
type Consumer struct {
	rdb        redis.UniversalClient
	cfg        *Config
	namespaces []string
	claimers   map[string]*Claimer
	mu         sync.Mutex
	stopCh     chan struct{}
}

// NewConsumer creates a new Consumer.
func NewConsumer(rdb redis.UniversalClient, cfg *Config) *Consumer {
	return &Consumer{
		rdb:      rdb,
		cfg:      cfg,
		claimers: make(map[string]*Claimer),
		stopCh:   make(chan struct{}),
	}
}

// Start begins consuming — blocks until Stop() is called.
func (c *Consumer) Start(ctx context.Context) {
	fmt.Printf("[worker] %s starting\n", c.cfg.WorkerID)

	StartMetricsServer(c.cfg.MetricsPort)

	// Initial namespace discovery
	c.refreshNamespaces(ctx)

	// Background namespace refresh
	go c.nsRefreshLoop(ctx)

	// Main consumer loop
	c.consumeLoop(ctx)

	// Shutdown
	c.shutdown()
}

// Stop signals graceful shutdown.
func (c *Consumer) Stop() {
	close(c.stopCh)
}

func (c *Consumer) consumeLoop(ctx context.Context) {
	for {
		select {
		case <-c.stopCh:
			return
		case <-ctx.Done():
			return
		default:
		}

		c.mu.Lock()
		namespaces := make([]string, len(c.namespaces))
		copy(namespaces, c.namespaces)
		c.mu.Unlock()

		if len(namespaces) == 0 {
			fmt.Println("[worker] No namespaces — waiting...")
			select {
			case <-time.After(10 * time.Second):
			case <-c.stopCh:
				return
			}
			continue
		}

		// Ensure claimers are running
		for _, ns := range namespaces {
			c.ensureClaimer(ctx, ns)
		}

		// Build streams map for XREADGROUP
		streams := make(map[string]string, len(namespaces))
		for _, ns := range namespaces {
			streams[fmt.Sprintf("%s.work-queue", ns)] = ">"
		}

		results, err := c.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    fmt.Sprintf("%s-consumer-group", namespaces[0]),
			Consumer: c.cfg.WorkerID,
			Streams:  streamsToSlice(streams),
			Count:    int64(c.cfg.BatchCount),
			Block:    time.Duration(c.cfg.BlockMs) * time.Millisecond,
		}).Result()

		if err != nil {
			if err == redis.Nil {
				continue // timeout — no messages
			}
			if isNoGroupError(err) {
				c.refreshNamespaces(ctx)
				continue
			}
			if strings.Contains(err.Error(), "context") {
				return
			}
			fmt.Printf("[worker] XREADGROUP error: %v — reconnecting\n", err)
			time.Sleep(3 * time.Second)
			continue
		}

		for _, stream := range results {
			ns := strings.Replace(stream.Stream, ".work-queue", "", 1)
			for _, msg := range stream.Messages {
				fields := xMessageToStringMap(msg.Values)
				item := &WorkItem{
					StreamID:   msg.ID,
					Namespace:  ns,
					StreamKey:  stream.Stream,
					Fields:     fields,
					ClaimCount: GetClaimCount(fields),
					DequeuedAt: time.Now(),
				}
				c.handleMessage(ctx, item)
			}
		}
	}
}

func (c *Consumer) handleMessage(ctx context.Context, item *WorkItem) {
	result := Dispatch(ctx, item, c.cfg)

	if result.ShouldACK() {
		group := fmt.Sprintf("%s-consumer-group", item.Namespace)
		if err := c.rdb.XAck(ctx, item.StreamKey, group, item.StreamID).Err(); err != nil {
			fmt.Printf("[worker] ACK failed for %s: %v\n", item.StreamID, err)
		}
	} else {
		fmt.Printf(
			"[worker] %s | %s NOT ACK'd — will be reclaimed\n",
			item.Namespace, item.StreamID,
		)
	}
}

func (c *Consumer) refreshNamespaces(ctx context.Context) {
	found := discoverNamespaces(ctx, c.rdb)

	c.mu.Lock()
	defer c.mu.Unlock()

	added   := diff(found, c.namespaces)
	removed := diff(c.namespaces, found)

	if len(added) > 0 {
		fmt.Printf("[worker] New namespaces: %v\n", added)
	}
	if len(removed) > 0 {
		fmt.Printf("[worker] Removed namespaces: %v\n", removed)
	}

	c.namespaces = found
	ActiveNamespaces.Set(float64(len(found)))

	if len(found) > 0 {
		fmt.Printf("[worker] Active namespaces: %v\n", found)
	}
}

func (c *Consumer) nsRefreshLoop(ctx context.Context) {
	ticker := time.NewTicker(NSScanInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			c.refreshNamespaces(ctx)
		case <-c.stopCh:
			return
		case <-ctx.Done():
			return
		}
	}
}

func (c *Consumer) ensureClaimer(ctx context.Context, ns string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.claimers[ns]; !ok {
		claimer := NewClaimer(c.rdb, ns, c.cfg.WorkerID, c.cfg, Dispatch)
		claimer.Start(ctx)
		c.claimers[ns] = claimer
	}
}

func (c *Consumer) shutdown() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for ns, claimer := range c.claimers {
		claimer.Stop()
		delete(c.claimers, ns)
	}
	fmt.Printf("[worker] %s shutdown complete\n", c.cfg.WorkerID)
}

// discoverNamespaces scans Redis for *.work-queue streams.
func discoverNamespaces(ctx context.Context, rdb redis.UniversalClient) []string {
	var namespaces []string
	var cursor uint64

	for {
		keys, nextCursor, err := rdb.Scan(ctx, cursor, "*.work-queue", 100).Result()
		if err != nil {
			break
		}
		for _, key := range keys {
			ns := strings.Replace(key, ".work-queue", "", 1)
			namespaces = append(namespaces, ns)
		}
		cursor = nextCursor
		if cursor == 0 {
			break
		}
	}
	return namespaces
}

func streamsToSlice(streams map[string]string) []string {
	keys := make([]string, 0, len(streams)*2)
	ids  := make([]string, 0, len(streams))
	for k, v := range streams {
		keys = append(keys, k)
		ids  = append(ids, v)
	}
	return append(keys, ids...)
}

func diff(a, b []string) []string {
	bSet := make(map[string]struct{}, len(b))
	for _, v := range b {
		bSet[v] = struct{}{}
	}
	var result []string
	for _, v := range a {
		if _, ok := bSet[v]; !ok {
			result = append(result, v)
		}
	}
	return result
}

// RunWorker is the main entry point called from main.go.
func RunWorker(cfg *Config) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rdb := newRedisClient(cfg)

	consumer := NewConsumer(rdb, cfg)

	// Graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		<-sigCh
		fmt.Println("[worker] Shutdown signal received")
		cancel()
		consumer.Stop()
	}()

	consumer.Start(ctx)
}

func newRedisClient(cfg *Config) redis.UniversalClient {
	if len(cfg.SentinelAddrs) > 0 {
		client := redis.NewFailoverClient(&redis.FailoverOptions{
			MasterName:    cfg.MasterName,
			SentinelAddrs: cfg.SentinelAddrs,
		})
		if err := client.Ping(context.Background()).Err(); err == nil {
			fmt.Println("[worker] Connected via Redis Sentinel")
			return client
		}
		fmt.Println("[worker] Sentinel failed — falling back to direct")
	}

	client := redis.NewClient(&redis.Options{
		Addr:         fmt.Sprintf("%s:%d", cfg.RedisHost, cfg.RedisPort),
		DialTimeout:  5 * time.Second,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 5 * time.Second,
	})
	fmt.Println("[worker] Connected directly to Redis master")
	return client
}
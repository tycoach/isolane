// isolane/worker/go/claimer.go
//
// XAUTOCLAIM poller — one goroutine per active namespace.
// Mirrors the Python claimer.py behaviour.

package worker

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// Claimer polls XAUTOCLAIM for a single namespace.
type Claimer struct {
	rdb        redis.UniversalClient
	namespace  string
	workerID   string
	cfg        *Config
	dispatchFn func(context.Context, *WorkItem, *Config) *DispatchResult
	stopCh     chan struct{}
}

// NewClaimer creates a new Claimer for the given namespace.
func NewClaimer(
	rdb redis.UniversalClient,
	namespace string,
	workerID string,
	cfg *Config,
	dispatchFn func(context.Context, *WorkItem, *Config) *DispatchResult,
) *Claimer {
	return &Claimer{
		rdb:        rdb,
		namespace:  namespace,
		workerID:   workerID,
		cfg:        cfg,
		dispatchFn: dispatchFn,
		stopCh:     make(chan struct{}),
	}
}

// Start launches the claimer in a background goroutine.
func (c *Claimer) Start(ctx context.Context) {
	go c.run(ctx)
	fmt.Printf(
		"[claimer] %s — started (timeout=%dms)\n",
		c.namespace, c.cfg.ClaimTimeoutMs,
	)
}

// Stop signals the claimer to stop.
func (c *Claimer) Stop() {
	close(c.stopCh)
}

func (c *Claimer) run(ctx context.Context) {
	interval := time.Duration(c.cfg.ClaimTimeoutMs/2) * time.Millisecond

	for {
		select {
		case <-c.stopCh:
			fmt.Printf("[claimer] %s — stopped\n", c.namespace)
			return
		case <-ctx.Done():
			return
		case <-time.After(interval):
			c.runOneCycle(ctx)
			XAutoclaimCyclesTotal.WithLabelValues(c.namespace).Inc()
		}
	}
}

func (c *Claimer) runOneCycle(ctx context.Context) {
	streamKey := fmt.Sprintf("%s.work-queue", c.namespace)
	group := fmt.Sprintf("%s-consumer-group", c.namespace)

	result, _, err := c.rdb.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   streamKey,
		Group:    group,
		Consumer: c.workerID,
		MinIdle:  time.Duration(c.cfg.ClaimTimeoutMs) * time.Millisecond,
		Start:    "0-0",
		Count:    10,
	}).Result()

	if err != nil {
		if isNoGroupError(err) {
			return
		}
		fmt.Printf("[claimer] %s — XAUTOCLAIM error: %v\n", c.namespace, err)
		return
	}

	for _, msg := range result {
		fields := xMessageToStringMap(msg.Values)

		if ShouldRouteToDLQ(fields, c.cfg.MaxClaimAttempts) {
			if err := RouteToDLQ(ctx, c.rdb, c.namespace, msg.ID, fields,
				"max_claims_exceeded"); err != nil {
				fmt.Printf("[claimer] DLQ write failed: %v\n", err)
			}
			c.rdb.XAck(ctx, streamKey, group, msg.ID)
			continue
		}

		updatedFields := IncrementClaimCount(fields)
		fmt.Printf(
			"[claimer] %s | reclaiming %s (attempt %s)\n",
			c.namespace, msg.ID, updatedFields["_claim_count"],
		)

		item := &WorkItem{
			StreamID:   msg.ID,
			Namespace:  c.namespace,
			StreamKey:  streamKey,
			Fields:     updatedFields,
			ClaimCount: GetClaimCount(updatedFields),
			DequeuedAt: time.Now(),
		}

		dispatchResult := c.dispatchFn(ctx, item, c.cfg)
		if dispatchResult.ShouldACK() {
			c.rdb.XAck(ctx, streamKey, group, msg.ID)
		}
	}
}

func isNoGroupError(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return contains(s, "NOGROUP") || contains(s, "no such key")
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) &&
		(s == substr || len(s) > 0 && containsStr(s, substr))
}

func containsStr(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func xMessageToStringMap(values map[string]interface{}) map[string]string {
	out := make(map[string]string, len(values))
	for k, v := range values {
		if s, ok := v.(string); ok {
			out[k] = s
		} else {
			out[k] = fmt.Sprintf("%v", v)
		}
	}
	return out
}

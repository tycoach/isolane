// isolane/worker/go/types.go
//
// Shared types for the Go worker.
// The Go worker is the production consumer — it replaces the
// Python consumer for throughput while the Python engine
// (dbt + Polars) handles transformation via subprocess calls.

package worker

import "time"

// WorkItem is a single work item dequeued from Redis Streams.
type WorkItem struct {
	StreamID   string            // Redis stream message ID e.g. "1712345678901-0"
	Namespace  string            // Tenant namespace e.g. "analytics"
	StreamKey  string            // Full stream key e.g. "analytics.work-queue"
	Fields     map[string]string // Raw Redis fields
	ClaimCount int               // Number of times XAUTOCLAIM has re-delivered this
	DequeuedAt time.Time
}

// PipelineID returns the pipeline identifier from the work item fields.
func (w *WorkItem) PipelineID() string {
	return w.Fields["pipeline_id"]
}

// BatchOffset returns the batch offset from the work item fields.
func (w *WorkItem) BatchOffset() string {
	if bo := w.Fields["batch_offset"]; bo != "" {
		return bo
	}
	return w.StreamID
}

// RecordsJSON returns the raw JSON records from the work item fields.
func (w *WorkItem) RecordsJSON() string {
	return w.Fields["records_json"]
}

// DispatchResult is the outcome of processing one work item.
type DispatchResult struct {
	WorkItem           *WorkItem
	Status             string // done | failed | quarantined
	RecordsIn          int
	RecordsOut         int
	RecordsQuarantined int
	Duration           time.Duration
	Error              error
}

// ShouldACK returns true if the work item should be ACK'd.
// Failed items are NOT ACK'd — XAUTOCLAIM will re-deliver them.
func (r *DispatchResult) ShouldACK() bool {
	return r.Status == "done" || r.Status == "quarantined"
}

// Config holds the worker configuration loaded from environment.
type Config struct {
	// Redis
	RedisHost     string
	RedisPort     int
	SentinelAddrs []string
	MasterName    string

	// PostgreSQL
	PostgresHost string
	PostgresPort int
	PostgresUser string
	PostgresPass string
	PostgresDB   string

	// Worker
	WorkerID         string
	MetricsPort      int
	ClaimTimeoutMs   int
	MaxClaimAttempts int
	BatchCount       int
	BlockMs          int

	// Engine
	DBTProjectDir  string
	RocksDBPath    string
	TenantRolePass string
}

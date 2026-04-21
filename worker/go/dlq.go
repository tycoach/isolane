// isolane/worker/go/dlq.go
//
// Dead-letter queue routing for the Go worker.
// Matches the Python dlq.py behaviour exactly.

package worker

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

const DefaultMaxClaimAttempts = 3

// GetClaimCount extracts _claim_count from work item fields.
func GetClaimCount(fields map[string]string) int {
	v, ok := fields["_claim_count"]
	if !ok {
		return 0
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0
	}
	return n
}

// ShouldRouteToDLQ returns true if the item has exceeded max claim attempts.
func ShouldRouteToDLQ(fields map[string]string, maxAttempts int) bool {
	return GetClaimCount(fields) >= maxAttempts
}

// IncrementClaimCount returns a copy of fields with _claim_count incremented.
func IncrementClaimCount(fields map[string]string) map[string]string {
	updated := make(map[string]string, len(fields))
	for k, v := range fields {
		updated[k] = v
	}
	updated["_claim_count"] = strconv.Itoa(GetClaimCount(fields) + 1)
	return updated
}

// RouteToDLQ writes the work item to the namespace DLQ stream.
func RouteToDLQ(
	ctx       context.Context,
	rdb       redis.UniversalClient,
	namespace string,
	streamID  string,
	fields    map[string]string,
	reason    string,
) error {
	dlqKey := fmt.Sprintf("%s.dlq", namespace)

	dlqFields := map[string]interface{}{
		"original_stream_id": streamID,
		"namespace":          namespace,
		"pipeline_id":        fields["pipeline_id"],
		"reason":             reason,
		"claim_count":        strconv.Itoa(GetClaimCount(fields)),
		"routed_at":          strconv.FormatInt(time.Now().UnixMilli(), 10),
	}

	err := rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: dlqKey,
		MaxLen: 10000,
		Approx: true,
		Values: dlqFields,
	}).Err()

	if err != nil {
		return fmt.Errorf("DLQ write failed: %w", err)
	}

	fmt.Printf(
		"[dlq] %s | message %s routed to DLQ after %d claims | reason: %s\n",
		namespace, streamID, GetClaimCount(fields), reason,
	)

	DLQTotal.WithLabelValues(namespace).Inc()
	return nil
}
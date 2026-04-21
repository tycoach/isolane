// isolane/worker/go/dispatcher.go
//
// Routes work items to the Python engine subprocess.
//
// The Go worker owns the Redis consumer loop, XAUTOCLAIM,
// and ACK logic. The Python engine owns schema validation,
// edge case checks, quarantine writes, staging writes, and dbt.
//

package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

// EngineResult is the JSON response from the Python engine subprocess.
type EngineResult struct {
	Status             string  `json:"status"`
	RecordsIn          int     `json:"records_in"`
	RecordsOut         int     `json:"records_out"`
	RecordsQuarantined int     `json:"records_quarantined"`
	DurationMs         float64 `json:"duration_ms"`
	ErrorMessage       string  `json:"error_message"`
}

// Dispatch calls the Python engine for a single work item.
// Returns a DispatchResult summarising the outcome.
func Dispatch(
	ctx context.Context,
	item *WorkItem,
	cfg *Config,
) *DispatchResult {
	start := time.Now()

	result := &DispatchResult{
		WorkItem: item,
	}

	// Call Python engine subprocess
	engineResult, err := callPythonEngine(ctx, item, cfg)
	if err != nil {
		result.Status = "failed"
		result.Error = err
		result.Duration = time.Since(start)
		fmt.Printf(
			"[dispatcher] %s.%s | engine error: %v\n",
			item.Namespace, item.PipelineID(), err,
		)
		return result
	}

	result.Status = engineResult.Status
	result.RecordsIn = engineResult.RecordsIn
	result.RecordsOut = engineResult.RecordsOut
	result.RecordsQuarantined = engineResult.RecordsQuarantined
	result.Duration = time.Since(start)

	if engineResult.ErrorMessage != "" {
		result.Error = fmt.Errorf("%s", engineResult.ErrorMessage)
	}

	// Update Prometheus metrics
	if result.RecordsOut > 0 {
		BatchProcessedTotal.WithLabelValues(item.PipelineID()).
			Add(float64(result.RecordsOut))
	}
	if result.RecordsQuarantined > 0 {
		QuarantineTotal.WithLabelValues(item.PipelineID()).
			Add(float64(result.RecordsQuarantined))
	}
	BatchDurationSeconds.WithLabelValues(item.PipelineID()).
		Observe(result.Duration.Seconds())

	fmt.Printf(
		"[dispatcher] %s.%s | %s | in=%d out=%d quarantined=%d duration=%dms\n",
		item.Namespace, item.PipelineID(), result.Status,
		result.RecordsIn, result.RecordsOut, result.RecordsQuarantined,
		result.Duration.Milliseconds(),
	)

	return result
}

// callPythonEngine invokes the Python engine as a subprocess.
func callPythonEngine(
	ctx context.Context,
	item *WorkItem,
	cfg *Config,
) (*EngineResult, error) {
	args := []string{
		"-m", "engine.dispatch",
		"--namespace", item.Namespace,
		"--pipeline", item.PipelineID(),
		"--stream-id", item.StreamID,
		"--batch-offset", item.BatchOffset(),
	}

	cmd := exec.CommandContext(ctx, "python3", args...)

	// Pass records via stdin to avoid shell argument length limits
	cmd.Stdin = strings.NewReader(item.RecordsJSON())

	// Set environment
	cmd.Env = buildEnv(cfg)

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		errMsg := stderr.String()
		stdoutMsg := stdout.String()
		if len(errMsg) > 1000 {
			errMsg = errMsg[len(errMsg)-1000:]
		}
		return nil, fmt.Errorf(
			"python engine exited %v\nSTDERR: %s\nSTDOUT: %s",
			err, errMsg, stdoutMsg,
		)
	}

	// Extract the last non-empty line — that's the JSON result
	// Previous lines are print() output from the engine (logs)
	outputLines := strings.Split(strings.TrimSpace(stdout.String()), "\n")
	var jsonLine string
	for i := len(outputLines) - 1; i >= 0; i-- {
		line := strings.TrimSpace(outputLines[i])
		if len(line) > 0 && line[0] == '{' {
			jsonLine = line
			break
		}
	}
	if jsonLine == "" {
		return nil, fmt.Errorf(
			"no JSON found in engine output: %s",
			stdout.String()[:min(500, len(stdout.String()))],
		)
	}

	var result EngineResult
	if err := json.Unmarshal([]byte(jsonLine), &result); err != nil {
		return nil, fmt.Errorf(
			"failed to parse engine output: %v (line: %s)",
			err, jsonLine,
		)
	}

	return &result, nil
}

func buildEnv(cfg *Config) []string {
	// Inherit the full process environment so PATH, HOME, etc. are available
	// then override with our specific values
	env := os.Environ()
	overrides := map[string]string{
		"PYTHONPATH":           "/app",
		"PYTHONUNBUFFERED":     "1",
		"POSTGRES_HOST":        cfg.PostgresHost,
		"POSTGRES_PORT":        fmt.Sprintf("%d", cfg.PostgresPort),
		"POSTGRES_USER":        cfg.PostgresUser,
		"POSTGRES_PASSWORD":    cfg.PostgresPass,
		"POSTGRES_DB":          cfg.PostgresDB,
		"REDIS_HOST":           cfg.RedisHost,
		"REDIS_PORT":           fmt.Sprintf("%d", cfg.RedisPort),
		"DBT_PROJECT_DIR":      cfg.DBTProjectDir,
		"ROCKSDB_PATH":         cfg.RocksDBPath,
		"TENANT_ROLE_PASSWORD": cfg.TenantRolePass,
	}

	// Apply overrides
	result := make([]string, 0, len(env)+len(overrides))
	seen := make(map[string]bool)
	for k := range overrides {
		seen[k] = true
	}
	for _, e := range env {
		key := e[:indexOf(e, '=')]
		if !seen[key] {
			result = append(result, e)
		}
	}
	for k, v := range overrides {
		result = append(result, fmt.Sprintf("%s=%s", k, v))
	}
	return result
}

func indexOf(s string, c byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == c {
			return i
		}
	}
	return len(s)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// ParseInt safely parses a string to int with a default.
func ParseInt(s string, def int) int {
	n, err := strconv.Atoi(s)
	if err != nil {
		return def
	}
	return n
}

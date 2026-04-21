// isolane/worker/go/cmd/worker/main.go
//
// Go worker entry point.
// Loads config from environment variables and starts the consumer.

package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	worker "github.com/tycoach/isolane/worker"
)

func main() {
	cfg := loadConfig()
	fmt.Printf("[worker] isolane Go worker starting — ID: %s\n", cfg.WorkerID)
	worker.RunWorker(cfg)
}

func loadConfig() *worker.Config {
	hostname, _ := os.Hostname()

	workerID := env("WORKER_ID", fmt.Sprintf("go-worker-%s", hostname))

	// Parse sentinel addresses
	var sentinelAddrs []string
	if addrs := env("REDIS_SENTINEL_ADDRS", ""); addrs != "" {
		for _, addr := range strings.Split(addrs, ",") {
			if a := strings.TrimSpace(addr); a != "" {
				sentinelAddrs = append(sentinelAddrs, a)
			}
		}
	}

	return &worker.Config{
		// Redis
		RedisHost:     env("REDIS_HOST", "redis-master"),
		RedisPort:     envInt("REDIS_PORT", 6379),
		SentinelAddrs: sentinelAddrs,
		MasterName:    env("REDIS_MASTER_NAME", "mtpif-master"),

		// PostgreSQL
		PostgresHost: env("POSTGRES_HOST", "postgres"),
		PostgresPort: envInt("POSTGRES_PORT", 5432),
		PostgresUser: env("POSTGRES_USER", "mtpif"),
		PostgresPass: env("POSTGRES_PASSWORD", "postgres"),
		PostgresDB:   env("POSTGRES_DB", "mtpif"),

		// Worker
		WorkerID:         workerID,
		MetricsPort:      envInt("WORKER_METRICS_PORT", 9090),
		ClaimTimeoutMs:   envInt("CLAIM_TIMEOUT_MS", 45000),
		MaxClaimAttempts: envInt("MAX_CLAIM_ATTEMPTS", 3),
		BatchCount:       envInt("BATCH_COUNT", 10),
		BlockMs:          envInt("BLOCK_MS", 2000),

		// Engine
		DBTProjectDir:  env("DBT_PROJECT_DIR", "/app/dbt"),
		RocksDBPath:    env("ROCKSDB_PATH", "/app/data/rocksdb"),
		TenantRolePass: env("TENANT_ROLE_PASSWORD", "change_me_dev"),
	}
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}
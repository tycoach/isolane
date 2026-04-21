// isolane/worker/go/metrics.go
//
// Prometheus metrics for the Go worker.
// Labels and metric names match the Python worker exactly
// so Grafana dashboards work for both.

package worker

import (
	"fmt"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	BatchProcessedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "ude_batch_records_processed_total",
			Help: "Total records processed.",
		},
		[]string{"pipeline"},
	)

	QuarantineTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "ude_quarantine_total",
			Help: "Total records routed to quarantine.",
		},
		[]string{"pipeline"},
	)

	BatchDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "ude_batch_duration_seconds",
			Help:    "Batch processing duration.",
			Buckets: []float64{0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0},
		},
		[]string{"pipeline"},
	)

	DLQTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "ude_dlq_total",
			Help: "Total messages routed to DLQ.",
		},
		[]string{"namespace"},
	)

	XAutoclaimCyclesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "ude_xautoclaim_cycles_total",
			Help: "Total XAUTOCLAIM cycles run.",
		},
		[]string{"namespace"},
	)

	ActiveNamespaces = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "ude_active_namespaces",
			Help: "Number of namespaces currently being consumed.",
		},
	)
)

func init() {
	prometheus.MustRegister(
		BatchProcessedTotal,
		QuarantineTotal,
		BatchDurationSeconds,
		DLQTotal,
		XAutoclaimCyclesTotal,
		ActiveNamespaces,
	)
}

// StartMetricsServer starts the Prometheus HTTP metrics server.
func StartMetricsServer(port int) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "ok")
	})

	addr := fmt.Sprintf(":%d", port)
	go func() {
		if err := http.ListenAndServe(addr, mux); err != nil {
			panic(fmt.Sprintf("[metrics] Server failed: %v", err))
		}
	}()
	fmt.Printf("[metrics] Server started on %s\n", addr)
}

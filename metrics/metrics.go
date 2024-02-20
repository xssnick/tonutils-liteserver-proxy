package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

type Metrics struct {
	ActiveADNLConnections prometheus.Gauge
	Requests              *prometheus.CounterVec
	LSErrors              *prometheus.CounterVec
	Queries               *prometheus.HistogramVec
	BackendQueries        *prometheus.HistogramVec
}

var Global *Metrics

func InitMetrics(namespace, subsystem string) {
	Global = &Metrics{
		ActiveADNLConnections: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "adnl_connections",
			Help:      "Active ADNL TCP connections with clients",
		}),
		Requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "adnl_requests",
			Help:      "Raw ADNL requests",
		}, []string{"key_name", "request_type"}),
		LSErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "ls_errors",
			Help:      "LSError responses count",
		}, []string{"key_name", "request_type", "code"}),
		Queries: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "ls_queries",
			Help:      "LS Requests to proxy statistics",
		}, []string{"key_name", "request_type", "hit_type"}),
		BackendQueries: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "backend_queries",
			Help:      "LS Requests to backend statistics",
		}, []string{"name", "request_type", "status"}),
	}
}

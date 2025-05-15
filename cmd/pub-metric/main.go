package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/xssnick/tonutils-go/liteclient"
	"github.com/xssnick/tonutils-go/ton"
)

var (
	Verbosity        = flag.Int("verbosity", 2, "3 = debug, 2 = info, 1 = warn, 0 = error")
	ConfigPath       = flag.String("config", "global.config.json", "ton config path")
	MetricsPort      = flag.Int("metrics-port", 8080, "Port for metrics server")
	RetryDelay       = flag.Int("retry-delay", 10, "Delay in seconds between retries")
	MetricsNamespace = flag.String("metrics-namespace", "pub", "Namespace for metrics")
)

type Metrics struct {
	ActiveServers prometheus.Gauge
	Queries       *prometheus.HistogramVec
	Fails         *prometheus.CounterVec
	ServerState   *prometheus.GaugeVec
	ServerLag     *prometheus.GaugeVec
}

func main() {
	flag.Parse()
	liteclient.Logger = func(v ...any) {}

	log.Logger = zerolog.New(zerolog.NewConsoleWriter()).With().Timestamp().Logger().Level(zerolog.InfoLevel)

	switch *Verbosity {
	case 3:
		log.Logger = log.Logger.Level(zerolog.DebugLevel).With().Logger()
		liteclient.Logger = func(v ...any) {
			log.Logger.Debug().Str("type", "LITECLIENT").Msg(fmt.Sprint(v...))
		}
	case 2:
		log.Logger = log.Logger.Level(zerolog.InfoLevel).With().Logger()
	case 1:
		log.Logger = log.Logger.Level(zerolog.WarnLevel).With().Logger()
	case 0:
		log.Logger = log.Logger.Level(zerolog.ErrorLevel).With().Logger()
	}

	// Инициализация метрик
	m := &Metrics{
		ActiveServers: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: *MetricsNamespace,
			Subsystem: "pub_ls",
			Name:      "active_servers",
			Help:      "Active liteservers",
		}),
		Queries: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: *MetricsNamespace,
			Subsystem: "pub_ls",
			Name:      "queries",
			Help:      "Queries stats",
		}, []string{"addr"}),
		Fails: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: *MetricsNamespace,
			Subsystem: "pub_ls",
			Name:      "fails",
			Help:      "Fails",
		}, []string{"addr", "reason"}),
		ServerState: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: *MetricsNamespace,
			Subsystem: "pub_ls",
			Name:      "server_state",
			Help:      "Server state",
		}, []string{"addr"}),
		ServerLag: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: *MetricsNamespace,
			Subsystem: "pub_ls",
			Name:      "server_lag",
			Help:      "Server lag",
		}, []string{"addr"}),
	}

	gCfg, err := liteclient.GetConfigFromFile(*ConfigPath)
	if err != nil {
		log.Fatal().Err(err).Msg("load config err")
	}

	go func() {
		lastSeqno := float64(0)
		seqnos := map[string]float64{}

		var mx sync.Mutex
		states := map[string]float64{}

		for {

			wCtx, cancel := context.WithTimeout(context.Background(), time.Duration(*RetryDelay)*time.Second)

			for _, ls := range gCfg.Liteservers {
				addr := fmt.Sprintf("%s:%d", intToIP4(ls.IP), ls.Port)
				states[addr] = -2

				go func(ls liteclient.LiteserverConfig) {
					conn := liteclient.NewConnectionPool()
					defer conn.Stop()

					startTime := time.Now()
					err := conn.AddConnection(wCtx, addr, ls.ID.Key)
					if err != nil {
						mx.Lock()
						states[addr] = -1
						mx.Unlock()
						m.Fails.WithLabelValues(addr, "conn").Inc()
						log.Debug().Err(err).Str("addr", addr).Msg("Failed to add connection")
						return
					}

					api := ton.NewAPIClient(conn)
					info, err := api.GetMasterchainInfo(wCtx)
					duration := time.Since(startTime)
					if err != nil {
						mx.Lock()
						states[addr] = -1
						mx.Unlock()
						m.Fails.WithLabelValues(addr, "query").Inc()
						log.Debug().Err(err).Str("addr", addr).Msg("Failed to get masterchain info")
						return
					}

					m.Queries.WithLabelValues(addr).Observe(duration.Seconds())
					mx.Lock()
					states[addr] = duration.Seconds()
					seqnos[addr] = float64(info.SeqNo)
					mx.Unlock()
					log.Debug().Str("addr", addr).Dur("duration", duration).Msg("GetMasterchainInfo")
				}(ls)
			}

			time.Sleep(time.Duration(*RetryDelay) * time.Second)
			cancel()

			active := 0
			dead := 0

			mx.Lock()
			for addr, state := range states {
				m.ServerState.WithLabelValues(addr).Set(state)
				if state >= 0 {
					active++
				} else if state < 0 {
					dead++
				}
			}

			for _, seqno := range seqnos {
				if seqno > lastSeqno {
					lastSeqno = seqno
				}
			}

			for addr, seqno := range seqnos {
				m.ServerLag.WithLabelValues(addr).Set(lastSeqno - seqno)
			}
			mx.Unlock()

			m.ActiveServers.Set(float64(active))
			log.Info().Int64("active", int64(active)).Int64("dead", int64(dead)).Msg("Servers")
		}
	}()

	prometheus.MustRegister(m.ActiveServers)
	prometheus.MustRegister(m.Queries)
	prometheus.MustRegister(m.Fails)
	prometheus.MustRegister(m.ServerState)
	prometheus.MustRegister(m.ServerLag)

	http.Handle("/metrics", promhttp.Handler())
	metricsAddr := fmt.Sprintf(":%d", *MetricsPort)
	log.Info().Msgf("Serving metrics on %s", metricsAddr)
	if err := http.ListenAndServe(metricsAddr, nil); err != nil {
		log.Fatal().Err(err).Msg("Error starting metrics server")
	}
}

func intToIP4(ipInt int64) string {
	b0 := strconv.FormatInt((ipInt>>24)&0xff, 10)
	b1 := strconv.FormatInt((ipInt>>16)&0xff, 10)
	b2 := strconv.FormatInt((ipInt>>8)&0xff, 10)
	b3 := strconv.FormatInt((ipInt & 0xff), 10)
	return b0 + "." + b1 + "." + b2 + "." + b3
}

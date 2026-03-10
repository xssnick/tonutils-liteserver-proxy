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

	prometheus.MustRegister(m.ActiveServers, m.Queries, m.Fails, m.ServerState, m.ServerLag)

	gCfg, err := liteclient.GetConfigFromFile(*ConfigPath)
	if err != nil {
		log.Fatal().Err(err).Msg("load config err")
	}

	http.Handle("/metrics", promhttp.Handler())
	metricsAddr := fmt.Sprintf(":%d", *MetricsPort)
	go func() {
		log.Info().Msgf("Serving metrics on %s", metricsAddr)
		if err := http.ListenAndServe(metricsAddr, nil); err != nil {
			log.Fatal().Err(err).Msg("Error starting metrics server")
		}
	}()

	var (
		mx      sync.Mutex
		lastSeq float64
		ticker  = time.NewTicker(time.Duration(*RetryDelay) * time.Second)
	)
	defer ticker.Stop()

	for {
		states := make(map[string]float64, len(gCfg.Liteservers))
		seqnos := make(map[string]float64, len(gCfg.Liteservers))

		var wg sync.WaitGroup

		for _, ls0 := range gCfg.Liteservers {
			ls := ls0
			addr := fmt.Sprintf("%s:%d", intToIP4(ls.IP), ls.Port)

			mx.Lock()
			states[addr] = -2
			mx.Unlock()

			wg.Add(1)
			go func(addr string, ls liteclient.LiteserverConfig) {
				defer wg.Done()

				ctx, cancel := context.WithTimeout(context.Background(), time.Duration(*RetryDelay)*time.Second)
				defer cancel()

				conn := liteclient.NewConnectionPool()
				defer conn.Stop()

				start := time.Now()
				if err := conn.AddConnection(ctx, addr, ls.ID.Key); err != nil {
					mx.Lock()
					states[addr] = -1
					mx.Unlock()
					m.Fails.WithLabelValues(addr, "conn").Inc()
					log.Debug().Err(err).Str("addr", addr).Msg("Failed to add connection")
					return
				}

				api := ton.NewAPIClient(conn)
				info, err := api.GetMasterchainInfo(ctx)
				dur := time.Since(start)
				if err != nil {
					mx.Lock()
					states[addr] = -1
					mx.Unlock()
					m.Fails.WithLabelValues(addr, "query").Inc()
					log.Debug().Err(err).Str("addr", addr).Msg("Failed to get masterchain info")
					return
				}

				m.Queries.WithLabelValues(addr).Observe(dur.Seconds())
				mx.Lock()
				states[addr] = dur.Seconds()
				seqnos[addr] = float64(info.SeqNo)
				if seqnos[addr] > lastSeq {
					lastSeq = seqnos[addr]
				}
				mx.Unlock()

				log.Debug().Str("addr", addr).Dur("duration", dur).Msg("GetMasterchainInfo")
			}(addr, ls)
		}

		wg.Wait()

		active := 0
		dead := 0

		mx.Lock()
		for addr, state := range states {
			m.ServerState.WithLabelValues(addr).Set(state)
			if state >= 0 {
				active++
			} else {
				dead++
			}
		}

		for addr, seq := range seqnos {
			m.ServerLag.WithLabelValues(addr).Set(lastSeq - seq)
		}
		mx.Unlock()

		m.ActiveServers.Set(float64(active))
		log.Info().
			Int("active", active).
			Int("dead", dead).
			Msg("Liteservers status")

		<-ticker.C
	}
}

func intToIP4(ipInt int64) string {
	ip := uint32(ipInt)
	b0 := strconv.Itoa(int(byte(ip >> 24)))
	b1 := strconv.Itoa(int(byte(ip >> 16)))
	b2 := strconv.Itoa(int(byte(ip >> 8)))
	b3 := strconv.Itoa(int(byte(ip)))
	return b0 + "." + b1 + "." + b2 + "." + b3
}

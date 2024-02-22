package main

import (
	"crypto/ed25519"
	"encoding/base64"
	"flag"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/xssnick/tonutils-go/liteclient"
	_ "github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-liteserver-proxy/config"
	"github.com/xssnick/tonutils-liteserver-proxy/internal/server"
	"github.com/xssnick/tonutils-liteserver-proxy/metrics"
	"net/http"
	"time"
)

var (
	Verbosity = flag.Int("verbosity", 2, "3 = debug, 2 = info, 1 = warn, 0 = error")
)

func main() {
	flag.Parse()
	liteclient.Logger = func(v ...any) {}

	log.Logger = zerolog.New(zerolog.NewConsoleWriter()).With().Timestamp().Logger().Level(zerolog.InfoLevel)

	switch *Verbosity {
	case 3:
		log.Logger = log.Logger.Level(zerolog.DebugLevel).With().Logger()
	case 2:
		log.Logger = log.Logger.Level(zerolog.InfoLevel).With().Logger()
	case 1:
		log.Logger = log.Logger.Level(zerolog.WarnLevel).With().Logger()
	case 0:
		log.Logger = log.Logger.Level(zerolog.ErrorLevel).With().Logger()
	}

	cfg, err := config.LoadConfig("ls-proxy-config.json")
	if err != nil {
		log.Fatal().Err(err).Msg("failed to load config")
		return
	}

	metrics.InitMetrics(cfg.MetricsNamespace, "tonutils_ls_proxy")

	if len(cfg.Backends) == 0 {
		log.Fatal().Msg("no backends specified")
	}

	for i, clientConfig := range cfg.Clients {
		key := ed25519.NewKeyFromSeed(clientConfig.PrivateKey)
		log.Info().Int("i", i).Str("pub_key", base64.StdEncoding.EncodeToString(key.Public().(ed25519.PublicKey))).Msg("liteserver initialized")
	}

	blc, err := server.NewBackendBalancer(cfg.Backends, server.BalancerTypeFailOver)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to init backend balancer")
		return
	}

	var cache *server.BlockCache
	if !cfg.DisableEmulationAndCache {
		cache = server.NewBlockCache(cfg.CacheConfig, blc)
	}

	go func() {
		http.Handle("/metrics", promhttp.Handler())
		if err := http.ListenAndServe(cfg.MetricsAddr, nil); err != nil {
			log.Fatal().Err(err).Msg("listen metrics failed")
		}
	}()

	log.Info().Str("addr", cfg.ListenAddr).Msg("listening tcp")
	proxy := server.NewProxyBalancer(cfg.Clients, blc, cache,
		cfg.DisableEmulationAndCache, int(cfg.MaxConnectionsPerIP), time.Duration(cfg.MaxKeepAliveSeconds)*time.Second)
	if err = proxy.Listen(cfg.ListenAddr); err != nil {
		log.Fatal().Err(err).Msg("listen failed")
		return
	}
}

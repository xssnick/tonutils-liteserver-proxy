package server

import (
	"context"
	"encoding/base64"
	"fmt"
	"github.com/rs/zerolog/log"
	"github.com/xssnick/tonutils-go/liteclient"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-liteserver-proxy/config"
	"github.com/xssnick/tonutils-liteserver-proxy/metrics"
	"reflect"
	"strings"
	"sync/atomic"
	"time"
)

type BalancerType string

const (
	BalancerTypeRoundRobin = "round_robin"
	BalancerTypeFailOver   = "fail_over"
	// TODO: req hash balancer, ip/key balancer, weighted
)

type Backend struct {
	Name   string
	Client *liteclient.ConnectionPool
	Weight uint64

	failsStreak uint64
	lastRequest int64
	lastSuccess int64
}

type BackendBalancer struct {
	backends []Backend

	balancerType BalancerType
	counter      uint64
}

func NewBackendBalancer(backends []config.BackendLiteserver, typ BalancerType) (*BackendBalancer, error) {
	var b BackendBalancer
	b.balancerType = typ
	for _, backend := range backends {
		client := liteclient.NewConnectionPool()
		if err := client.AddConnection(context.Background(), backend.Addr, base64.StdEncoding.EncodeToString(backend.Key)); err != nil {
			log.Error().Err(err).Str("backend", backend.Addr).Msg("failed to connect")
			continue
		}

		b.backends = append(b.backends, Backend{
			Name:   backend.Name,
			Client: client,
		})
		log.Info().Str("backend", backend.Addr).Msg("connected to backend")
	}

	if len(b.backends) == 0 {
		return nil, fmt.Errorf("no active backends")
	}
	return &b, nil
}

func (b *BackendBalancer) GetClient() ton.LiteClient {
	switch b.balancerType {
	case BalancerTypeFailOver:
		for _, backend := range b.backends {
			if atomic.LoadUint64(&backend.failsStreak) > 10 &&
				atomic.LoadInt64(&backend.lastRequest)-atomic.LoadInt64(&backend.lastSuccess) > 5 {
				// failed node
				continue
			}
			return &backend
		}

		// all nodes failed over switch to round-robin, and maybe it will become alive
		fallthrough
	case BalancerTypeRoundRobin:
		x := atomic.AddUint64(&b.counter, 1)
		return &b.backends[x%uint64(len(b.backends))]
	default:
		panic("unknown balancer type:" + b.balancerType)
	}
}

func (b *Backend) QueryLiteserver(ctx context.Context, payload tl.Serializable, result tl.Serializable) (err error) {
	tm := time.Now()
	defer func() {
		if _, ok := payload.([]tl.Serializable); ok {
			// don't track waitMaster for clear stats
			return
		}

		atomic.StoreInt64(&b.lastRequest, time.Now().Unix())
		status := "ok"
		if err != nil {
			if strings.HasSuffix(err.Error(), "context canceled") {
				// we don't consider canceled context as an error
				return
			}
			atomic.AddUint64(&b.failsStreak, 1)
			status = "failed"
			log.Debug().Err(err).Str("name", b.Name).Msg("backend query failed")
		} else if ls, ok := result.(ton.LSError); ok {
			atomic.AddUint64(&b.failsStreak, 1)
			status = "ls_error"
			log.Debug().Str("name", b.Name).Str("reason", ls.Text).Int32("code", ls.Code).Msg("backend query ls error")
		} else {
			atomic.StoreUint64(&b.failsStreak, 0)
			atomic.StoreInt64(&b.lastSuccess, atomic.LoadInt64(&b.lastRequest))
		}

		metrics.Global.BackendQueries.WithLabelValues(b.Name, reflect.TypeOf(payload).String(), status).Observe(time.Since(tm).Seconds())
	}()

	if dl, ok := ctx.Deadline(); !ok || dl.After(time.Now().Add(10*time.Second)) {
		var cancel func()
		ctx, cancel = context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
	}

	if err = b.Client.QueryLiteserver(ctx, payload, result); err != nil {
		return err
	}
	return nil
}

func (b *Backend) StickyContext(ctx context.Context) context.Context {
	return b.Client.StickyContext(ctx)
}

func (b *Backend) StickyContextNextNode(ctx context.Context) (context.Context, error) {
	return b.Client.StickyContextNextNode(ctx)
}

func (b *Backend) StickyNodeID(ctx context.Context) uint32 {
	return b.Client.StickyNodeID(ctx)
}

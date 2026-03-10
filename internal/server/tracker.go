package server

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/xssnick/tonutils-go/ton"
)

type liteClientSource interface {
	GetClient() ton.LiteClient
}

type MasterSeqnoTracker struct {
	clientSource         liteClientSource
	ready                atomic.Bool
	seqno                atomic.Uint32
	fetchMasterchainInfo func(context.Context, ton.LiteClient, uint32) (*ton.MasterchainInfo, error)
	retryDelay           time.Duration
}

func NewMasterSeqnoTracker(clientSource liteClientSource) *MasterSeqnoTracker {
	t := &MasterSeqnoTracker{
		clientSource:         clientSource,
		fetchMasterchainInfo: getMasterchainInfo,
		retryDelay:           time.Second,
	}

	go t.run()

	return t
}

func (t *MasterSeqnoTracker) CurrentMasterSeqno() (uint32, bool) {
	if !t.ready.Load() {
		return 0, false
	}
	return t.seqno.Load(), true
}

func (t *MasterSeqnoTracker) poll(ctx context.Context, waitSeqno uint32) (*ton.MasterchainInfo, error) {
	return t.fetchMasterchainInfo(ctx, t.clientSource.GetClient(), waitSeqno)
}

func (t *MasterSeqnoTracker) run() {
	var waitSeqno, streak uint32
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		inf, err := t.poll(ctx, waitSeqno)
		cancel()
		if err != nil {
			if streak > 3 {
				log.Warn().Err(err).Msg("standalone master seqno tracker failed, retrying in 1s")
			}
			streak++
			time.Sleep(t.retryDelay)
			continue
		}

		streak = 0
		if inf == nil || inf.Last == nil {
			time.Sleep(t.retryDelay)
			continue
		}

		t.seqno.Store(inf.Last.SeqNo)
		t.ready.Store(true)
		waitSeqno = inf.Last.SeqNo + 1
	}
}

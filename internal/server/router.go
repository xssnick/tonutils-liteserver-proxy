package server

import (
	"fmt"
	"strings"
	"sync"

	"github.com/rs/zerolog/log"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-liteserver-proxy/config"
)

const (
	BackendRoleFast    = "fast"
	BackendRoleArchive = "archive"
)

type MasterSeqnoSource interface {
	CurrentMasterSeqno() (uint32, bool)
}

type BackendRouter struct {
	fast             *BackendBalancer
	archive          *BackendBalancer
	archiveThreshold uint32

	mx          sync.RWMutex
	seqnoSource MasterSeqnoSource
}

func NewBackendRouter(backends []config.BackendLiteserver, typ BalancerType) (*BackendRouter, error) {
	var fastBackends, archiveBackends []config.BackendLiteserver
	for _, backend := range backends {
		switch role := strings.ToLower(strings.TrimSpace(backend.Role)); role {
		case "", BackendRoleFast:
			fastBackends = append(fastBackends, backend)
		case BackendRoleArchive:
			archiveBackends = append(archiveBackends, backend)
		default:
			return nil, fmt.Errorf("unknown backend role %q for %s", backend.Role, backend.Addr)
		}
	}

	if len(fastBackends) == 0 {
		return nil, fmt.Errorf("no fast backends configured")
	}

	fast, err := NewBackendBalancer(fastBackends, typ)
	if err != nil {
		return nil, fmt.Errorf("failed to init fast backend balancer: %w", err)
	}

	var archive *BackendBalancer
	if len(archiveBackends) > 0 {
		archive, err = NewBackendBalancer(archiveBackends, typ)
		if err != nil {
			return nil, fmt.Errorf("failed to init archive backend balancer: %w", err)
		}
	}

	return &BackendRouter{
		fast:    fast,
		archive: archive,
	}, nil
}

func (r *BackendRouter) SetSeqnoSource(src MasterSeqnoSource) {
	r.mx.Lock()
	r.seqnoSource = src
	r.mx.Unlock()
}

func (r *BackendRouter) SetArchiveThreshold(threshold uint32) {
	r.mx.Lock()
	r.archiveThreshold = threshold
	r.mx.Unlock()
}

func (r *BackendRouter) ArchiveThreshold() uint32 {
	r.mx.RLock()
	defer r.mx.RUnlock()
	return r.archiveThreshold
}

func (r *BackendRouter) GetClient() ton.LiteClient {
	return r.fast.GetClient()
}

func (r *BackendRouter) SelectClientForPayload(payload tl.Serializable) (ton.LiteClient, bool) {
	if seqno, ok := payloadMinSeqno(payload); ok {
		return r.SelectClientBySeqno(seqno)
	}
	return r.fast.GetClient(), false
}

func (r *BackendRouter) SelectClientByBlockID(id *ton.BlockIDExt) (ton.LiteClient, bool) {
	if id == nil {
		return r.fast.GetClient(), false
	}
	return r.SelectClientBySeqno(id.SeqNo)
}

func (r *BackendRouter) SelectClientByBlockShort(id *ton.BlockInfoShort) (ton.LiteClient, bool) {
	if id == nil || id.Seqno < 0 {
		return r.fast.GetClient(), false
	}
	return r.SelectClientBySeqno(uint32(id.Seqno))
}

func (r *BackendRouter) SelectClientBySeqno(seqno uint32) (ton.LiteClient, bool) {
	threshold := r.ArchiveThreshold()
	if !r.shouldUseArchive(seqno, threshold) {
		return r.fast.GetClient(), false
	}

	log.Debug().Uint32("seqno", seqno).Uint32("threshold", threshold).Msg("routing request to archive backend")
	return r.archive.GetClient(), true
}

func (r *BackendRouter) shouldUseArchive(seqno uint32, threshold uint32) bool {
	if r.archive == nil || threshold == 0 {
		return false
	}

	currentSeqno, ok := r.currentMasterSeqno()
	if !ok || currentSeqno < seqno {
		return false
	}

	return currentSeqno-seqno >= threshold
}

func (r *BackendRouter) currentMasterSeqno() (uint32, bool) {
	r.mx.RLock()
	src := r.seqnoSource
	r.mx.RUnlock()
	if src == nil {
		return 0, false
	}
	return src.CurrentMasterSeqno()
}

func payloadMinSeqno(payload tl.Serializable) (uint32, bool) {
	switch v := payload.(type) {
	case ton.GetBlockData:
		return seqnoFromBlockID(v.ID)
	case *ton.GetBlockData:
		return seqnoFromBlockID(v.ID)
	case ton.GetAccountState:
		return seqnoFromBlockID(v.ID)
	case *ton.GetAccountState:
		return seqnoFromBlockID(v.ID)
	case ton.GetAccountStatePruned:
		return seqnoFromBlockID(v.ID)
	case *ton.GetAccountStatePruned:
		return seqnoFromBlockID(v.ID)
	case ton.GetOneTransaction:
		return seqnoFromBlockID(v.ID)
	case *ton.GetOneTransaction:
		return seqnoFromBlockID(v.ID)
	case ton.GetConfigAll:
		return seqnoFromBlockID(v.BlockID)
	case *ton.GetConfigAll:
		return seqnoFromBlockID(v.BlockID)
	case ton.GetConfigParams:
		return seqnoFromBlockID(v.BlockID)
	case *ton.GetConfigParams:
		return seqnoFromBlockID(v.BlockID)
	case ton.GetState:
		return seqnoFromBlockID(v.ID)
	case *ton.GetState:
		return seqnoFromBlockID(v.ID)
	case ton.GetShardBlockProof:
		return seqnoFromBlockID(v.ID)
	case *ton.GetShardBlockProof:
		return seqnoFromBlockID(v.ID)
	case ton.GetShardInfo:
		return seqnoFromBlockID(v.ID)
	case *ton.GetShardInfo:
		return seqnoFromBlockID(v.ID)
	case ton.GetBlockHeader:
		return seqnoFromBlockID(v.ID)
	case *ton.GetBlockHeader:
		return seqnoFromBlockID(v.ID)
	case ton.GetAllShardsInfo:
		return seqnoFromBlockID(v.ID)
	case *ton.GetAllShardsInfo:
		return seqnoFromBlockID(v.ID)
	case ton.ListBlockTransactions:
		return seqnoFromBlockID(v.ID)
	case *ton.ListBlockTransactions:
		return seqnoFromBlockID(v.ID)
	case ton.ListBlockTransactionsExt:
		return seqnoFromBlockID(v.ID)
	case *ton.ListBlockTransactionsExt:
		return seqnoFromBlockID(v.ID)
	case ton.RunSmcMethod:
		return seqnoFromBlockID(v.ID)
	case *ton.RunSmcMethod:
		return seqnoFromBlockID(v.ID)
	case ton.LookupBlock:
		return seqnoFromBlockShort(v.ID)
	case *ton.LookupBlock:
		return seqnoFromBlockShort(v.ID)
	case ton.GetBlockProof:
		return minKnownSeqno(v.KnownBlock, v.TargetBlock)
	case *ton.GetBlockProof:
		return minKnownSeqno(v.KnownBlock, v.TargetBlock)
	case ton.GetTransactions:
		return 0, false
	case *ton.GetTransactions:
		return 0, false
	default:
		return 0, false
	}
}

func seqnoFromBlockID(id *ton.BlockIDExt) (uint32, bool) {
	if id == nil {
		return 0, false
	}
	return id.SeqNo, true
}

func seqnoFromBlockShort(id *ton.BlockInfoShort) (uint32, bool) {
	if id == nil || id.Seqno < 0 {
		return 0, false
	}
	return uint32(id.Seqno), true
}

func minKnownSeqno(ids ...*ton.BlockIDExt) (uint32, bool) {
	var (
		min uint32
		ok  bool
	)
	for _, id := range ids {
		if id == nil {
			continue
		}
		if !ok || id.SeqNo < min {
			min = id.SeqNo
			ok = true
		}
	}
	return min, ok
}

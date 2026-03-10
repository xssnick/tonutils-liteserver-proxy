package server

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/ton"
)

type BackendStorageDepth struct {
	FastSeqno             uint32
	ArchiveSeqno          uint32
	Name                  string
	CurrentSeqno          uint32
	OldestAvailableSeqno  uint32
	FirstUnavailableSeqno uint32
	AvailableDepth        uint32
	RouteThreshold        uint32
}

func (r *BackendRouter) DetectArchiveThreshold(ctx context.Context) error {
	if r.archive == nil {
		r.SetArchiveThreshold(0)
		log.Info().Msg("archive routing disabled: no archive backends configured")
		return nil
	}

	archiveCurrentSeqno, err := currentMasterSeqno(ctx, r.archive.GetClient())
	if err != nil {
		return fmt.Errorf("failed to fetch archive current master seqno: %w", err)
	}

	var depths []BackendStorageDepth
	for i := range r.fast.backends {
		depth, err := r.detectFastBackendStorageDepth(ctx, &r.fast.backends[i], archiveCurrentSeqno)
		if err != nil {
			return fmt.Errorf("failed to detect storage depth for fast backend %s: %w", r.fast.backends[i].Name, err)
		}
		depths = append(depths, depth)
		log.Info().
			Str("backend", depth.Name).
			Uint32("fast_seqno", depth.FastSeqno).
			Uint32("archive_seqno", depth.ArchiveSeqno).
			Uint32("current_seqno", depth.CurrentSeqno).
			Uint32("oldest_available_seqno", depth.OldestAvailableSeqno).
			Uint32("first_unavailable_seqno", depth.FirstUnavailableSeqno).
			Uint32("available_depth", depth.AvailableDepth).
			Uint32("route_threshold", depth.RouteThreshold).
			Msg("detected fast backend storage depth")
	}

	threshold := effectiveArchiveThreshold(depths)
	r.SetArchiveThreshold(threshold)
	if threshold == 0 {
		log.Info().Msg("archive routing disabled: all fast backends appear to have full history")
		return nil
	}

	log.Info().
		Uint32("threshold", threshold).
		Int("fast_backends", len(depths)).
		Msg("archive routing threshold detected from fast backend storage depth")
	return nil
}

func (r *BackendRouter) detectFastBackendStorageDepth(ctx context.Context, backend *Backend, archiveCurrentSeqno uint32) (BackendStorageDepth, error) {
	fastCurrentSeqno, err := currentMasterSeqno(ctx, backend)
	if err != nil {
		return BackendStorageDepth{}, err
	}
	currentSeqno := minUint32(fastCurrentSeqno, archiveCurrentSeqno)
	if currentSeqno == 0 {
		return BackendStorageDepth{}, fmt.Errorf("common current seqno is zero")
	}

	oldestAvailable, firstUnavailable, err := findStorageBoundary(currentSeqno, func(seqno uint32) (bool, error) {
		return r.fastHasBlockData(ctx, backend, seqno)
	})
	if err != nil {
		return BackendStorageDepth{}, err
	}

	depth := BackendStorageDepth{
		FastSeqno:             fastCurrentSeqno,
		ArchiveSeqno:          archiveCurrentSeqno,
		Name:                  backend.Name,
		CurrentSeqno:          currentSeqno,
		OldestAvailableSeqno:  oldestAvailable,
		FirstUnavailableSeqno: firstUnavailable,
		AvailableDepth:        currentSeqno - oldestAvailable,
		RouteThreshold:        routeThresholdFromBoundary(currentSeqno, firstUnavailable),
	}
	return depth, nil
}

func currentMasterSeqno(ctx context.Context, client ton.LiteClient) (uint32, error) {
	infoCtx, cancel := context.WithTimeout(ctx, 12*time.Second)
	defer cancel()

	info, err := getMasterchainInfo(infoCtx, client, 0)
	if err != nil {
		return 0, err
	}
	if info == nil || info.Last == nil {
		return 0, fmt.Errorf("masterchain info is empty")
	}
	return info.Last.SeqNo, nil
}

func (r *BackendRouter) fastHasBlockData(ctx context.Context, backend *Backend, seqno uint32) (bool, error) {
	lookupCtx, cancel := context.WithTimeout(ctx, 12*time.Second)
	id, err := lookupMasterBlockID(lookupCtx, r.archive.GetClient(), seqno)
	cancel()
	if err != nil {
		return false, err
	}

	blockCtx, cancel := context.WithTimeout(ctx, 12*time.Second)
	_, _, err = getBlock(blockCtx, backend, id)
	cancel()
	if err == nil {
		return true, nil
	}
	if isLiteserverErr(err) {
		return false, nil
	}
	return false, err
}

func findStorageBoundary(current uint32, isAvailable func(seqno uint32) (bool, error)) (uint32, uint32, error) {
	if current == 0 {
		return 0, 0, fmt.Errorf("current seqno must be greater than 0")
	}

	ok, err := isAvailable(current)
	if err != nil {
		return 0, 0, err
	}
	if !ok {
		return 0, 0, fmt.Errorf("current seqno %d is not available", current)
	}

	oldestAvailable := current
	var firstUnavailable uint32

	for diff := uint32(128); ; {
		candidate := current - minUint32(diff, current-1)
		ok, err = isAvailable(candidate)
		if err != nil {
			return 0, 0, err
		}
		if ok {
			oldestAvailable = candidate
			if candidate == 1 {
				return 1, 0, nil
			}
		} else {
			firstUnavailable = candidate
			break
		}

		if candidate == 1 {
			return 1, 0, nil
		}
		diff *= 2
	}

	for oldestAvailable-firstUnavailable > 1 {
		candidate := firstUnavailable + (oldestAvailable-firstUnavailable)/2
		ok, err = isAvailable(candidate)
		if err != nil {
			return 0, 0, err
		}
		if ok {
			oldestAvailable = candidate
			continue
		}
		firstUnavailable = candidate
	}

	return oldestAvailable, firstUnavailable, nil
}

func effectiveArchiveThreshold(depths []BackendStorageDepth) uint32 {
	var threshold uint32
	for _, depth := range depths {
		if depth.RouteThreshold == 0 {
			continue
		}
		if threshold == 0 || depth.RouteThreshold < threshold {
			threshold = depth.RouteThreshold
		}
	}
	return threshold
}

func routeThresholdFromBoundary(current uint32, firstUnavailable uint32) uint32 {
	if firstUnavailable == 0 || firstUnavailable > current {
		return 0
	}
	return current - firstUnavailable
}

func lookupMasterBlockID(ctx context.Context, client ton.LiteClient, seqno uint32) (*ton.BlockIDExt, error) {
	var resp tl.Serializable
	if err := client.QueryLiteserver(ctx, ton.LookupBlock{
		Mode: 1,
		ID: &ton.BlockInfoShort{
			Workchain: -1,
			Shard:     math.MinInt64,
			Seqno:     int32(seqno),
		},
	}, &resp); err != nil {
		return nil, err
	}

	switch t := resp.(type) {
	case ton.BlockHeader:
		return t.ID, nil
	case ton.LSError:
		return nil, t
	default:
		return nil, fmt.Errorf("unexpected lookup response %T", resp)
	}
}

func isLiteserverErr(err error) bool {
	for err != nil {
		switch err.(type) {
		case ton.LSError, *ton.LSError:
			return true
		}
		err = errors.Unwrap(err)
	}
	return false
}

func minUint32(a uint32, b uint32) uint32 {
	if a < b {
		return a
	}
	return b
}

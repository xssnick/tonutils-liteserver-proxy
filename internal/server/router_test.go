package server

import (
	"sync/atomic"
	"testing"

	"github.com/xssnick/tonutils-go/ton"
)

type staticSeqnoSource struct {
	seqno uint32
	ok    bool
}

func (s staticSeqnoSource) CurrentMasterSeqno() (uint32, bool) {
	return s.seqno, s.ok
}

func testRouter(threshold uint32, current uint32, withArchive bool) *BackendRouter {
	router := &BackendRouter{
		fast: &BackendBalancer{
			backends:     []Backend{{Name: "fast"}},
			balancerType: BalancerTypeRoundRobin,
		},
		archiveThreshold: threshold,
	}
	if withArchive {
		router.archive = &BackendBalancer{
			backends:     []Backend{{Name: "archive"}},
			balancerType: BalancerTypeRoundRobin,
		}
	}
	router.SetSeqnoSource(staticSeqnoSource{
		seqno: current,
		ok:    true,
	})
	return router
}

func selectedBackendName(t *testing.T, client ton.LiteClient) string {
	t.Helper()

	backend, ok := client.(*Backend)
	if !ok {
		t.Fatalf("unexpected client type %T", client)
	}
	return backend.Name
}

func TestBackendBalancerFailoverReturnsSliceBackend(t *testing.T) {
	balancer := &BackendBalancer{
		backends: []Backend{
			{Name: "fast-a"},
			{Name: "fast-b"},
		},
		balancerType: BalancerTypeFailOver,
	}

	atomic.StoreUint64(&balancer.backends[0].failsStreak, 11)
	atomic.StoreInt64(&balancer.backends[0].lastRequest, 100)
	atomic.StoreInt64(&balancer.backends[0].lastSuccess, 0)

	client := balancer.GetClient()
	backend, ok := client.(*Backend)
	if !ok {
		t.Fatalf("unexpected client type %T", client)
	}
	if backend != &balancer.backends[1] {
		t.Fatalf("expected backend pointer to point into slice")
	}
}

func TestBackendRouterSelectsArchiveByBlockSeqno(t *testing.T) {
	router := testRouter(100, 250, true)

	client, useArchive := router.SelectClientForPayload(ton.GetBlockData{
		ID: &ton.BlockIDExt{SeqNo: 149},
	})
	if !useArchive {
		t.Fatalf("expected archive route")
	}
	if name := selectedBackendName(t, client); name != "archive" {
		t.Fatalf("expected archive backend, got %s", name)
	}
}

func TestBackendRouterKeepsFastWithoutSeqno(t *testing.T) {
	router := testRouter(100, 250, true)

	client, useArchive := router.SelectClientForPayload(ton.GetTransactions{})
	if useArchive {
		t.Fatalf("did not expect archive route")
	}
	if name := selectedBackendName(t, client); name != "fast" {
		t.Fatalf("expected fast backend, got %s", name)
	}
}

func TestBackendRouterUsesOldestProofSeqno(t *testing.T) {
	router := testRouter(100, 250, true)

	client, useArchive := router.SelectClientForPayload(ton.GetBlockProof{
		KnownBlock:  &ton.BlockIDExt{SeqNo: 220},
		TargetBlock: &ton.BlockIDExt{SeqNo: 120},
	})
	if !useArchive {
		t.Fatalf("expected archive route for old proof target")
	}
	if name := selectedBackendName(t, client); name != "archive" {
		t.Fatalf("expected archive backend, got %s", name)
	}
}

func TestBackendRouterFallsBackToFastWithoutSeqnoSource(t *testing.T) {
	router := &BackendRouter{
		fast: &BackendBalancer{
			backends:     []Backend{{Name: "fast"}},
			balancerType: BalancerTypeRoundRobin,
		},
		archive: &BackendBalancer{
			backends:     []Backend{{Name: "archive"}},
			balancerType: BalancerTypeRoundRobin,
		},
		archiveThreshold: 100,
	}

	client, useArchive := router.SelectClientForPayload(ton.GetBlockData{
		ID: &ton.BlockIDExt{SeqNo: 10},
	})
	if useArchive {
		t.Fatalf("did not expect archive route without current seqno")
	}
	if name := selectedBackendName(t, client); name != "fast" {
		t.Fatalf("expected fast backend, got %s", name)
	}
}

func TestFindStorageBoundary(t *testing.T) {
	oldestAvailable, firstUnavailable, err := findStorageBoundary(1000, func(seqno uint32) (bool, error) {
		return seqno >= 321, nil
	})
	if err != nil {
		t.Fatalf("findStorageBoundary returned error: %v", err)
	}
	if oldestAvailable != 321 {
		t.Fatalf("expected oldest available 321, got %d", oldestAvailable)
	}
	if firstUnavailable != 320 {
		t.Fatalf("expected first unavailable 320, got %d", firstUnavailable)
	}
}

func TestFindStorageBoundaryWithFullHistory(t *testing.T) {
	oldestAvailable, firstUnavailable, err := findStorageBoundary(1000, func(seqno uint32) (bool, error) {
		return true, nil
	})
	if err != nil {
		t.Fatalf("findStorageBoundary returned error: %v", err)
	}
	if oldestAvailable != 1 {
		t.Fatalf("expected oldest available 1, got %d", oldestAvailable)
	}
	if firstUnavailable != 0 {
		t.Fatalf("expected no unavailable seqno, got %d", firstUnavailable)
	}
}

func TestEffectiveArchiveThreshold(t *testing.T) {
	threshold := effectiveArchiveThreshold([]BackendStorageDepth{
		{Name: "fast-a", RouteThreshold: 1500},
		{Name: "fast-b", RouteThreshold: 900},
		{Name: "fast-c", RouteThreshold: 0},
	})
	if threshold != 900 {
		t.Fatalf("expected threshold 900, got %d", threshold)
	}
}

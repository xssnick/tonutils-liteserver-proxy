package server

import (
	"context"
	"testing"

	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/ton"
)

type fakeLiteClient struct {
	id string
}

func (f *fakeLiteClient) QueryLiteserver(context.Context, tl.Serializable, tl.Serializable) error {
	return nil
}

func (f *fakeLiteClient) StickyContext(ctx context.Context) context.Context {
	return ctx
}

func (f *fakeLiteClient) StickyContextNextNode(ctx context.Context) (context.Context, error) {
	return ctx, nil
}

func (f *fakeLiteClient) StickyContextNextNodeBalanced(ctx context.Context) (context.Context, error) {
	return ctx, nil
}

func (f *fakeLiteClient) StickyNodeID(context.Context) uint32 {
	return 0
}

type rotatingLiteClientSource struct {
	clients []ton.LiteClient
	next    int
}

func (r *rotatingLiteClientSource) GetClient() ton.LiteClient {
	client := r.clients[r.next]
	if r.next < len(r.clients)-1 {
		r.next++
	}
	return client
}

func TestMasterSeqnoTrackerPollUsesFreshClient(t *testing.T) {
	first := &fakeLiteClient{id: "first"}
	second := &fakeLiteClient{id: "second"}
	source := &rotatingLiteClientSource{
		clients: []ton.LiteClient{first, second},
	}

	var seen []string
	tracker := &MasterSeqnoTracker{
		clientSource: source,
		fetchMasterchainInfo: func(ctx context.Context, client ton.LiteClient, seqno uint32) (*ton.MasterchainInfo, error) {
			seen = append(seen, client.(*fakeLiteClient).id)
			return &ton.MasterchainInfo{
				Last: &ton.BlockIDExt{SeqNo: seqno + 1},
			}, nil
		},
	}

	if _, err := tracker.poll(context.Background(), 10); err != nil {
		t.Fatalf("first poll failed: %v", err)
	}
	if _, err := tracker.poll(context.Background(), 11); err != nil {
		t.Fatalf("second poll failed: %v", err)
	}

	if len(seen) != 2 {
		t.Fatalf("expected 2 polls, got %d", len(seen))
	}
	if seen[0] != "first" || seen[1] != "second" {
		t.Fatalf("expected fresh clients, got %v", seen)
	}
}

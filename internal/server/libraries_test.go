package server

import (
	"bytes"
	"context"
	"reflect"
	"testing"
	"time"

	lru "github.com/hashicorp/golang-lru"
	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

type validationLiteClient struct {
	checkQuery func(payload tl.Serializable) error
	response   tl.Serializable
}

func (m *validationLiteClient) QueryLiteserver(ctx context.Context, payload tl.Serializable, result tl.Serializable) error {
	if m.checkQuery != nil {
		if err := m.checkQuery(payload); err != nil {
			return err
		}
	}
	reflect.ValueOf(result).Elem().Set(reflect.ValueOf(m.response))
	return nil
}

func (m *validationLiteClient) StickyContext(ctx context.Context) context.Context {
	return ctx
}

func (m *validationLiteClient) StickyContextNextNode(ctx context.Context) (context.Context, error) {
	return ctx, nil
}

func (m *validationLiteClient) StickyContextNextNodeBalanced(ctx context.Context) (context.Context, error) {
	return ctx, nil
}

func (m *validationLiteClient) StickyNodeID(ctx context.Context) uint32 {
	return 0
}

type stubLibraryCache struct {
	libs   *cell.Dictionary
	cached bool
	err    error
}

func (s *stubLibraryCache) MethodEmulationEnabled() bool {
	return false
}

func (s *stubLibraryCache) LookupBlockInCache(id *ton.BlockInfoShort) (*ton.BlockHeader, error) {
	return nil, nil
}

func (s *stubLibraryCache) GetTransaction(ctx context.Context, block *Block, account *ton.AccountID, lt int64) (*ton.TransactionInfo, error) {
	return nil, nil
}

func (s *stubLibraryCache) GetLibraries(ctx context.Context, hashes [][]byte) (*cell.Dictionary, bool, error) {
	return s.libs, s.cached, s.err
}

func (s *stubLibraryCache) WaitMasterBlock(ctx context.Context, seqno uint32, timeout time.Duration) error {
	return nil
}

func (s *stubLibraryCache) GetZeroState() (*ton.ZeroStateIDExt, error) {
	return nil, nil
}

func (s *stubLibraryCache) GetMasterBlock(ctx context.Context, id *ton.BlockIDExt, skipChecks bool) (*MasterBlock, bool, error) {
	return nil, false, nil
}

func (s *stubLibraryCache) GetLastMasterBlock(ctx context.Context) (*MasterBlock, bool, error) {
	return nil, false, nil
}

func (s *stubLibraryCache) GetAccountStateInBlock(ctx context.Context, block *Block, addr *address.Address) (*ton.AccountState, bool, error) {
	return nil, false, nil
}

func (s *stubLibraryCache) CacheBlockIfNeeded(ctx context.Context, id *ton.BlockIDExt) (*Block, bool, error) {
	return nil, false, nil
}

func libraryTestHash(v byte) []byte {
	return bytes.Repeat([]byte{v}, 32)
}

func TestNormalizeLibraryListForLookupMatchesLiteServerSemantics(t *testing.T) {
	var hashes [][]byte
	for i := byte(0); i < 16; i++ {
		hashes = append(hashes, libraryTestHash(16-i))
	}
	hashes = append(hashes, libraryTestHash(10))
	hashes = append(hashes, libraryTestHash(42))

	got := normalizeLibraryListForLookup(hashes)

	if len(got) != 16 {
		t.Fatalf("unexpected normalized len: got %d want 16", len(got))
	}
	if bytes.Equal(got[len(got)-1], libraryTestHash(42)) {
		t.Fatalf("expected hashes after first 16 items to be ignored")
	}
	for i := 1; i < len(got); i++ {
		if bytes.Compare(got[i-1], got[i]) >= 0 {
			t.Fatalf("expected normalized hashes to be strictly sorted and deduplicated")
		}
	}
}

func TestUnwrapLibraryResultCell(t *testing.T) {
	library := cell.BeginCell().MustStoreUInt(0xFF00F4A4, 32).EndCell()
	wrapped := cell.BeginCell().MustStoreRef(library).EndCell()

	if got := unwrapLibraryResultCell(library, library.Hash()); got != library {
		t.Fatalf("expected direct library cell to be returned")
	}

	if got := unwrapLibraryResultCell(wrapped, library.Hash()); got != library {
		t.Fatalf("expected wrapped library cell to be unwrapped")
	}

	if got := unwrapLibraryResultCell(wrapped, wrapped.Hash()); got != wrapped {
		t.Fatalf("expected wrapper hash to still resolve to wrapper")
	}

	nonCanonical := cell.BeginCell().MustStoreUInt(1, 1).MustStoreRef(library).EndCell()
	if got := unwrapLibraryResultCell(nonCanonical, library.Hash()); got != nil {
		t.Fatalf("expected non-canonical wrapper not to be unwrapped")
	}
}

func TestGetLibrariesUnwrapsEmptyRootWrapper(t *testing.T) {
	library := cell.BeginCell().MustStoreUInt(0xFF00F4A413F4BCF2, 64).EndCell()
	wrapped := cell.BeginCell().MustStoreRef(library).EndCell()
	missing := libraryTestHash(0xAB)

	mock := &validationLiteClient{
		response: ton.LibraryResult{
			Result: []*ton.LibraryEntry{
				{
					Hash: library.Hash(),
					Data: wrapped,
				},
			},
		},
		checkQuery: func(payload tl.Serializable) error {
			req, ok := payload.(ton.GetLibraries)
			if !ok {
				t.Fatalf("unexpected request type: %T", payload)
			}
			if len(req.LibraryList) != 2 {
				t.Fatalf("unexpected request len: %d", len(req.LibraryList))
			}
			if !bytes.Equal(req.LibraryList[0], library.Hash()) {
				t.Fatalf("unexpected first hash")
			}
			if !bytes.Equal(req.LibraryList[1], missing) {
				t.Fatalf("unexpected second hash")
			}
			return nil
		},
	}

	got, err := getLibraries(context.Background(), mock, library.Hash(), missing)
	if err != nil {
		t.Fatal(err)
	}

	if len(got) != 2 {
		t.Fatalf("unexpected response len: %d", len(got))
	}
	if got[0] == nil {
		t.Fatalf("expected wrapped library to be found")
	}
	if !bytes.Equal(got[0].Hash(), library.Hash()) {
		t.Fatalf("unexpected returned hash: %x", got[0].Hash())
	}
	if got[1] != nil {
		t.Fatalf("expected missing library to stay nil")
	}
}

func TestBlockCacheGetLibrariesAppliesLiteServerNormalizationBeforeCache(t *testing.T) {
	libsCache, err := lru.NewARC(32)
	if err != nil {
		t.Fatalf("failed to create cache: %v", err)
	}

	cache := &BlockCache{libsCache: libsCache}
	var hashes [][]byte
	for i := byte(1); i <= 17; i++ {
		hash := libraryTestHash(i)
		hashes = append(hashes, hash)
		cache.storeLibrary(hash, cell.BeginCell().MustStoreUInt(uint64(i), 8).EndCell())
	}

	libs, cached, err := cache.GetLibraries(context.Background(), hashes)
	if err != nil {
		t.Fatal(err)
	}
	if !cached {
		t.Fatalf("expected fully cached result")
	}

	all, err := libs.LoadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 16 {
		t.Fatalf("unexpected library count: got %d want 16", len(all))
	}
	for _, kv := range all {
		hash := kv.Key.MustLoadSlice(256)
		if bytes.Equal(hash, libraryTestHash(17)) {
			t.Fatalf("expected hash after first 16 request items to be ignored")
		}
	}
}

func TestHandleGetLibrariesReturnsRootCellInsteadOfWrapper(t *testing.T) {
	library := cell.BeginCell().MustStoreUInt(0x12345678, 32).EndCell()
	libs := cell.NewDict(256)
	if err := libs.Set(
		cell.BeginCell().MustStoreSlice(library.Hash(), 256).EndCell(),
		cell.BeginCell().MustStoreRef(library).EndCell(),
	); err != nil {
		t.Fatalf("failed to build libraries dict: %v", err)
	}

	balancer := &ProxyBalancer{
		cache: &stubLibraryCache{
			libs:   libs,
			cached: true,
		},
	}

	resp, hit := balancer.handleGetLibraries(context.Background(), &ton.GetLibraries{
		LibraryList: [][]byte{library.Hash()},
	})
	if hit != HitTypeCache {
		t.Fatalf("unexpected hit type: %s", hit)
	}

	result, ok := resp.(ton.LibraryResult)
	if !ok {
		t.Fatalf("unexpected response type: %T", resp)
	}
	if len(result.Result) != 1 {
		t.Fatalf("unexpected result len: %d", len(result.Result))
	}
	if !bytes.Equal(result.Result[0].Hash, library.Hash()) {
		t.Fatalf("unexpected library hash in response")
	}
	if !bytes.Equal(result.Result[0].Data.Hash(), library.Hash()) {
		t.Fatalf("expected response library root hash to match original library")
	}
	if result.Result[0].Data.BitsSize() == 0 && result.Result[0].Data.RefsNum() == 1 {
		t.Fatalf("expected library root, got wrapper cell")
	}
}

func TestLibrariesWithProofTLCompatibility(t *testing.T) {
	req := GetLibrariesWithProof{
		ID: &ton.BlockIDExt{
			Workchain: -1,
			Shard:     -9223372036854775808,
			SeqNo:     1,
			RootHash:  libraryTestHash(1),
			FileHash:  libraryTestHash(2),
		},
		Mode:        3,
		LibraryList: [][]byte{libraryTestHash(3)},
	}

	reqData, err := tl.Serialize(req, true)
	if err != nil {
		t.Fatalf("failed to serialize request: %v", err)
	}

	var parsedReq tl.Serializable
	if _, err = tl.Parse(&parsedReq, reqData, true); err != nil {
		t.Fatalf("failed to parse request: %v", err)
	}
	gotReq, ok := parsedReq.(GetLibrariesWithProof)
	if !ok {
		t.Fatalf("unexpected parsed request type: %T", parsedReq)
	}
	if !gotReq.ID.Equals(req.ID) || gotReq.Mode != req.Mode || len(gotReq.LibraryList) != 1 ||
		!bytes.Equal(gotReq.LibraryList[0], req.LibraryList[0]) {
		t.Fatalf("request roundtrip mismatch")
	}

	resp := LibraryResultWithProof{
		ID:   req.ID.Copy(),
		Mode: req.Mode,
		Result: []*ton.LibraryEntry{{
			Hash: req.LibraryList[0],
			Data: cell.BeginCell().MustStoreUInt(7, 3).EndCell(),
		}},
		StateProof: cell.BeginCell().MustStoreUInt(1, 1).EndCell(),
		DataProof:  cell.BeginCell().MustStoreUInt(2, 2).EndCell(),
	}

	respData, err := tl.Serialize(resp, true)
	if err != nil {
		t.Fatalf("failed to serialize response: %v", err)
	}

	var parsedResp tl.Serializable
	if _, err = tl.Parse(&parsedResp, respData, true); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}
	gotResp, ok := parsedResp.(LibraryResultWithProof)
	if !ok {
		t.Fatalf("unexpected parsed response type: %T", parsedResp)
	}
	if !gotResp.ID.Equals(resp.ID) || gotResp.Mode != resp.Mode || len(gotResp.Result) != 1 {
		t.Fatalf("response roundtrip mismatch")
	}
	if !bytes.Equal(gotResp.Result[0].Hash, resp.Result[0].Hash) {
		t.Fatalf("response hash mismatch")
	}
}

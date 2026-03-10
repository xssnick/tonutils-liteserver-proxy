package server

import (
	"testing"

	lru "github.com/hashicorp/golang-lru"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestBlockCacheStoreLibraryHandlesNilCache(t *testing.T) {
	cache := &BlockCache{}
	lib := cell.BeginCell().MustStoreUInt(1, 1).EndCell()

	cache.storeLibrary([]byte("hash"), lib)
}

func TestBlockCacheStoreLibraryCachesCell(t *testing.T) {
	libsCache, err := lru.NewARC(4)
	if err != nil {
		t.Fatalf("failed to create cache: %v", err)
	}

	cache := &BlockCache{libsCache: libsCache}
	lib := cell.BeginCell().MustStoreUInt(1, 1).EndCell()

	cache.storeLibrary([]byte("hash"), lib)

	got, ok := cache.libsCache.Get("hash")
	if !ok {
		t.Fatal("expected library to be cached")
	}
	if got != lib {
		t.Fatal("cached library pointer mismatch")
	}
}

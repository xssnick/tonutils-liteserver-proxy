package server

import (
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func init() {
	tl.Register(GetLibrariesWithProof{}, "liteServer.getLibrariesWithProof id:tonNode.blockIdExt mode:# library_list:(vector int256) = liteServer.LibraryResultWithProof")
	tl.Register(LibraryResultWithProof{}, "liteServer.libraryResultWithProof id:tonNode.blockIdExt mode:# result:(vector liteServer.libraryEntry) state_proof:bytes data_proof:bytes = liteServer.LibraryResultWithProof")
}

type GetLibrariesWithProof struct {
	ID          *ton.BlockIDExt `tl:"struct"`
	Mode        int32           `tl:"int"`
	LibraryList [][]byte        `tl:"vector int256"`
}

type LibraryResultWithProof struct {
	ID         *ton.BlockIDExt     `tl:"struct"`
	Mode       int32               `tl:"int"`
	Result     []*ton.LibraryEntry `tl:"vector struct"`
	StateProof *cell.Cell          `tl:"cell"`
	DataProof  *cell.Cell          `tl:"cell"`
}

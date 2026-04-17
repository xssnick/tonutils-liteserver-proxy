package server

import (
	"bytes"
	"fmt"
	"sort"

	"github.com/xssnick/tonutils-go/tvm/cell"
)

func normalizeLibraryListForLookup(hashes [][]byte) [][]byte {
	if len(hashes) > 16 {
		hashes = hashes[:16]
	}
	if len(hashes) == 0 {
		return hashes
	}

	normalized := append([][]byte(nil), hashes...)
	sort.Slice(normalized, func(i, j int) bool {
		return bytes.Compare(normalized[i], normalized[j]) < 0
	})

	out := normalized[:0]
	for _, hash := range normalized {
		if len(out) == 0 || !bytes.Equal(out[len(out)-1], hash) {
			out = append(out, hash)
		}
	}
	return out
}

func unwrapLibraryResultCell(root *cell.Cell, hash []byte) *cell.Cell {
	for root != nil {
		if bytes.Equal(hash, root.Hash()) {
			return root
		}

		// Some liteservers wrap the actual library root into an empty root cell.
		// Only unwrap that canonical 0-bit/1-ref form.
		if root.BitsSize() != 0 || root.RefsNum() != 1 {
			return nil
		}

		root = root.MustPeekRef(0)
	}

	return nil
}

func loadLibraryDictValueCell(value *cell.Slice) (*cell.Cell, error) {
	if value == nil {
		return nil, fmt.Errorf("library value is nil")
	}
	if value.BitsLeft() != 0 || value.RefsNum() != 1 {
		return nil, fmt.Errorf("library value is not a single ref")
	}
	return value.LoadRefCell()
}

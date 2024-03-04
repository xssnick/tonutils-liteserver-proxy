//go:build (darwin && cgo) || linux

package emulate

// #cgo LDFLAGS: -L ./lib -Wl,-rpath,./lib -lemulator
// #include <stdlib.h>
// #include <stdbool.h>
// #include "./lib/emulator-extern.h"
import "C"

import (
	"fmt"
	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"
	"math/big"
	"time"
	"unsafe"
)

type MethodConfig struct {
	C7   *cell.Cell `tlb:"^"`
	Libs *cell.Cell `tlb:"^"`
}

type RunMethodParams struct {
	Code     *cell.Cell   `tlb:"^"`
	Data     *cell.Cell   `tlb:"^"`
	Stack    *cell.Cell   `tlb:"^"`
	Params   MethodConfig `tlb:"^"`
	MethodID int32        `tlb:"## 32"`
}

type RunResult struct {
	ExitCode int32      `tlb:"## 32"`
	GasUsed  int64      `tlb:"## 64"`
	Stack    *cell.Cell `tlb:"^"`
}

func init() {
	C.emulator_set_verbosity_level(0)
}

func RunGetMethod(params RunMethodParams, maxGas int64) (*RunResult, error) {
	req, err := tlb.ToCell(params)
	if err != nil {
		return nil, err
	}

	boc := req.ToBOCWithFlags(false)

	cReq := C.CBytes(boc)
	defer C.free(unsafe.Pointer(cReq))

	res := unsafe.Pointer(C.tvm_emulator_emulate(C.uint32_t(len(boc)), (*C.char)(cReq), C.int64_t(maxGas)))
	if res == nil {
		return nil, fmt.Errorf("failed to execute tvm")
	}
	defer C.free(res)

	sz := *(*C.uint32_t)(res)
	data := C.GoBytes(unsafe.Pointer(uintptr(res)+4), C.int(sz))
	c, err := cell.FromBOC(data)
	if err != nil {
		return nil, err
	}

	var result RunResult
	if err := tlb.LoadFromCell(&result, c.BeginParse()); err != nil {
		return nil, err
	}
	return &result, nil
}

func PrepareC7(addr *address.Address, tm time.Time, seed []byte, balance *big.Int, cfg *cell.Dictionary, code *cell.Cell) ([]any, error) {
	if len(seed) != 32 {
		return nil, fmt.Errorf("seed len is not 32")
	}

	var tuple = make([]any, 0, 14)
	tuple = append(tuple, uint32(0x076ef1ea))
	tuple = append(tuple, uint8(0))
	tuple = append(tuple, uint8(0))
	tuple = append(tuple, uint32(tm.Unix()))
	tuple = append(tuple, uint8(0))
	tuple = append(tuple, uint8(0))
	tuple = append(tuple, new(big.Int).SetBytes(seed))
	tuple = append(tuple, []any{balance, nil})
	tuple = append(tuple, cell.BeginCell().MustStoreAddr(addr).ToSlice())
	if cfg != nil {
		tuple = append(tuple, cfg.AsCell())
		tuple = append(tuple, code)
		tuple = append(tuple, []any{0, nil}) // storage fees
		tuple = append(tuple, uint8(0))
		tuple = append(tuple, nil) // prev blocks
	} else {
		tuple = append(tuple, nil)
	}

	return []any{tuple}, nil
}

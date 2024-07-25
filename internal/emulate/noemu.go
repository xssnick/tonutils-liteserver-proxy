//go:build noemu

package emulate

import (
	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tvm/cell"
	"math/big"
	"time"
)

func RunGetMethod(params RunMethodParams, maxGas int64) (*RunResult, error) {
	panic("emulator not compiled")
}

func PrepareC7(addr *address.Address, tm time.Time, seed []byte, balance *big.Int, cfg *cell.Dictionary, code *cell.Cell) ([]any, error) {
	panic("emulator not compiled")
}

package emulate

import "github.com/xssnick/tonutils-go/tvm/cell"

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

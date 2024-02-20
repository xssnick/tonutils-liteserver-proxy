//go:build (darwin && cgo) || linux

package emulate

// #cgo LDFLAGS: -L ./lib -Wl,-rpath,./lib -lemulator
// #include <stdlib.h>
// #include <stdbool.h>
// #include "./lib/emulator-extern.h"
import "C"

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"
	"math/big"
	"time"
	"unsafe"
)

type RunMethodParams struct {
	Code    *cell.Cell
	Data    *cell.Cell
	Address *address.Address
	Stack   *cell.Cell
	Balance *big.Int
	Libs    *cell.Dictionary
	Config  *cell.Dictionary
	Time    time.Time
}

type RunResult struct {
	ExitCode int32      `tlb:"## 32"`
	GasUsed  int64      `tlb:"## 64"`
	Stack    *cell.Cell `tlb:"^"`

	C7 *cell.Cell `tlb:"-"`
}

func init() {
	C.emulator_set_verbosity_level(0)
}

func RunGetMethod(id int32, params RunMethodParams, withC7 bool, maxGas int64) (*RunResult, error) {
	cCfg := C.CString(b64(params.Config.AsCell()))
	defer C.free(unsafe.Pointer(cCfg))

	cStack := C.CString(b64(params.Stack))
	defer C.free(unsafe.Pointer(cStack))

	cCode := C.CString(b64(params.Code))
	defer C.free(unsafe.Pointer(cCode))

	cData := C.CString(b64(params.Data))
	defer C.free(unsafe.Pointer(cData))

	emu := C.tvm_emulator_create(cCode, cData, 0)

	C.tvm_emulator_set_gas_limit(emu, C.longlong(maxGas))
	if !params.Libs.IsEmpty() {
		cLibs := C.CString(b64(params.Libs.AsCell()))
		defer C.free(unsafe.Pointer(cLibs))
		C.tvm_emulator_set_libraries(emu, cLibs)
	}

	cAddr := C.CString(params.Address.String())
	defer C.free(unsafe.Pointer(cAddr))

	rnd := make([]byte, 32)
	_, _ = rand.Read(rnd)

	cRnd := C.CString(hex.EncodeToString(rnd))
	defer C.free(unsafe.Pointer(cRnd))
	C.tvm_emulator_set_c7(emu, cAddr, C.uint(uint32(params.Time.Unix())), C.ulonglong(params.Balance.Uint64()), cRnd, cCfg)

	// TODO: set prev blocks?
	if withC7 {
		// TODO: build c7 cell
	}

	res := C.tvm_emulator_run_get_method_optimized(emu, C.int(id), cStack)
	C.tvm_emulator_destroy(emu)

	boc, err := base64.StdEncoding.DecodeString(C.GoString(res))
	if err != nil {
		return nil, err
	}

	c, err := cell.FromBOC(boc)
	if err != nil {
		return nil, err
	}

	var result RunResult
	if err := tlb.LoadFromCell(&result, c.BeginParse()); err != nil {
		return nil, err
	}
	return &result, nil
}

func b64(c *cell.Cell) string {
	return base64.StdEncoding.EncodeToString(c.ToBOCWithFlags(false))
}

func prepareC7() {
	/*

		td::Ref<vm::Tuple> prepare_vm_c7(SmartContract::Args args, td::Ref<vm::Cell> code) {
		  td::BitArray<256> rand_seed;
		  if (args.rand_seed) {
		    rand_seed = args.rand_seed.unwrap();
		  } else {
		    rand_seed.as_slice().fill(0);
		  }
		  td::RefInt256 rand_seed_int{true};
		  rand_seed_int.unique_write().import_bits(rand_seed.cbits(), 256, false);

		  td::uint32 now = 0;
		  if (args.now) {
		    now = args.now.unwrap();
		  }

		  vm::CellBuilder cb;
		  if (args.address) {
		    td::BigInt256 dest_addr;
		    dest_addr.import_bits((*args.address).addr.as_bitslice());
		    cb.store_ones(1).store_zeroes(2).store_long((*args.address).workchain, 8).store_int256(dest_addr, 256);
		  }
		  auto address = cb.finalize();
		  auto config = td::Ref<vm::Cell>();

		  if (args.config) {
		    config = (*args.config)->get_root_cell();
		  }

		  std::vector<vm::StackEntry> tuple = {
		      td::make_refint(0x076ef1ea),                            // [ magic:0x076ef1ea
		      td::make_refint(0),                                     //   actions:Integer
		      td::make_refint(0),                                     //   msgs_sent:Integer
		      td::make_refint(now),                                   //   unixtime:Integer
		      td::make_refint(0),              //TODO:                //   block_lt:Integer
		      td::make_refint(0),              //TODO:                //   trans_lt:Integer
		      std::move(rand_seed_int),                               //   rand_seed:Integer
		      block::CurrencyCollection(args.balance).as_vm_tuple(),  //   balance_remaining:[Integer (Maybe Cell)]
		      vm::load_cell_slice_ref(address),                       //  myself:MsgAddressInt
		      vm::StackEntry::maybe(config)                           //vm::StackEntry::maybe(td::Ref<vm::Cell>())
		  };
		  if (args.config && args.config.value()->get_global_version() >= 4) {
		    tuple.push_back(code.not_null() ? code : vm::StackEntry{});        // code:Cell
		    tuple.push_back(block::CurrencyCollection::zero().as_vm_tuple());  // in_msg_value:[Integer (Maybe Cell)]
		    tuple.push_back(td::zero_refint());                                // storage_fees:Integer

		    // See crypto/block/mc-config.cpp#2115 (get_prev_blocks_info)
		    // [ wc:Integer shard:Integer seqno:Integer root_hash:Integer file_hash:Integer] = BlockId;
		    // [ last_mc_blocks:[BlockId...]
		    //   prev_key_block:BlockId ] : PrevBlocksInfo
		    tuple.push_back(args.prev_blocks_info ? args.prev_blocks_info.value() : vm::StackEntry{});  // prev_block_info
		  }
		  auto tuple_ref = td::make_cnt_ref<std::vector<vm::StackEntry>>(std::move(tuple));
		  //LOG(DEBUG) << "SmartContractInfo initialized with " << vm::StackEntry(tuple).to_string();
		  return vm::make_tuple_ref(std::move(tuple_ref));
		}
	*/
}

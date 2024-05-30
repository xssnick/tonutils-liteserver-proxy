package server

import (
	"bytes"
	"context"
	"fmt"
	lru "github.com/hashicorp/golang-lru"
	"github.com/rs/zerolog/log"
	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
	"github.com/xssnick/tonutils-liteserver-proxy/config"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

var ErrTimeout = ton.LSError{
	Code: 652,
	Text: "timeout",
}

type KeyBlock struct {
	Seqno uint32
	Proof *cell.Cell
	Prev  *KeyBlock
}

type MasterBlock struct {
	Block
	StateHash []byte
	GenTime   uint32
	Config    *cell.Dictionary

	mx sync.RWMutex
}

type ShardBlock struct {
	Block
	mx sync.RWMutex
}

type Block struct {
	ID            *ton.BlockIDExt
	Data          *cell.Cell
	DataRaw       []byte // TODO: align boc serialization to be same with c++, because of file hash
	ShardAccounts *tlb.ShardAccountBlocks

	MasterID *ton.BlockIDExt

	accountsCache *lru.ARCCache
}

type ShardInfo struct {
	shardBlocks map[uint32]*ShardBlock
	lastBlock   *ton.BlockIDExt
	updatedAt   time.Time
}

type BlockCache struct {
	config config.CacheConfig

	balancer  *BackendBalancer
	libsCache *lru.ARCCache

	lastBlock *ton.BlockIDExt
	zeroState *ton.ZeroStateIDExt

	keyBlocksProofs map[uint32]*KeyBlock

	masterBlocks map[uint32]*MasterBlock
	shards       map[string]*ShardInfo

	mcWaiter unsafe.Pointer
	mx       sync.RWMutex
}

func NewBlockCache(config config.CacheConfig, balancer *BackendBalancer) *BlockCache {
	b := &BlockCache{
		config:       config,
		balancer:     balancer,
		masterBlocks: map[uint32]*MasterBlock{},
		shards:       map[string]*ShardInfo{},
	}

	if config.MaxCachedLibraries > 0 {
		libsCache, err := lru.NewARC(int(config.MaxCachedLibraries))
		if err != nil {
			panic("failed to init libs cache: " + err.Error())
		}
		b.libsCache = libsCache
	}

	ch := make(chan struct{})
	atomic.StorePointer(&b.mcWaiter, unsafe.Pointer(&ch))

	fetched := make(chan bool)
	// fetch fresh master blocks
	go func() {
		var waitSeqno, streak uint32
		for {
			ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
			inf, err := getMasterchainInfo(ctx, b.balancer.GetClient(), waitSeqno)
			cancel()
			if err != nil {
				if streak > 3 {
					log.Warn().Err(err).Msg("fetch new master info failed, we will retry in 1s")
				}
				streak++
				time.Sleep(1 * time.Second)
				continue
			}
			streak = 0

			b.mx.RLock()
			hasZeroState := b.zeroState != nil
			b.mx.RUnlock()

			if !hasZeroState {
				b.mx.Lock()
				b.zeroState = inf.Init
				b.mx.Unlock()
			}

			ctx, cancel = context.WithTimeout(context.Background(), 8*time.Second)
			block, _, err := b.GetMasterBlock(ctx, inf.Last, true)
			cancel()
			if err != nil {
				log.Warn().Err(err).Msg("fetch new master block failed, we will retry in 1s")
				time.Sleep(1 * time.Second)
				continue
			}

			if waitSeqno == 0 {
				close(fetched)
			}
			lag := time.Since(time.Unix(int64(block.GenTime), 0)).Round(time.Second)
			if lag > 60*time.Second {
				log.Warn().Uint32("seqno", block.Block.ID.SeqNo).Dur("lag", lag/1000).Msg("new master info fetched, lag looks high")
			} else {
				log.Debug().Uint32("seqno", block.Block.ID.SeqNo).Dur("lag", lag/1000).Msg("new master info fetched")
			}

			waitSeqno = block.Block.ID.SeqNo + 1
		}
	}()

	<-fetched

	return b
}

func (c *BlockCache) GetLibraries(ctx context.Context, hashes [][]byte) (*cell.Dictionary, bool, error) {
	libs := cell.NewDict(256)
	if len(hashes) == 0 {
		return libs, true, nil
	}

	var toFetch [][]byte
	for _, hash := range hashes {
		if c.libsCache != nil {
			lib, ok := c.libsCache.Get(string(hash))
			if ok {
				if err := libs.Set(cell.BeginCell().MustStoreSlice(hash, 256).EndCell(), lib.(*cell.Cell)); err != nil {
					return nil, false, err
				}
				continue
			}
		}
		toFetch = append(toFetch, hash)
	}

	if len(toFetch) == 0 {
		return libs, true, nil
	}

	fetchedLibs, err := getLibraries(ctx, c.balancer.GetClient(), toFetch...)
	if err != nil {
		return nil, false, err
	}

	for i, cl := range fetchedLibs {
		if cl != nil {
			c.libsCache.Add(string(toFetch[i]), cl)
			if err = libs.Set(cell.BeginCell().MustStoreSlice(cl.Hash(), 256).EndCell(), cl); err != nil {
				return nil, false, err
			}
		}
	}

	return libs, false, nil
}

func (c *BlockCache) GetMasterBlock(ctx context.Context, id *ton.BlockIDExt, skipChecks bool) (*MasterBlock, bool, error) {
	if id.Workchain != -1 {
		return nil, false, fmt.Errorf("not a master workchain: %d %d", id.Workchain, id.SeqNo)
	}

	var lastSeqno uint32
	c.mx.RLock()
	b := c.masterBlocks[id.SeqNo]
	if c.lastBlock != nil {
		lastSeqno = c.lastBlock.SeqNo
	}
	c.mx.RUnlock()

	if !skipChecks {
		if lastSeqno > 0 && id.SeqNo < lastSeqno-c.config.MaxMasterBlockSeqnoDiffToCache {
			return nil, false, ton.LSError{
				Code: 410,
				Text: "too old master info requested",
			}
		}

		if lastSeqno > 0 && id.SeqNo > lastSeqno+200 {
			return nil, false, ton.LSError{
				Code: 411,
				Text: "too future block",
			}
		}
	}

	if b == nil {
		// lock optimization
		c.mx.Lock()
		b = c.masterBlocks[id.SeqNo]
		if b == nil {
			b = &MasterBlock{}
			c.masterBlocks[id.SeqNo] = b
		}
		c.mx.Unlock()
	}

	b.mx.Lock()
	defer b.mx.Unlock()

	if b.Block.ID != nil {
		if !b.Block.ID.Equals(id) {
			return nil, false, ton.LSError{
				Code: 651,
				Text: "unknown block id",
			}
		}
		return b, true, nil
	}

	blockRaw, blockCell, err := getBlock(ctx, c.balancer.GetClient(), id)
	if err != nil {
		return nil, false, fmt.Errorf("failed to get block data: %w", err)
	}

	var block tlb.Block
	if err = tlb.LoadFromCell(&block, blockCell.BeginParse()); err != nil {
		return nil, false, fmt.Errorf("failed to parse block data: %w", err)
	}

	sh := block.StateUpdate.BeginParse()
	if _, err = sh.LoadSlice(8 + 256); err != nil {
		return nil, false, fmt.Errorf("corrpted state update first bits: %w", err)
	}

	stateHash, err := sh.LoadSlice(256)
	if err != nil {
		return nil, false, fmt.Errorf("corrpted state update: %w", err)
	}

	if block.Extra == nil || block.Extra.Custom == nil || block.BlockInfo.NotMaster {
		return nil, false, fmt.Errorf("not complete master block")
	}

	var cfg *cell.Dictionary
	if block.Extra.Custom.KeyBlock {
		// key block has config
		cfg = block.Extra.Custom.ConfigParams.Config.Params
	} else {
		c.mx.RLock()
		prev := c.masterBlocks[id.SeqNo-1]
		c.mx.RUnlock()

		if prev != nil {
			prev.mx.RLock()
			cfg = prev.Config
			prev.mx.RUnlock()
		}

		if cfg == nil {
			// fetch config directly, because we don't know current
			cfg, err = getBlockchainConfig(ctx, c.balancer.GetClient(), id)
			if err != nil {
				return nil, false, fmt.Errorf("failed to get config: %w", err)
			}
		}
	}

	var shardAccounts tlb.ShardAccountBlocks
	if err = tlb.LoadFromCellAsProof(&shardAccounts, block.Extra.ShardAccountBlocks.BeginParse()); err != nil {
		return nil, false, fmt.Errorf("failed to load shard accounts from block: %w", err)
	}

	shards, err := ton.LoadShardsFromHashes(block.Extra.Custom.ShardHashes, false)
	if err != nil {
		return nil, false, err
	}

	var cache *lru.ARCCache
	if c.config.MaxCachedAccountsPerBlock > 0 {
		// arc cache will still hold frequently used accounts even if there are many new account requests
		cache, err = lru.NewARC(int(c.config.MaxCachedAccountsPerBlock))
		if err != nil {
			return nil, false, err
		}
	}

	b.Block = Block{
		ID:            id,
		Data:          blockCell,
		DataRaw:       blockRaw,
		ShardAccounts: &shardAccounts,
		accountsCache: cache,
		MasterID:      id,
	}
	b.Config = cfg
	b.GenTime = block.BlockInfo.GenUtime
	b.StateHash = stateHash

	c.mx.RLock()
	lastUpdated := c.lastBlock == nil || b.Block.ID.SeqNo > c.lastBlock.SeqNo
	c.mx.RUnlock()

	if lastUpdated {
		c.mx.Lock()
		if c.lastBlock == nil || b.Block.ID.SeqNo > c.lastBlock.SeqNo {
			c.lastBlock = b.Block.ID

			for _, shard := range shards {
				shardKey := getShardKey(shard.Workchain, shard.Shard)
				si := c.shards[shardKey]
				if si == nil {
					si = &ShardInfo{
						shardBlocks: map[uint32]*ShardBlock{},
					}
					c.shards[shardKey] = si
					log.Debug().Str("key", shardKey).Int("shards", len(c.shards)).Msg("creating shard info")
				}
				si.lastBlock = shard
				si.updatedAt = time.Now()

				// clean old shard blocks
				for u, shardBlock := range si.shardBlocks {
					if si.lastBlock.SeqNo-shardBlock.ID.SeqNo > c.config.MaxShardBlockSeqnoDiffToCache {
						delete(si.shardBlocks, u)
					}
				}
			}

			// clean old blocks
			for k, lb := range c.masterBlocks {
				if lb.ID != nil && c.lastBlock.SeqNo-lb.Block.ID.SeqNo > c.config.MaxMasterBlockSeqnoDiffToCache {
					delete(c.masterBlocks, k)
				}
			}
			// remove old merged shards
			staleBefore := time.Now().Add(-30 * time.Minute)
			for k, sx := range c.shards {
				if sx.updatedAt.Before(staleBefore) {
					delete(c.shards, k)
				}
			}
		}
		c.mx.Unlock()

		// broadcast new master and init new waiter
		old := (*chan struct{})(atomic.LoadPointer(&c.mcWaiter))
		ch := make(chan struct{})
		atomic.StorePointer(&c.mcWaiter, unsafe.Pointer(&ch))
		close(*old)
	}

	return b, false, nil
}

func (c *BlockCache) GetZeroState() (*ton.ZeroStateIDExt, error) {
	if c.zeroState == nil {
		return nil, fmt.Errorf("zero state is not fetched yet")
	}
	return c.zeroState, nil
}

func (c *BlockCache) GetLastMasterBlock(ctx context.Context) (*MasterBlock, bool, error) {
	c.mx.RLock()
	lb := c.lastBlock
	c.mx.RUnlock()

	if lb == nil {
		return nil, false, fmt.Errorf("last master is not fetched yet")
	}

	return c.GetMasterBlock(ctx, lb, false)
}

func (c *BlockCache) WaitMasterBlock(ctx context.Context, seqno uint32, timeout time.Duration) error {
	c.mx.RLock()
	already := c.lastBlock != nil && seqno <= c.lastBlock.SeqNo
	c.mx.RUnlock()

	if already {
		return nil
	}

	wait := time.NewTimer(timeout)
	defer wait.Stop()

	for {
		waiter := *(*chan struct{})(atomic.LoadPointer(&c.mcWaiter))

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-wait.C:
			return ton.LSError{
				Code: 652,
				Text: "timeout",
			}
		case <-waiter:
			c.mx.RLock()
			already = c.lastBlock != nil && seqno <= c.lastBlock.SeqNo
			c.mx.RUnlock()

			if already {
				return nil
			}
		}
	}
}

func getBlockchainConfig(ctx context.Context, client ton.LiteClient, block *ton.BlockIDExt) (*cell.Dictionary, error) {
	var resp tl.Serializable
	var err error
	err = client.QueryLiteserver(ctx, ton.GetConfigAll{
		Mode:    0b1111111111,
		BlockID: block,
	}, &resp)
	if err != nil {
		return nil, err
	}

	switch t := resp.(type) {
	case ton.ConfigAll:
		stateExtra, err := ton.CheckShardMcStateExtraProof(block, []*cell.Cell{t.StateProof, t.ConfigProof})
		if err != nil {
			return nil, fmt.Errorf("incorrect proof: %w", err)
		}

		return stateExtra.ConfigParams.Config.Params, nil
	case ton.LSError:
		return nil, t
	}
	return nil, fmt.Errorf("unexpected response from node")
}

func (c *BlockCache) GetAccountState(ctx context.Context, id *ton.BlockIDExt, addr *address.Address) (*ton.AccountState, bool, error) {
	block, blockFromCache, err := c.CacheBlockIfNeeded(ctx, id)
	if err != nil {
		return nil, false, err
	}

	if block == nil {
		account, err := getAccount(ctx, c.balancer.GetClient(), id, addr)
		if err != nil {
			return nil, false, err
		}
		return account, false, nil
	}

	acc, cached, err := c.GetAccountStateInBlock(ctx, block, addr)
	if err != nil {
		return nil, false, err
	}

	if cached {
		cached = blockFromCache
	}

	return acc, cached, err
}

func (c *BlockCache) GetAccountStateInBlock(ctx context.Context, block *Block, addr *address.Address) (*ton.AccountState, bool, error) {
	addrStr := addr.String()

	if block.accountsCache != nil {
		acc, ok := block.accountsCache.Get(addrStr)
		if ok {
			return acc.(*ton.AccountState), true, nil
		}
	}

	account, err := getAccount(ctx, c.balancer.GetClient(), block.ID, addr)
	if err != nil {
		return nil, false, err
	}

	if block.accountsCache != nil {
		block.accountsCache.Add(addrStr, account)
	}

	return account, false, nil
}

func (c *BlockCache) LookupBlockInCache(id *ton.BlockInfoShort) (*ton.BlockHeader, error) {
	var blk *Block
	if id.Workchain == -1 {
		c.mx.RLock()
		b := c.masterBlocks[uint32(id.Seqno)]
		c.mx.RUnlock()

		if b != nil {
			b.mx.RLock()
			if b.Block.Data != nil {
				blk = &b.Block
			}
			b.mx.RUnlock()
		}
	} else {
		var b *ShardBlock
		shardKey := getShardKey(id.Workchain, id.Shard)
		c.mx.RLock()
		si := c.shards[shardKey]
		if si != nil {
			b = si.shardBlocks[uint32(id.Seqno)]
		}
		c.mx.RUnlock()

		if si == nil {
			log.Debug().Str("key", shardKey).Msg("no shard info in cache")
		}

		if b != nil {
			b.mx.RLock()
			if b.Block.Data != nil {
				blk = &b.Block
			}
			b.mx.RUnlock()
		}
	}

	if blk != nil {
		sk := cell.CreateProofSkeleton()
		sk.ProofRef(0).SetRecursive()

		hdrProof, err := blk.Data.CreateProof(sk)
		if err != nil {
			return nil, err
		}

		return &ton.BlockHeader{
			ID:          blk.ID,
			Mode:        0,
			HeaderProof: hdrProof.ToBOCWithFlags(false),
		}, nil
	}
	return nil, nil
}

func (c *BlockCache) MethodEmulationEnabled() bool {
	return !c.config.DisableGetMethodsEmulation
}

func (c *BlockCache) CacheBlockIfNeeded(ctx context.Context, id *ton.BlockIDExt) (*Block, bool, error) {
	var fromCache bool
	var data *Block
	if id.Workchain != -1 {
		shardKey := getShardKey(id.Workchain, id.Shard)
		var b *ShardBlock
		c.mx.RLock()
		si := c.shards[shardKey]
		if si != nil {
			b = si.shardBlocks[id.SeqNo]
		}
		needCache := si != nil && id.SeqNo >= si.lastBlock.SeqNo-c.config.MaxShardBlockSeqnoDiffToCache
		c.mx.RUnlock()

		if si == nil {
			log.Debug().Str("key", shardKey).Msg("no shard info in cache")
		} else if !needCache {
			log.Debug().Str("key", shardKey).Uint32("seqno", id.SeqNo).
				Uint32("last_seqno", si.lastBlock.SeqNo).
				Uint32("max_diff", c.config.MaxShardBlockSeqnoDiffToCache).
				Msg("block is not for caching")
		} else {
			log.Debug().Str("key", shardKey).Uint32("seqno", id.SeqNo).
				Uint32("last_seqno", si.lastBlock.SeqNo).
				Uint32("max_diff", c.config.MaxShardBlockSeqnoDiffToCache).
				Msg("block is cacheable")
		}

		if b != nil {
			b.mx.RLock()
			dataFetched := b.Data != nil
			b.mx.RUnlock()

			if dataFetched {
				if !b.ID.Equals(id) {
					return nil, false, ton.LSError{
						Code: 651,
						Text: "unknown block id",
					}
				}
				data = &b.Block
				fromCache = true
			}
		}

		if needCache && data == nil {
			c.mx.Lock()
			b = si.shardBlocks[id.SeqNo]
			if b == nil {
				b = &ShardBlock{
					Block: Block{
						ID: id.Copy(),
					},
				}
				si.shardBlocks[id.SeqNo] = b
			}
			c.mx.Unlock()

			b.mx.Lock()
			defer b.mx.Unlock()

			if b.Data == nil {
				blockRaw, blk, err := getBlock(ctx, c.balancer.GetClient(), id)
				if err != nil {
					return nil, false, err
				}

				var block tlb.Block
				if err = tlb.LoadFromCell(&block, blk.BeginParse()); err != nil {
					return nil, false, fmt.Errorf("failed to parse block data: %w", err)
				}

				var shardAccounts tlb.ShardAccountBlocks
				if err = tlb.LoadFromCellAsProof(&shardAccounts, block.Extra.ShardAccountBlocks.BeginParse()); err != nil {
					return nil, false, fmt.Errorf("failed to load shard accounts from block: %w", err)
				}

				if c.config.MaxCachedAccountsPerBlock > 0 {
					// arc cache will still hold frequently used accounts even if there are many new account requests
					cache, err := lru.NewARC(int(c.config.MaxCachedAccountsPerBlock))
					if err != nil {
						return nil, false, err
					}
					b.accountsCache = cache
				}

				b.MasterID = &ton.BlockIDExt{
					Workchain: -1,
					Shard:     -0x8000000000000000,
					SeqNo:     block.BlockInfo.MasterRef.SeqNo,
					RootHash:  block.BlockInfo.MasterRef.RootHash,
					FileHash:  block.BlockInfo.MasterRef.FileHash,
				}
				b.Data = blk
				b.DataRaw = blockRaw
				b.ShardAccounts = &shardAccounts
			} else {
				fromCache = true
			}
			data = &b.Block

			log.Debug().Uint32("seqno", id.SeqNo).Msg("shard block resolved")
		}
	} else {
		c.mx.RLock()
		b := c.masterBlocks[id.SeqNo]
		needCache := c.lastBlock != nil && id.SeqNo >= c.lastBlock.SeqNo-c.config.MaxMasterBlockSeqnoDiffToCache
		c.mx.RUnlock()

		if b != nil && b.Block.ID != nil {
			log.Debug().Uint32("seqno", id.SeqNo).
				Uint32("last_seqno", c.lastBlock.SeqNo).
				Uint32("max_diff", c.config.MaxMasterBlockSeqnoDiffToCache).
				Msg("master block in cache")
		} else if !needCache {
			log.Debug().Uint32("seqno", id.SeqNo).
				Uint32("last_seqno", c.lastBlock.SeqNo).
				Uint32("max_diff", c.config.MaxMasterBlockSeqnoDiffToCache).
				Msg("master block is not for caching")
		} else {
			log.Debug().Uint32("seqno", id.SeqNo).
				Uint32("last_seqno", c.lastBlock.SeqNo).
				Uint32("max_diff", c.config.MaxMasterBlockSeqnoDiffToCache).
				Msg("master block is cacheable")
		}

		if b != nil && b.Block.ID != nil {
			if !b.Block.ID.Equals(id) {
				return nil, false, ton.LSError{
					Code: 651,
					Text: "unknown block id",
				}
			}
			data = &b.Block
			fromCache = true
		} else if needCache {
			// fetch and cache master block
			ms, cached, err := c.GetMasterBlock(ctx, id, false)
			if err != nil {
				return nil, false, err
			}
			data = &ms.Block
			fromCache = cached
		}
	}

	return data, fromCache, nil
}

func (c *BlockCache) GetTransaction(ctx context.Context, block *Block, account *ton.AccountID, lt int64) (*ton.TransactionInfo, error) {
	sk := cell.CreateProofSkeleton()
	pathToDict := sk.ProofRef(3).ProofRef(2).ProofRef(0)

	accKey := cell.BeginCell().MustStoreSlice(account.ID, 256).EndCell()
	acc, accProofPath, err := block.ShardAccounts.Accounts.LoadValueWithProof(accKey, pathToDict)
	if err != nil {
		return &ton.TransactionInfo{
			ID:          block.ID,
			Proof:       nil,
			Transaction: nil,
		}, nil
	}

	if err = tlb.LoadFromCell(new(tlb.CurrencyCollection), acc); err != nil {
		log.Warn().Err(err).Int64("lt", lt).Msg("failed to load currency collection from shard account")
		return nil, ton.LSError{
			Code: 500,
			Text: "failed to load currency collection from shard account",
		}
	}

	var accBlock tlb.AccountBlock
	if err = tlb.LoadFromCell(&accBlock, acc); err != nil {
		log.Warn().Err(err).Int64("lt", lt).Msg("failed to load account block from shard account")
		return nil, ton.LSError{
			Code: 500,
			Text: "failed to load account block from shard account",
		}
	}

	key := cell.BeginCell().MustStoreInt(lt, 64).EndCell()
	accTx, _, err := accBlock.Transactions.LoadValueWithProof(key, accProofPath)
	if err != nil {
		return &ton.TransactionInfo{
			ID:          block.ID,
			Proof:       nil,
			Transaction: nil,
		}, nil
	}

	proof, err := block.Data.CreateProof(sk)
	if err != nil {
		log.Warn().Err(err).Int64("lt", lt).Msg("failed to create transaction proof")
		return nil, ton.LSError{
			Code: 500,
			Text: "failed to create proof",
		}
	}

	tx, err := accTx.LoadRefCell()
	if err != nil {
		log.Warn().Err(err).Int64("lt", lt).Msg("failed to load transaction ref")
		return nil, ton.LSError{
			Code: 500,
			Text: "failed to load transaction ref",
		}
	}

	return &ton.TransactionInfo{
		ID:          block.ID,
		Proof:       proof.ToBOC(),
		Transaction: tx.ToBOC(),
	}, nil
}

func getAccount(ctx context.Context, client ton.LiteClient, block *ton.BlockIDExt, addr *address.Address) (*ton.AccountState, error) {
	var resp tl.Serializable
	err := client.QueryLiteserver(ctx, ton.GetAccountState{
		ID: block,
		Account: ton.AccountID{
			Workchain: addr.Workchain(),
			ID:        addr.Data(),
		},
	}, &resp)
	if err != nil {
		return nil, err
	}

	switch t := resp.(type) {
	case ton.AccountState:
		if !t.ID.Equals(block) {
			return nil, fmt.Errorf("response with incorrect block")
		}
		return &t, nil
	case ton.LSError:
		return nil, t
	}
	return nil, fmt.Errorf("unexpected response")
}

func getMasterchainInfo(ctx context.Context, client ton.LiteClient, seqno uint32) (_ *ton.MasterchainInfo, err error) {
	var prefix []byte
	if seqno > 0 {
		prefix, err = tl.Serialize(ton.WaitMasterchainSeqno{
			Seqno:   int32(seqno),
			Timeout: int32(6500),
		}, true)
		if err != nil {
			return nil, err
		}
	}

	suffix, err := tl.Serialize(ton.GetMasterchainInf{}, true)
	if err != nil {
		return nil, err
	}

	var resp tl.Serializable
	err = client.QueryLiteserver(ctx, tl.Raw(append(prefix, suffix...)), &resp)
	if err != nil {
		return nil, err
	}

	switch t := resp.(type) {
	case ton.MasterchainInfo:
		return &t, nil
	case ton.LSError:
		return nil, t
	}
	return nil, fmt.Errorf("unexpected response")
}

func getShardKey(wc int32, shard int64) string {
	return fmt.Sprint(wc) + ":" + fmt.Sprint(shard)
}

func getBlock(ctx context.Context, client ton.LiteClient, block *ton.BlockIDExt) ([]byte, *cell.Cell, error) {
	var resp tl.Serializable
	err := client.QueryLiteserver(ctx, ton.GetBlockData{ID: block}, &resp)
	if err != nil {
		return nil, nil, err
	}

	switch t := resp.(type) {
	case ton.BlockData:
		pl, err := cell.FromBOC(t.Payload)
		if err != nil {
			return nil, nil, err
		}

		if !bytes.Equal(pl.Hash(), block.RootHash) {
			return nil, nil, fmt.Errorf("incorrect block")
		}
		return t.Payload, pl, nil
	case ton.LSError:
		return nil, nil, t
	}
	return nil, nil, fmt.Errorf("unexpected response")
}

func getLibraries(ctx context.Context, client ton.LiteClient, hashes ...[]byte) ([]*cell.Cell, error) {
	var (
		resp tl.Serializable
		err  error
	)

	if err = client.QueryLiteserver(ctx, ton.GetLibraries{LibraryList: hashes}, &resp); err != nil {
		return nil, err
	}

	switch t := resp.(type) {
	case ton.LibraryResult:
		libList := make([]*cell.Cell, len(hashes))

		for i := 0; i < len(hashes); i++ {
			for _, e := range t.Result {
				// we are calculating hash by ourselves
				// to make sure that LS is not cheating
				if bytes.Equal(hashes[i], e.Data.Hash()) {
					libList[i] = e.Data
				}
			}
		}

		return libList, nil
	case ton.LSError:
		return nil, t
	}
	return nil, fmt.Errorf("unexpected response")
}

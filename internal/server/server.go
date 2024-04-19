package server

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"fmt"
	lru "github.com/hashicorp/golang-lru"
	"github.com/kevinms/leakybucket-go"
	"github.com/rs/zerolog/log"
	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/adnl"
	"github.com/xssnick/tonutils-go/liteclient"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
	"github.com/xssnick/tonutils-liteserver-proxy/config"
	"github.com/xssnick/tonutils-liteserver-proxy/internal/emulate"
	"github.com/xssnick/tonutils-liteserver-proxy/metrics"
	"hash/crc64"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const HitTypeEmulated = "emulated"
const HitTypeBackend = "backend"
const HitTypeCache = "cache"
const HitTypeGPCache = "gp_cache"
const HitTypeInflightCache = "inflight_cache"
const HitTypeFailedValidate = "failed_validate"
const HitTypeFailedInternal = "failed_internal"

type Cache interface {
	MethodEmulationEnabled() bool
	LookupBlockInCache(id *ton.BlockInfoShort) (*ton.BlockHeader, error)
	GetTransaction(ctx context.Context, block *Block, account *ton.AccountID, lt int64) (*ton.TransactionInfo, error)
	GetLibraries(ctx context.Context, hashes [][]byte) (*cell.Dictionary, bool, error)
	WaitMasterBlock(ctx context.Context, seqno uint32, timeout time.Duration) error
	GetZeroState() (*ton.ZeroStateIDExt, error)
	GetMasterBlock(ctx context.Context, id *ton.BlockIDExt) (*MasterBlock, bool, error)
	GetLastMasterBlock(ctx context.Context) (*MasterBlock, bool, error)
	GetAccountStateInBlock(ctx context.Context, block *Block, addr *address.Address) (*ton.AccountState, bool, error)
	CacheBlockIfNeeded(ctx context.Context, id *ton.BlockIDExt) (*Block, bool, error)
}

type Client struct {
	processor chan *liteclient.LiteServerQuery
}

type ClientConnInfo struct {
	Client      *liteclient.ServerClient
	LastRequest int64
}

type ClientIPInfo struct {
	ActiveConnections map[uint16]*ClientConnInfo
}

type CacheWaiter struct {
	W      chan bool
	Result tl.Serializable
}

type ProxyBalancer struct {
	srv             *liteclient.Server
	backendBalancer *BackendBalancer

	ips map[string]*ClientIPInfo

	cache               Cache
	configs             map[string]*KeyConfig
	onlyProxy           bool
	maxConnectionsPerIP int
	maxKeepAlive        time.Duration

	gpCache *lru.ARCCache

	inflightCache map[uint64]*CacheWaiter
	iflCacheMx    sync.RWMutex

	mx sync.RWMutex
}

type KeyConfig struct {
	name          string
	limiterPerIP  *leakybucket.Collector
	limiterPerKey *leakybucket.LeakyBucket
}

func NewProxyBalancer(configs []config.ClientConfig, backendBalancer *BackendBalancer, cache Cache, onlyProxy bool, maxConnectionsPerIP int, maxKeepAlive time.Duration, gpCacheSize int) *ProxyBalancer {
	s := &ProxyBalancer{
		backendBalancer:     backendBalancer,
		configs:             map[string]*KeyConfig{},
		cache:               cache,
		onlyProxy:           onlyProxy,
		maxConnectionsPerIP: maxConnectionsPerIP,
		maxKeepAlive:        maxKeepAlive,
		ips:                 map[string]*ClientIPInfo{},
		inflightCache:       map[uint64]*CacheWaiter{},
	}

	if gpCacheSize > 0 {
		var err error
		s.gpCache, err = lru.NewARC(gpCacheSize)
		if err != nil {
			panic("failed to init general purpose cache: " + err.Error())
		}
	}

	var keys []ed25519.PrivateKey

	for _, cfg := range configs {
		key := ed25519.NewKeyFromSeed(cfg.PrivateKey)
		keys = append(keys, key)

		var keyCfg KeyConfig
		keyCfg.name = cfg.Name
		if cfg.CapacityPerKey > 0 {
			keyCfg.limiterPerKey = leakybucket.NewLeakyBucket(cfg.CoolingPerSec, cfg.CapacityPerKey)
		}
		if cfg.CapacityPerIP > 0 {
			keyCfg.limiterPerIP = leakybucket.NewCollector(cfg.CoolingPerSec, cfg.CapacityPerIP, true)
		}

		s.configs[string(key.Public().(ed25519.PublicKey))] = &keyCfg
	}
	s.srv = liteclient.NewServer(keys)

	s.srv.SetMessageHandler(s.handleRequest)
	s.srv.SetConnectionHook(func(client *liteclient.ServerClient) error {
		ip := client.IP()

		s.mx.Lock()
		defer s.mx.Unlock()

		info := s.ips[ip]
		if info == nil {
			info = &ClientIPInfo{
				ActiveConnections: map[uint16]*ClientConnInfo{},
			}
			s.ips[ip] = info
		}

		if s.maxConnectionsPerIP > 0 && len(info.ActiveConnections) >= s.maxConnectionsPerIP {
			log.Debug().Str("addr", ip).Msg("client connection refused, too many connections")

			return fmt.Errorf("too many connections")
		}
		info.ActiveConnections[client.Port()] = &ClientConnInfo{
			Client:      client,
			LastRequest: time.Now().Unix(),
		}

		log.Debug().Str("addr", ip).Uint16("port", client.Port()).Int("connections", len(info.ActiveConnections)).Msg("new client connected")
		metrics.Global.ActiveADNLConnections.Add(1)

		return nil
	})
	s.srv.SetDisconnectHook(func(client *liteclient.ServerClient) {
		ip := client.IP()
		s.mx.Lock()
		if info := s.ips[ip]; info != nil {
			delete(info.ActiveConnections, client.Port())
			if len(info.ActiveConnections) == 0 {
				delete(s.ips, ip)
			}
		}
		s.mx.Unlock()

		log.Debug().Str("addr", ip).Msg("client disconnected")
		metrics.Global.ActiveADNLConnections.Sub(1)
	})

	if s.maxKeepAlive > 0 {
		go func() {
			for {
				start := time.Now()
				last := start.Add(-s.maxKeepAlive).Unix()
				s.mx.Lock()
				for _, ip := range s.ips {
					for _, client := range ip.ActiveConnections {
						if client.LastRequest < last {
							client.Client.Close()
						}
					}
				}
				s.mx.Unlock()
				log.Debug().Str("took", time.Since(start).String()).Msg("connections cleanup completed")

				time.Sleep(5 * time.Second)
			}
		}()
	}
	return s
}

func (s *ProxyBalancer) Listen(addr string) error {
	return s.srv.Listen(addr)
}

var crcTable = crc64.MakeTable(crc64.ECMA)

func (s *ProxyBalancer) handleRequest(ctx context.Context, sc *liteclient.ServerClient, msg tl.Serializable) error {
	lim := s.configs[string(sc.ServerKey())]
	if lim == nil {
		return fmt.Errorf("unknown server key")
	}

	s.mx.RLock()
	if ip := s.ips[sc.IP()]; ip != nil {
		if conn := ip.ActiveConnections[sc.Port()]; conn != nil {
			atomic.StoreInt64(&conn.LastRequest, time.Now().Unix())
		}
	}
	s.mx.RUnlock()

	limited := false
	defer func() {
		metrics.Global.Requests.WithLabelValues(lim.name, reflect.TypeOf(msg).String(), fmt.Sprint(limited)).Add(1)
	}()

	switch m := msg.(type) {
	case adnl.MessageQuery:
		switch q := m.Data.(type) {
		case liteclient.LiteServerQuery:
			cost := int64(1) // TODO: dynamic cost (depending on query)

			if (lim.limiterPerIP != nil && lim.limiterPerIP.Add(sc.IP(), cost) != cost) ||
				(lim.limiterPerKey != nil && lim.limiterPerKey.Add(cost) != cost) {
				limited = true
				return sc.Send(adnl.MessageAnswer{ID: m.ID, Data: ton.LSError{
					Code: 429,
					Text: "too many requests",
				}})
			}

			go func() {
				var resp tl.Serializable

				tm := time.Now()
				hitType := HitTypeBackend
				if !s.onlyProxy {
					if wt, ok := q.Data.(ton.WaitMasterchainSeqno); ok {
						q.Data = []tl.Serializable{wt, wt}
					}

					switch v := q.Data.(type) {
					case []tl.Serializable: // wait master probably
						if len(v) != 2 {
							_ = sc.Send(adnl.MessageAnswer{ID: m.ID, Data: ton.LSError{
								Code: 400,
								Text: "unexpected len of queries",
							}})
							return
						}

						wt, ok := v[0].(ton.WaitMasterchainSeqno)
						if !ok {
							_ = sc.Send(adnl.MessageAnswer{ID: m.ID, Data: ton.LSError{
								Code: 400,
								Text: "unexpected first query type",
							}})
							return
						}

						tmWait := time.Now()
						if err := s.cache.WaitMasterBlock(ctx, uint32(wt.Seqno), time.Duration(wt.Timeout)*time.Second); err != nil {
							if ls, ok := err.(ton.LSError); ok {
								_ = sc.Send(adnl.MessageAnswer{ID: m.ID, Data: ls})
								return
							}
							return
						}
						log.Debug().Dur("took", time.Since(tmWait)).Msg("master block wait finished")
						q.Data = v[1]

						// reset time to not track waiting time
						tm = time.Now()
					}

					switch v := q.Data.(type) {
					case ton.GetVersion:
						hitType = HitTypeEmulated
						resp = ton.Version{
							Mode:         0,
							Version:      0x101,
							Capabilities: 7,
							Now:          uint32(time.Now().Unix()),
						}
					case ton.GetTime:
						hitType = HitTypeEmulated
						resp = ton.CurrentTime{
							Now: uint32(time.Now().Unix()),
						}
					case ton.WaitMasterchainSeqno:
						// we simulated it before, but there is no following request type, so we answer same as LS
						hitType = HitTypeEmulated
						resp = ton.LSError{
							Code: 0,
							Text: "Not enough data to read at 0",
						}
					case ton.GetMasterchainInfoExt:
						resp, hitType = s.handleGetMasterchainInfoExt(ctx, &v)
					case ton.GetMasterchainInf:
						resp, hitType = s.handleGetMasterchainInfo(ctx)
					case ton.GetLibraries:
						resp, hitType = s.handleGetLibraries(ctx, &v)
					case ton.GetOneTransaction:
						resp, hitType = s.handleGetTransaction(ctx, &v)
					case ton.GetBlockData:
						resp, hitType = s.handleGetBlock(ctx, &v)
					case ton.GetAccountState:
						resp, hitType = s.handleGetAccount(ctx, &v)
					case ton.LookupBlock:
						resp, hitType = s.handleLookupBlock(ctx, &v)
					case ton.GetBlockHeader:
					case ton.GetConfigAll:
					case ton.GetBlockProof:
					case ton.GetConfigParams:
					case ton.GetAllShardsInfo:
					case ton.ListBlockTransactions:
					case ton.ListBlockTransactionsExt:
					case ton.RunSmcMethod:
					case ton.GetState:
					case ton.GetAccountStatePruned:
					case ton.GetShardInfo:
					case ton.GetTransactions:
					case ton.GetShardBlockProof:
					case ton.SendMessage:
					default:
						log.Debug().Str("ip", sc.IP()).Type("request", q.Data).Msg("unknown request type")

						q.Data = ton.Object{} // overwrite to not make metrics dirty
						hitType = HitTypeFailedValidate
						resp = ton.LSError{
							Code: -400,
							Text: "unknown request type",
						}
					}
				}

				defer func() {
					if ls, ok := resp.(ton.LSError); ok {
						metrics.Global.LSErrors.WithLabelValues(lim.name, reflect.TypeOf(q.Data).String(), fmt.Sprint(ls.Code)).Add(1)
					}

					snc := time.Since(tm)
					metrics.Global.Queries.WithLabelValues(lim.name, reflect.TypeOf(q.Data).String(), hitType).Observe(snc.Seconds())
					log.Debug().Str("ip", sc.IP()).Type("request", q.Data).Type("response", resp).Dur("took", snc).Msg("query finished")
				}()

				var gpKey uint64
				if resp == nil && s.gpCache != nil {
					rqData, err := tl.Serialize(q.Data, true)
					if err != nil {
						log.Warn().Type("request", q.Data).Msg("serialization for hash failed")

						resp = ton.LSError{
							Code: 400,
							Text: "request serialization failed",
						}
					}
					gpKey = crc64.Checksum(rqData, crcTable)

					lead := false
					s.iflCacheMx.Lock()
					wt := s.inflightCache[gpKey]
					if wt == nil {
						wt = &CacheWaiter{
							W:      make(chan bool),
							Result: nil,
						}
						s.inflightCache[gpKey] = wt
						lead = true
					}
					s.iflCacheMx.Unlock()

					if lead {
						defer func() {
							wt.Result = resp
							close(wt.W)

							s.iflCacheMx.Lock()
							delete(s.inflightCache, gpKey)
							s.iflCacheMx.Unlock()
						}()
					} else {
						select {
						case <-ctx.Done():
							resp = ton.LSError{
								Code: 400,
								Text: "canceled",
							}
						case <-wt.W:
							resp = wt.Result
							hitType = HitTypeInflightCache
							log.Debug().Type("request", q.Data).Int("cache_sz", len(s.inflightCache)).Msg("result from inflight cache")
						}
					}

					if resp == nil {
						resp, _ = s.gpCache.Get(gpKey)
						if resp != nil {
							log.Debug().Type("request", q.Data).Type("response", resp).Msg("fetched from gp cache")
							hitType = HitTypeGPCache
						}
					}
				}

				if !s.onlyProxy && resp == nil && s.cache.MethodEmulationEnabled() {
					// we have it here to cache calls, because they are heavy
					if v, ok := q.Data.(ton.RunSmcMethod); ok {
						resp, hitType = s.handleRunSmcMethod(ctx, &v)
					}
				}

				if resp == nil {
					log.Debug().Type("request", q.Data).Msg("direct proxy")
					// we expect to have only fast nodes, so timeout is short
					ctx, cancel := context.WithTimeout(ctx, 7*time.Second)

					lsTm := time.Now()
					err := s.backendBalancer.GetClient().QueryLiteserver(ctx, q.Data, &resp)
					cancel()
					if err != nil {
						if strings.HasSuffix(err.Error(), "context canceled") {
							resp = ton.LSError{
								Code: 400,
								Text: "canceled",
							}
						} else {
							log.Warn().Err(err).Type("request", q.Data).Dur("took", time.Since(lsTm)).Msg("query failed")

							resp = ton.LSError{
								Code: 502,
								Text: "backend node timeout",
							}
						}
					} else if s.gpCache != nil {
						if _, ok := resp.(ton.LSError); !ok {
							// do not cache errors
							s.gpCache.Add(gpKey, resp)
						}
					}
				}

				_ = sc.Send(adnl.MessageAnswer{ID: m.ID, Data: resp})
			}()

			return nil
		}
	case liteclient.TCPPing:
		return sc.Send(liteclient.TCPPong{RandomID: m.RandomID})
	}

	return fmt.Errorf("something unknown: %s", reflect.TypeOf(msg).String())
}

func (s *ProxyBalancer) handleRunSmcMethod(ctx context.Context, v *ton.RunSmcMethod) (tl.Serializable, string) {
	block, cachedBlock, err := s.cache.CacheBlockIfNeeded(ctx, v.ID)
	if err != nil {
		if ls, ok := err.(ton.LSError); ok {
			return ls, HitTypeFailedValidate
		}
		if ctx.Err() != nil {
			return ErrTimeout, HitTypeFailedValidate
		}

		log.Warn().Err(err).Type("request", v).Msg("failed to get block")
		return ton.LSError{
			Code: 500,
			Text: "failed to resolve block",
		}, HitTypeFailedInternal
	}

	if block == nil {
		log.Debug().Int32("wc", v.ID.Workchain).Uint32("seqno", v.ID.SeqNo).Msg("direct run method, block is not for caching")

		// block is too old for cache, for now we proxy it to backend,
		// but maybe it is reasonable to throw an error
		return nil, HitTypeBackend
	}

	masterBlock, cachedMasterBlock, err := s.cache.GetMasterBlock(ctx, block.MasterID)
	if err != nil {
		if ls, ok := err.(ton.LSError); ok {
			return ls, HitTypeFailedValidate
		}
		if ctx.Err() != nil {
			return ErrTimeout, HitTypeFailedValidate
		}

		log.Warn().Err(err).Type("request", v).Msg("failed to get master block")
		return ton.LSError{
			Code: 500,
			Text: "failed to resolve master block",
		}, HitTypeFailedInternal
	}

	addr := address.NewAddress(0, byte(v.Account.Workchain), v.Account.ID)
	state, cachedState, err := s.cache.GetAccountStateInBlock(ctx, block, addr)
	if err != nil {
		if ls, ok := err.(ton.LSError); ok {
			return ls, HitTypeFailedValidate
		}
		if ctx.Err() != nil {
			return ErrTimeout, HitTypeFailedValidate
		}

		log.Warn().Err(err).Type("request", v).Msg("failed to get account")

		return ton.LSError{
			Code: 500,
			Text: "failed to get account state",
		}, HitTypeFailedInternal
	}

	if state.State == nil {
		return ton.LSError{
			Code: ton.ErrCodeContractNotInitialized,
			Text: "contract is not initialized",
		}, HitTypeFailedValidate
	}

	var st tlb.AccountState
	if err = st.LoadFromCell(state.State.BeginParse()); err != nil {
		log.Warn().Err(err).Type("request", v).Msg("failed to parse account")
		return ton.LSError{
			Code: 500,
			Text: "failed to parse account state: " + err.Error(),
		}, HitTypeFailedInternal
	}

	if st.StateInit == nil || st.StateInit.Code == nil {
		return ton.RunMethodResult{
			Mode:       v.Mode,
			ID:         v.ID,
			ShardBlock: state.Shard,
			ExitCode:   ton.ErrCodeContractNotInitialized,
		}, HitTypeEmulated
	}

	libsCodes, cachedLibs, err := s.cache.GetLibraries(ctx, findLibs(st.StateInit.Code))
	if err != nil {
		if ls, ok := err.(ton.LSError); ok {
			return ls, HitTypeFailedValidate
		}
		if ctx.Err() != nil {
			return ErrTimeout, HitTypeFailedValidate
		}

		return ton.LSError{
			Code: 500,
			Text: "failed resolve libraries: " + err.Error(),
		}, HitTypeFailedInternal
	}

	// TODO: precompiled contracts in go

	var seed = make([]byte, 32)
	_, _ = rand.Read(seed)

	libsCell := libsCodes.AsCell()
	if libsCell == nil {
		libsCell = cell.BeginCell().EndCell()
	}

	c7tuple, err := emulate.PrepareC7(addr, time.Now(), seed, st.Balance.Nano(), masterBlock.Config, st.StateInit.Code)
	if err != nil {
		return ton.LSError{
			Code: 500,
			Text: "failed to prepare c7: " + err.Error(),
		}, HitTypeFailedInternal
	}

	stack := tlb.NewStack()
	stack.Push(c7tuple)
	c7cell, err := stack.ToCell()
	if err != nil {
		return ton.LSError{
			Code: 500,
			Text: "failed to build c7 stack: " + err.Error(),
		}, HitTypeFailedInternal
	}

	log.Debug().Hex("code_hash", st.StateInit.Code.Hash()).Int32("method_id", int32(v.MethodID)).Msg("running get method on contract")

	etm := time.Now()
	res, err := emulate.RunGetMethod(emulate.RunMethodParams{
		Code:  st.StateInit.Code,
		Data:  st.StateInit.Data,
		Stack: v.Params,
		Params: emulate.MethodConfig{
			C7:   c7cell,
			Libs: libsCell,
		},
		MethodID: int32(v.MethodID),
	}, 1_000_000)
	if err != nil {
		log.Warn().Err(err).Type("request", v).Msg("failed to emulate get method")

		return ton.LSError{
			Code: 500,
			Text: "failed to emulate run method: " + err.Error(),
		}, HitTypeFailedInternal
	}
	took := time.Since(etm)
	metrics.Global.RunGetMethodEmulation.Observe(took.Seconds())
	log.Debug().Dur("took", took).Msg("get method emulation finished")

	var stateProof *cell.Cell

	if v.Mode&2 != 0 {
		stateProof, err = state.State.CreateProof(cell.CreateProofSkeleton())
		if err != nil {
			log.Warn().Err(err).Type("request", v).Msg("failed to prepare state proof args")

			return ton.LSError{
				Code: 500,
				Text: "failed to prepare state proof args: " + err.Error(),
			}, HitTypeFailedInternal
		}
	}

	hit := HitTypeBackend
	if cachedBlock && cachedMasterBlock && cachedLibs {
		hit = HitTypeEmulated
		if cachedState {
			hit = HitTypeCache
		}
	}

	return ton.RunMethodResult{
		Mode:       v.Mode,
		ID:         v.ID,
		ShardBlock: state.Shard,
		ShardProof: state.ShardProof,
		Proof:      state.Proof,
		StateProof: stateProof,
		InitC7:     c7cell,
		LibExtras:  nil,
		ExitCode:   res.ExitCode,
		Result:     res.Stack,
	}, hit
}

func (s *ProxyBalancer) handleGetMasterchainInfoExt(ctx context.Context, v *ton.GetMasterchainInfoExt) (tl.Serializable, string) {
	if v.Mode != 0 {
		return ton.LSError{
			Code: 400,
			Text: "non zero mode is not supported",
		}, HitTypeFailedValidate
	}

	block, cached, err := s.cache.GetLastMasterBlock(ctx)
	if err != nil {
		if ls, ok := err.(ton.LSError); ok {
			return ls, HitTypeFailedInternal
		}
		if ctx.Err() != nil {
			return ErrTimeout, HitTypeFailedValidate
		}

		log.Warn().Err(err).Type("request", v).Msg("failed to get last master")
		return ton.LSError{
			Code: 500,
			Text: "failed to resolve master block",
		}, HitTypeFailedInternal
	}

	zero, err := s.cache.GetZeroState()
	if err != nil {
		log.Warn().Err(err).Type("request", v).Msg("failed to get zero state")

		return ton.LSError{
			Code: 500,
			Text: "failed to resolve zero state",
		}, HitTypeFailedInternal
	}

	hit := HitTypeBackend
	if cached {
		hit = HitTypeCache
	}

	return ton.MasterchainInfoExt{
		Mode:          v.Mode,
		Version:       0x101,
		Capabilities:  7,
		Last:          block.Block.ID,
		LastUTime:     block.GenTime,
		Now:           uint32(time.Now().Unix()),
		StateRootHash: block.StateHash,
		Init:          zero,
	}, hit
}

func (s *ProxyBalancer) handleGetMasterchainInfo(ctx context.Context) (tl.Serializable, string) {
	block, cached, err := s.cache.GetLastMasterBlock(ctx)
	if err != nil {
		if ls, ok := err.(ton.LSError); ok {
			return ls, HitTypeFailedInternal
		}
		if ctx.Err() != nil {
			return ErrTimeout, HitTypeFailedValidate
		}

		log.Warn().Err(err).Type("request", ton.GetMasterchainInf{}).Msg("failed to get last master")
		return ton.LSError{
			Code: 500,
			Text: "failed to resolve master block",
		}, HitTypeFailedInternal
	}

	zero, err := s.cache.GetZeroState()
	if err != nil {
		log.Warn().Err(err).Type("request", ton.GetMasterchainInf{}).Msg("failed to get zero state")

		return ton.LSError{
			Code: 500,
			Text: "failed to resolve zero state",
		}, HitTypeFailedInternal
	}

	hit := HitTypeBackend
	if cached {
		hit = HitTypeCache
	}
	return ton.MasterchainInfo{
		Last:          block.Block.ID,
		StateRootHash: block.StateHash,
		Init:          zero,
	}, hit
}

func (s *ProxyBalancer) handleGetLibraries(ctx context.Context, v *ton.GetLibraries) (tl.Serializable, string) {
	libs, cached, err := s.cache.GetLibraries(ctx, v.LibraryList)
	if err != nil {
		if ls, ok := err.(ton.LSError); ok {
			return ls, HitTypeFailedValidate
		}
		if ctx.Err() != nil {
			return ErrTimeout, HitTypeFailedValidate
		}

		log.Warn().Err(err).Type("request", v).Msg("failed to get libraries")
		return ton.LSError{
			Code: 500,
			Text: "failed to get libraries",
		}, HitTypeFailedInternal
	}

	all, err := libs.LoadAll()
	if err != nil {
		log.Warn().Err(err).Type("request", v).Msg("failed to load libraries")
		return ton.LSError{
			Code: 500,
			Text: "failed to load libraries",
		}, HitTypeFailedInternal
	}

	var libsRes []*ton.LibraryEntry
	for _, kv := range all {
		libsRes = append(libsRes, &ton.LibraryEntry{
			Hash: kv.Key.MustLoadSlice(256),
			Data: kv.Value.MustToCell(),
		})
	}

	hit := HitTypeBackend
	if cached {
		hit = HitTypeCache
	}

	return ton.LibraryResult{
		Result: libsRes,
	}, hit
}

func (s *ProxyBalancer) handleGetBlock(ctx context.Context, v *ton.GetBlockData) (tl.Serializable, string) {
	block, blockFromCache, err := s.cache.CacheBlockIfNeeded(ctx, v.ID)
	if err != nil {
		if ls, ok := err.(ton.LSError); ok {
			return ls, HitTypeFailedValidate
		}
		if ctx.Err() != nil {
			return ErrTimeout, HitTypeFailedValidate
		}

		log.Warn().Err(err).Type("request", v).Msg("failed to get block")
		return ton.LSError{
			Code: 500,
			Text: "failed to resolve block",
		}, HitTypeFailedInternal
	}

	if block == nil {
		return nil, HitTypeBackend
	}

	hitType := HitTypeBackend
	if blockFromCache {
		hitType = HitTypeCache
	}

	return &ton.BlockData{
		ID:      block.ID,
		Payload: block.DataRaw,
	}, hitType
}

func (s *ProxyBalancer) handleGetTransaction(ctx context.Context, v *ton.GetOneTransaction) (tl.Serializable, string) {
	block, blockFromCache, err := s.cache.CacheBlockIfNeeded(ctx, v.ID)
	if err != nil {
		if ls, ok := err.(ton.LSError); ok {
			return ls, HitTypeFailedValidate
		}
		if ctx.Err() != nil {
			return ErrTimeout, HitTypeFailedValidate
		}

		log.Warn().Err(err).Type("request", v).Msg("failed to get block")
		return ton.LSError{
			Code: 500,
			Text: "failed to resolve block",
		}, HitTypeFailedInternal
	}

	if block == nil {
		return nil, HitTypeBackend
	}

	data, err := s.cache.GetTransaction(ctx, block, v.AccID, v.LT)
	if err != nil {
		if ls, ok := err.(ton.LSError); ok {
			return ls, HitTypeFailedValidate
		}
		if ctx.Err() != nil {
			return ErrTimeout, HitTypeFailedValidate
		}

		log.Warn().Err(err).Type("request", v).Msg("failed to get transaction")
		return ton.LSError{
			Code: 500,
			Text: "failed to get transaction",
		}, HitTypeFailedInternal
	}

	if blockFromCache {
		return data, HitTypeEmulated
	}
	return data, HitTypeBackend
}

func (s *ProxyBalancer) handleGetAccount(ctx context.Context, v *ton.GetAccountState) (tl.Serializable, string) {
	block, blockFromCache, err := s.cache.CacheBlockIfNeeded(ctx, v.ID)
	if err != nil {
		if ls, ok := err.(ton.LSError); ok {
			return ls, HitTypeFailedValidate
		}
		if ctx.Err() != nil {
			return ErrTimeout, HitTypeFailedValidate
		}

		log.Warn().Err(err).Type("request", v).Msg("failed to get block")
		return ton.LSError{
			Code: 500,
			Text: "failed to resolve block",
		}, HitTypeFailedInternal
	}

	if block == nil {
		return nil, HitTypeBackend
	}

	state, cachedState, err := s.cache.GetAccountStateInBlock(ctx, block, address.NewAddress(0, byte(v.Account.Workchain), v.Account.ID))
	if err != nil {
		if ls, ok := err.(ton.LSError); ok {
			return ls, HitTypeFailedValidate
		}
		if ctx.Err() != nil {
			return ErrTimeout, HitTypeFailedValidate
		}

		log.Warn().Err(err).Type("request", v).Msg("failed to get account state")
		return ton.LSError{
			Code: 500,
			Text: "failed to get account state",
		}, HitTypeFailedInternal
	}

	if blockFromCache && cachedState {
		return state, HitTypeCache
	}
	return state, HitTypeBackend
}

func (s *ProxyBalancer) handleLookupBlock(ctx context.Context, v *ton.LookupBlock) (tl.Serializable, string) {
	if v.Mode != 1 {
		log.Debug().Int32("seqno", v.ID.Seqno).Int64("shard", v.ID.Shard).
			Int32("wc", v.ID.Workchain).Uint32("mode", v.Mode).Msg("requested lookup block with non 1 mode")
		// TODO: support non zero mode too
		return nil, HitTypeBackend
	}

	hdr, err := s.cache.LookupBlockInCache(v.ID)
	if err != nil {
		log.Warn().Err(err).Type("request", v).Msg("failed to get lookup block in cache")

		return ton.LSError{
			Code: 500,
			Text: "failed to lookup block in cache",
		}, HitTypeFailedInternal
	}

	if hdr == nil {
		// not in cache
		log.Debug().Int32("seqno", v.ID.Seqno).Int64("shard", v.ID.Shard).
			Int32("wc", v.ID.Workchain).Uint32("mode", v.Mode).Msg("lookup block cache miss")
		return nil, HitTypeBackend
	}
	return hdr, HitTypeCache
}

func findLibs(code *cell.Cell) (res [][]byte) {
	if code.RefsNum() == 0 && code.GetType() == cell.LibraryCellType {
		slc := code.BeginParse()
		slc.MustLoadSlice(8)
		return [][]byte{slc.MustLoadSlice(256)}
	}

	for i := 0; i < int(code.RefsNum()); i++ {
		res = append(res, findLibs(code.MustPeekRef(i))...)
	}
	return res
}

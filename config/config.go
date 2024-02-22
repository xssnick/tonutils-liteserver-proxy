package config

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

type BackendLiteserver struct {
	Name string
	Addr string
	Key  []byte
}

type ClientConfig struct {
	Name           string
	PrivateKey     []byte
	CapacityPerIP  int64
	CapacityPerKey int64
	CoolingPerSec  float64
}

type CacheConfig struct {
	DisableProofChainKeyBlocksCache bool
	DisableGetMethodsEmulation      bool
	MaxCachedAccountsPerBlock       uint32
	MaxCachedLibraries              uint32
	MaxMasterBlockSeqnoDiffToCache  uint32
	MaxShardBlockSeqnoDiffToCache   uint32
}

type Config struct {
	ListenAddr               string
	MetricsAddr              string
	MetricsNamespace         string
	DisableEmulationAndCache bool
	CacheConfig              CacheConfig
	Clients                  []ClientConfig
	Backends                 []BackendLiteserver
	MaxConnectionsPerIP      uint32
	MaxKeepAliveSeconds      uint32
	ResponseGeneralCacheSize uint32
}

func LoadConfig(path string) (*Config, error) {
	path, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}

	dir := filepath.Dir(path)
	if _, err = os.Stat(dir); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			err = os.MkdirAll(dir, os.ModePerm)
		}
		if err != nil {
			return nil, fmt.Errorf("failed to check directory: %w", err)
		}
	}

	_, err = os.Stat(path)
	if os.IsNotExist(err) {
		_, private, err := ed25519.GenerateKey(nil)
		if err != nil {
			return nil, err
		}

		exampleKey, _ := base64.StdEncoding.DecodeString("n4VDnSCUuSpjnCyUk9e3QOOd6o0ItSWYbTnW3Wnn8wk=")
		cfg := &Config{
			ListenAddr:               "0.0.0.0:7445",
			MetricsAddr:              "0.0.0.0:8058",
			MetricsNamespace:         "basic",
			DisableEmulationAndCache: false,
			CacheConfig: CacheConfig{
				DisableProofChainKeyBlocksCache: false,
				MaxCachedAccountsPerBlock:       256,
				MaxCachedLibraries:              8192,
				MaxMasterBlockSeqnoDiffToCache:  60,
				MaxShardBlockSeqnoDiffToCache:   12,
			},
			Clients: []ClientConfig{
				{
					Name:           "default",
					PrivateKey:     private.Seed(),
					CapacityPerIP:  100,
					CapacityPerKey: 0,
					CoolingPerSec:  5,
				},
			},
			Backends: []BackendLiteserver{
				{
					Addr: "5.9.10.47:19949",
					Key:  exampleKey,
				},
			},
			MaxConnectionsPerIP:      16,
			MaxKeepAliveSeconds:      60,
			ResponseGeneralCacheSize: 1024,
		}

		err = SaveConfig(cfg, path)
		if err != nil {
			return nil, fmt.Errorf("failed to save config: %w", err)
		}

		return cfg, nil
	} else if err == nil {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("failed to read config: %w", err)
		}

		var cfg Config
		err = json.Unmarshal(data, &cfg)
		if err != nil {
			return nil, fmt.Errorf("failed to parse config: %w", err)
		}

		return &cfg, nil
	}

	return nil, err
}

func SaveConfig(cfg *Config, path string) error {
	data, err := json.MarshalIndent(cfg, "", "\t")
	if err != nil {
		return err
	}

	err = os.WriteFile(path, data, 0766)
	if err != nil {
		return err
	}
	return nil
}

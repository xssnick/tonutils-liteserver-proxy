package server

import (
	"crypto/ed25519"
	"testing"
	"time"

	"github.com/xssnick/tonutils-liteserver-proxy/config"
)

func TestNewProxyBalancerSkipsGeneralPurposeCacheInProxyOnlyMode(t *testing.T) {
	proxy := NewProxyBalancer(testProxyClientConfigs(), nil, nil, true, 0, 0, map[string]int{
		"general":     10,
		"get_account": 10,
		"run_method":  10,
	})

	if proxy.gpCache != nil {
		t.Fatalf("expected gp cache to be disabled in proxy-only mode")
	}
}

func TestNewProxyBalancerInitializesGeneralPurposeCacheWhenNotProxyOnly(t *testing.T) {
	proxy := NewProxyBalancer(testProxyClientConfigs(), nil, nil, false, 0, 0, map[string]int{
		"general": 10,
	})

	if proxy.gpCache[GPTypeGeneral] == nil {
		t.Fatalf("expected general purpose cache to be initialized")
	}
}

func testProxyClientConfigs() []config.ClientConfig {
	seed := make([]byte, ed25519.SeedSize)
	seed[0] = 1
	return []config.ClientConfig{{
		Name:       "test",
		PrivateKey: seed,
	}}
}

func TestNormalizeWaitMasterchainTimeout(t *testing.T) {
	tests := []struct {
		name         string
		timeoutMS    int32
		maxKeepAlive time.Duration
		expected     time.Duration
	}{
		{
			name:      "milliseconds",
			timeoutMS: 6500,
			expected:  6500 * time.Millisecond,
		},
		{
			name:      "zero timeout",
			timeoutMS: 0,
			expected:  0,
		},
		{
			name:      "negative timeout",
			timeoutMS: -1,
			expected:  0,
		},
		{
			name:      "cap large request",
			timeoutMS: 65000,
			expected:  maxWaitMasterchainTimeout,
		},
		{
			name:         "respect keep alive",
			timeoutMS:    65000,
			maxKeepAlive: 5 * time.Second,
			expected:     5 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := normalizeWaitMasterchainTimeout(tt.timeoutMS, tt.maxKeepAlive)
			if got != tt.expected {
				t.Fatalf("unexpected timeout: got %v want %v", got, tt.expected)
			}
		})
	}
}

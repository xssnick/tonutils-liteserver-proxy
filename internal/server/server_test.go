package server

import (
	"testing"
	"time"
)

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

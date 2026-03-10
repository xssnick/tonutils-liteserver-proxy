package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSaveConfigUsesPrivatePermissions(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")

	if err := SaveConfig(&Config{}, path); err != nil {
		t.Fatalf("failed to save config: %v", err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("failed to stat config: %v", err)
	}

	if perms := info.Mode().Perm(); perms != 0600 {
		t.Fatalf("unexpected config permissions: got %o want %o", perms, 0600)
	}
}

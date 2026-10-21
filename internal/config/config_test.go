package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDefaults(t *testing.T) {
	config, err := Load("")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if config.UDP.Listen != "127.0.0.1:9301" || config.Web.Listen != "127.0.0.1:8766" || config.Database.Path != "data/linkage.db" || config.Protocol.EventTimezone != "Asia/Shanghai" {
		t.Fatalf("unexpected defaults: %+v", config)
	}
}

func TestEnvironmentOverridesYAML(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("udp:\n  listen: 127.0.0.1:9401\n"), 0600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	t.Setenv("LINKAGE_UDP_LISTEN", "127.0.0.1:9501")
	config, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if config.UDP.Listen != "127.0.0.1:9501" {
		t.Fatalf("UDP listen = %q, want environment override", config.UDP.Listen)
	}
}

func TestLoadRejectsUnknownFields(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("udp:\n  unknown: value\n"), 0600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	if _, err := Load(path); err == nil {
		t.Fatal("Load() accepted unknown fields")
	}
}

func TestLoadExampleConfig(t *testing.T) {
	path := filepath.Join("..", "..", "config", "linkage-gateway.example.yaml")
	config, err := Load(path)
	if err != nil {
		t.Fatalf("Load(example config) error = %v", err)
	}
	if config.Version != 1 || config.Web.ClientQueue != 64 || config.Protocol.EventTimezone != "Asia/Shanghai" {
		t.Fatalf("unexpected example config values: %+v", config)
	}
}

func TestLoadRejectsUnsupportedVersion(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("version: 2\n"), 0600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	if _, err := Load(path); err == nil {
		t.Fatal("Load() accepted an unsupported config version")
	}
}

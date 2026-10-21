package config

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Version  int            `yaml:"version"`
	UDP      UDPConfig      `yaml:"udp"`
	Web      WebConfig      `yaml:"web"`
	Database DatabaseConfig `yaml:"database"`
	Protocol ProtocolConfig `yaml:"protocol"`
	Logging  LoggingConfig  `yaml:"logging"`
}

type UDPConfig struct {
	Listen         string `yaml:"listen"`
	MaxPacketBytes int    `yaml:"max_packet_bytes"`
}

type WebConfig struct {
	Listen         string   `yaml:"listen"`
	WebSocketPath  string   `yaml:"websocket_path"`
	AllowedOrigins []string `yaml:"allowed_origins"`
	ClientQueue    int      `yaml:"client_queue_size"`
}

type DatabaseConfig struct {
	Path          string `yaml:"path"`
	BusyTimeoutMS int    `yaml:"busy_timeout_ms"`
	JournalMode   string `yaml:"journal_mode"`
}

type ProtocolConfig struct {
	TextEncoding  string `yaml:"text_encoding"`
	EventTimezone string `yaml:"event_timezone"`
	DedupWindowMS int    `yaml:"dedup_window_ms"`
}

type LoggingConfig struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"`
}

func Defaults() Config {
	return Config{
		Version: 1,
		UDP: UDPConfig{
			Listen:         "127.0.0.1:9301",
			MaxPacketBytes: 65535,
		},
		Web: WebConfig{
			Listen:         "127.0.0.1:8766",
			WebSocketPath:  "/ws/linkage",
			AllowedOrigins: []string{"http://localhost:5173"},
			ClientQueue:    64,
		},
		Database: DatabaseConfig{
			Path:          "data/linkage.db",
			BusyTimeoutMS: 5000,
			JournalMode:   "WAL",
		},
		Protocol: ProtocolConfig{
			TextEncoding:  "GB2312",
			EventTimezone: "Asia/Shanghai",
			DedupWindowMS: 3000,
		},
		Logging: LoggingConfig{
			Level:  "INFO",
			Format: "text",
		},
	}
}

func Load(path string) (Config, error) {
	config := Defaults()
	if path != "" {
		file, err := os.Open(path)
		if err != nil {
			return Config{}, fmt.Errorf("open config %q: %w", path, err)
		}
		decoder := yaml.NewDecoder(file)
		decoder.KnownFields(true)
		if err := decoder.Decode(&config); err != nil {
			file.Close()
			return Config{}, fmt.Errorf("decode config %q: %w", path, err)
		}
		var extra any
		if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
			file.Close()
			if err == nil {
				return Config{}, fmt.Errorf("config %q contains multiple YAML documents", path)
			}
			return Config{}, fmt.Errorf("decode trailing config data %q: %w", path, err)
		}
		if err := file.Close(); err != nil {
			return Config{}, fmt.Errorf("close config %q: %w", path, err)
		}
	}
	if err := config.applyEnvironment(); err != nil {
		return Config{}, err
	}
	if err := config.Validate(); err != nil {
		return Config{}, err
	}
	return config, nil
}

func (config *Config) applyEnvironment() error {
	stringOverrides := []struct {
		name  string
		value *string
	}{
		{"LINKAGE_UDP_LISTEN", &config.UDP.Listen},
		{"LINKAGE_WEB_LISTEN", &config.Web.Listen},
		{"LINKAGE_WEBSOCKET_PATH", &config.Web.WebSocketPath},
		{"LINKAGE_DATABASE_PATH", &config.Database.Path},
		{"LINKAGE_DATABASE_JOURNAL_MODE", &config.Database.JournalMode},
		{"LINKAGE_PROTOCOL_TEXT_ENCODING", &config.Protocol.TextEncoding},
		{"LINKAGE_PROTOCOL_EVENT_TIMEZONE", &config.Protocol.EventTimezone},
		{"LINKAGE_LOGGING_LEVEL", &config.Logging.Level},
		{"LINKAGE_LOGGING_FORMAT", &config.Logging.Format},
	}
	for _, override := range stringOverrides {
		if value, exists := os.LookupEnv(override.name); exists {
			*override.value = value
		}
	}

	integerOverrides := []struct {
		name  string
		value *int
	}{
		{"LINKAGE_UDP_MAX_PACKET_BYTES", &config.UDP.MaxPacketBytes},
		{"LINKAGE_WEB_CLIENT_QUEUE_SIZE", &config.Web.ClientQueue},
		{"LINKAGE_DATABASE_BUSY_TIMEOUT_MS", &config.Database.BusyTimeoutMS},
		{"LINKAGE_PROTOCOL_DEDUP_WINDOW_MS", &config.Protocol.DedupWindowMS},
	}
	for _, override := range integerOverrides {
		value, exists := os.LookupEnv(override.name)
		if !exists {
			continue
		}
		parsed, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("environment variable %s must be an integer: %w", override.name, err)
		}
		*override.value = parsed
	}
	if value, exists := os.LookupEnv("LINKAGE_WEB_ALLOWED_ORIGINS"); exists {
		config.Web.AllowedOrigins = splitNonEmpty(value, ",")
	}
	return nil
}

func (config Config) Validate() error {
	if config.Version != 1 {
		return fmt.Errorf("unsupported configuration version %d", config.Version)
	}
	if _, _, err := net.SplitHostPort(config.UDP.Listen); err != nil {
		return fmt.Errorf("invalid UDP listen address: %w", err)
	}
	if config.UDP.MaxPacketBytes < 16 || config.UDP.MaxPacketBytes > 65535 {
		return errors.New("udp.max_packet_bytes must be between 16 and 65535")
	}
	if _, _, err := net.SplitHostPort(config.Web.Listen); err != nil {
		return fmt.Errorf("invalid web listen address: %w", err)
	}
	if !strings.HasPrefix(config.Web.WebSocketPath, "/") {
		return errors.New("web.websocket_path must start with /")
	}
	if config.Web.ClientQueue < 1 || config.Web.ClientQueue > 4096 {
		return errors.New("web.client_queue_size must be between 1 and 4096")
	}
	if strings.TrimSpace(config.Database.Path) == "" {
		return errors.New("database.path must not be empty")
	}
	if config.Database.BusyTimeoutMS < 0 {
		return errors.New("database.busy_timeout_ms must not be negative")
	}
	if !strings.EqualFold(config.Database.JournalMode, "WAL") && !strings.EqualFold(config.Database.JournalMode, "DELETE") {
		return errors.New("database.journal_mode must be WAL or DELETE")
	}
	if !strings.EqualFold(config.Protocol.TextEncoding, "GB2312") {
		return errors.New("protocol.text_encoding currently supports only GB2312")
	}
	if _, err := time.LoadLocation(config.Protocol.EventTimezone); err != nil {
		return fmt.Errorf("invalid protocol.event_timezone: %w", err)
	}
	if config.Protocol.DedupWindowMS < 0 {
		return errors.New("protocol.dedup_window_ms must not be negative")
	}
	if !strings.EqualFold(config.Logging.Level, "DEBUG") && !strings.EqualFold(config.Logging.Level, "INFO") && !strings.EqualFold(config.Logging.Level, "WARN") && !strings.EqualFold(config.Logging.Level, "ERROR") {
		return errors.New("logging.level must be DEBUG, INFO, WARN, or ERROR")
	}
	if !strings.EqualFold(config.Logging.Format, "text") && !strings.EqualFold(config.Logging.Format, "json") {
		return errors.New("logging.format must be text or json")
	}
	return nil
}

func splitNonEmpty(value string, separator string) []string {
	parts := strings.Split(value, separator)
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			result = append(result, part)
		}
	}
	return result
}

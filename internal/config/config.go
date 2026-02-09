package config

import (
	"fmt"
	"os"
	"strconv"
)

// ServerConfig holds configuration for the bridge server.
type ServerConfig struct {
	NATSUrl    string
	NATSJWT    string
	NATSSeed   string
	KVBucket   string
	ListenAddr string
	PublicPort int
	DataPort   int
	ClientID   string
}

// ClientConfig holds configuration for the bridge client.
type ClientConfig struct {
	NATSUrl     string
	NATSJWT     string
	NATSSeed    string
	KVBucket    string
	ClientID    string
	ServerHost  string
	DataPort    int
	LocalTarget string
}

// LoadServerConfig loads server configuration from environment variables.
func LoadServerConfig() (*ServerConfig, error) {
	cfg := &ServerConfig{
		NATSUrl:    getEnv("NATS_URL", ""),
		NATSJWT:    getEnv("NATS_JWT", ""),
		NATSSeed:   getEnv("NATS_SEED", ""),
		KVBucket:   getEnv("KV_BUCKET", "bridge-sessions"),
		ClientID:   getEnv("CLIENT_ID", ""),
		ListenAddr: getEnv("LISTEN_ADDR", "0.0.0.0"),
		PublicPort: getEnvInt("PUBLIC_PORT", 443),
		DataPort:   getEnvInt("DATA_PORT", 9443),
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}

// LoadClientConfig loads client configuration from environment variables.
func LoadClientConfig() (*ClientConfig, error) {
	cfg := &ClientConfig{
		NATSUrl:     getEnv("NATS_URL", ""),
		NATSJWT:     getEnv("NATS_JWT", ""),
		NATSSeed:    getEnv("NATS_SEED", ""),
		KVBucket:    getEnv("KV_BUCKET", "bridge-sessions"),
		ClientID:    getEnv("CLIENT_ID", ""),
		ServerHost:  getEnv("SERVER_HOST", ""),
		DataPort:    getEnvInt("DATA_PORT", 9443),
		LocalTarget: getEnv("LOCAL_TARGET", "localhost:443"),
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}

// Validate checks that required server configuration fields are set.
func (c *ServerConfig) Validate() error {
	if c.NATSUrl == "" {
		return fmt.Errorf("NATS_URL is required")
	}
	if c.NATSJWT == "" {
		return fmt.Errorf("NATS_JWT is required")
	}
	if c.NATSSeed == "" {
		return fmt.Errorf("NATS_SEED is required")
	}
	if c.ClientID == "" {
		return fmt.Errorf("CLIENT_ID is required")
	}
	return nil
}

// Validate checks that required client configuration fields are set.
func (c *ClientConfig) Validate() error {
	if c.NATSUrl == "" {
		return fmt.Errorf("NATS_URL is required")
	}
	if c.NATSJWT == "" {
		return fmt.Errorf("NATS_JWT is required")
	}
	if c.NATSSeed == "" {
		return fmt.Errorf("NATS_SEED is required")
	}
	if c.ClientID == "" {
		return fmt.Errorf("CLIENT_ID is required")
	}
	if c.ServerHost == "" {
		return fmt.Errorf("SERVER_HOST is required")
	}
	return nil
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if i, err := strconv.Atoi(value); err == nil {
			return i
		}
	}
	return defaultValue
}

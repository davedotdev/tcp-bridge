package nats

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	registerSubject = "bridge.register"
)

// Client wraps NATS connection with bridge-specific functionality.
type Client struct {
	nc  *nats.Conn
	js  jetstream.JetStream
	kv  jetstream.KeyValue
	ctx context.Context
}

// ConnectSignal is sent when a new connection needs to be bridged.
type ConnectSignal struct {
	Token     string `json:"token"`
	Timestamp int64  `json:"timestamp"`
}

// RegisterRequest is sent by clients to register with the server.
type RegisterRequest struct {
	ClientID  string `json:"client_id"`
	Timestamp int64  `json:"timestamp"`
}

// RegisterResponse is sent by the server in response to registration.
type RegisterResponse struct {
	Status   string `json:"status"`
	DataPort int    `json:"server_data_port"`
	Error    string `json:"error,omitempty"`
}


// Connect establishes a connection to NATS with JWT/NKey authentication.
func Connect(ctx context.Context, url, jwt, seed, kvBucket string) (*Client, error) {
	opts := []nats.Option{
		nats.UserJWTAndSeed(jwt, seed),
		nats.Name("tcp-bridge"),
		nats.ReconnectWait(2 * time.Second),
		nats.MaxReconnects(-1),
	}

	nc, err := nats.Connect(url, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to NATS: %w", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("failed to create JetStream context: %w", err)
	}

	kv, err := js.KeyValue(ctx, kvBucket)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("failed to access KV bucket %q: %w", kvBucket, err)
	}

	return &Client{
		nc:  nc,
		js:  js,
		kv:  kv,
		ctx: ctx,
	}, nil
}

// Close closes the NATS connection.
func (c *Client) Close() {
	c.nc.Close()
}

// tokenTTL is how long tokens live in KV before auto-expiring.
// Should be longer than connectionTimeout to avoid premature expiry.
const tokenTTL = 60 * time.Second

// StoreToken stores a session token in the KV store for one-time validation.
// Tokens auto-expire after tokenTTL to prevent garbage accumulation.
func (c *Client) StoreToken(token string) error {
	_, err := c.kv.Create(c.ctx, token, []byte("1"), jetstream.KeyTTL(tokenTTL))
	return err
}

// ValidateAndDeleteToken checks if a token exists and deletes it atomically.
// Returns true if the token was valid and has been consumed.
func (c *Client) ValidateAndDeleteToken(token string) bool {
	_, err := c.kv.Get(c.ctx, token)
	if err != nil {
		return false
	}

	// Delete immediately to prevent reuse
	if err := c.kv.Delete(c.ctx, token); err != nil {
		return false
	}

	return true
}

// DeleteToken removes a token from the KV store.
// Used for cleanup when connections timeout or fail.
func (c *Client) DeleteToken(token string) error {
	return c.kv.Delete(c.ctx, token)
}

// PublishConnect sends a connection signal to the specified client.
func (c *Client) PublishConnect(clientID string, signal ConnectSignal) error {
	subject := fmt.Sprintf("bridge.connect.%s", clientID)
	b, err := json.Marshal(signal)
	if err != nil {
		return err
	}
	return c.nc.Publish(subject, b)
}

// SubscribeConnect subscribes to connection signals for this client.
func (c *Client) SubscribeConnect(clientID string, handler func(ConnectSignal)) (*nats.Subscription, error) {
	subject := fmt.Sprintf("bridge.connect.%s", clientID)
	return c.nc.Subscribe(subject, func(msg *nats.Msg) {
		var signal ConnectSignal
		if err := json.Unmarshal(msg.Data, &signal); err != nil {
			return
		}
		handler(signal)
	})
}

// HandleRegister sets up a handler for client registration requests.
func (c *Client) HandleRegister(dataPort int, handler func(req RegisterRequest) RegisterResponse) (*nats.Subscription, error) {
	return c.nc.Subscribe(registerSubject, func(msg *nats.Msg) {
		var req RegisterRequest
		if err := json.Unmarshal(msg.Data, &req); err != nil {
			resp := RegisterResponse{Status: "error", Error: "invalid request"}
			b, _ := json.Marshal(resp)
			msg.Respond(b)
			return
		}

		resp := handler(req)
		resp.DataPort = dataPort
		b, _ := json.Marshal(resp)
		msg.Respond(b)
	})
}

// Register sends a registration request to the server.
func (c *Client) Register(clientID string) (*RegisterResponse, error) {
	req := RegisterRequest{
		ClientID:  clientID,
		Timestamp: time.Now().Unix(),
	}
	b, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	msg, err := c.nc.Request(registerSubject, b, 10*time.Second)
	if err != nil {
		return nil, fmt.Errorf("registration failed: %w", err)
	}

	var resp RegisterResponse
	if err := json.Unmarshal(msg.Data, &resp); err != nil {
		return nil, fmt.Errorf("invalid registration response: %w", err)
	}

	return &resp, nil
}

// Conn returns the underlying NATS connection.
func (c *Client) Conn() *nats.Conn {
	return c.nc
}

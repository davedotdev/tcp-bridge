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
	ConnID    string `json:"conn_id"`
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

// SessionData stored in KV for each pending connection.
type SessionData struct {
	ConnID  string `json:"conn_id"`
	Created int64  `json:"created"`
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

// StoreToken stores a session token in the KV store.
func (c *Client) StoreToken(token, connID string) error {
	data := SessionData{
		ConnID:  connID,
		Created: time.Now().Unix(),
	}
	b, err := json.Marshal(data)
	if err != nil {
		return err
	}
	_, err = c.kv.Put(c.ctx, token, b)
	return err
}

// ValidateAndDeleteToken checks if a token exists and deletes it atomically.
// Returns the connection ID if valid, empty string if not.
func (c *Client) ValidateAndDeleteToken(token string) (string, bool) {
	entry, err := c.kv.Get(c.ctx, token)
	if err != nil {
		return "", false
	}

	// Delete immediately to prevent reuse
	if err := c.kv.Delete(c.ctx, token); err != nil {
		return "", false
	}

	var data SessionData
	if err := json.Unmarshal(entry.Value(), &data); err != nil {
		return "", false
	}

	return data.ConnID, true
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

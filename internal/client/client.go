package client

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/davedotdev/tcp-bridge/internal/config"
	bridgenats "github.com/davedotdev/tcp-bridge/internal/nats"
	"github.com/nats-io/nats.go"
)

// heartbeatInterval is how often the client announces liveness to the
// server. Must be well under the server's client expiry window.
const heartbeatInterval = 10 * time.Second

// Client is the bridge client that connects to the server
// and forwards connections to local services.
type Client struct {
	cfg    *config.ClientConfig
	nats   *bridgenats.Client
	ctx    context.Context
	cancel context.CancelFunc
}

// New creates a new bridge client.
func New(ctx context.Context, cfg *config.ClientConfig) (*Client, error) {
	natsClient, err := bridgenats.Connect(ctx, cfg.NATSUrl, cfg.NATSJWT, cfg.NATSSeed, cfg.KVBucket)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(ctx)

	c := &Client{
		cfg:    cfg,
		nats:   natsClient,
		ctx:    ctx,
		cancel: cancel,
	}

	return c, nil
}

// Run starts the client and blocks until context is cancelled.
func (c *Client) Run() error {
	// Register with server
	if err := c.registerWithRetry(); err != nil {
		return err
	}

	// Subscribe to connection signals
	sub, err := c.nats.SubscribeConnect(c.cfg.ClientID, func(signal bridgenats.ConnectSignal) {
		go c.handleConnectSignal(signal)
	})
	if err != nil {
		return err
	}
	defer sub.Unsubscribe()

	// Keep the server's liveness tracking fresh so a crashed client
	// doesn't hold the bridge forever.
	go c.heartbeatLoop()

	log.Printf("client started: id=%s target=%s", c.cfg.ClientID, c.cfg.LocalTarget)

	// Keep alive and handle reconnection
	<-c.ctx.Done()
	return nil
}

// Stop gracefully shuts down the client.
func (c *Client) Stop() {
	// Tell the server we're going away so another client can connect
	// immediately rather than waiting for heartbeat expiry.
	if err := c.nats.PublishDeregister(c.cfg.ClientID); err != nil {
		log.Printf("failed to deregister: %v", err)
	}
	c.cancel()
	c.nats.Close()
}

func (c *Client) heartbeatLoop() {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := c.nats.PublishHeartbeat(c.cfg.ClientID); err != nil {
				log.Printf("failed to send heartbeat: %v", err)
			}
		case <-c.ctx.Done():
			return
		}
	}
}

func (c *Client) registerWithRetry() error {
	var lastErr error
	for attempt := 0; attempt < 5; attempt++ {
		resp, err := c.nats.Register(c.cfg.ClientID)
		if err != nil {
			lastErr = err
			log.Printf("registration attempt %d failed: %v", attempt+1, err)
			time.Sleep(time.Duration(attempt+1) * time.Second)
			continue
		}

		if resp.Status == "ok" {
			log.Printf("registered with server, data port: %d", resp.DataPort)
			return nil
		}

		msg := resp.Error
		if resp.ConnectedClientID != "" {
			msg = fmt.Sprintf("server is in use by client %q since %s; disconnect it before retrying",
				resp.ConnectedClientID, time.Unix(resp.ConnectedSince, 0).Format(time.RFC3339))
		}
		log.Printf("registration rejected: %s", msg)
		return &RegistrationError{Message: msg}
	}

	return lastErr
}

// handleConnectSignal is called when the server signals a new connection.
func (c *Client) handleConnectSignal(signal bridgenats.ConnectSignal) {
	connector := &Connector{
		cfg:    c.cfg,
		signal: signal,
	}
	connector.Connect()
}

// SetupReconnectHandler sets up automatic re-registration on reconnect.
func (c *Client) SetupReconnectHandler() {
	c.nats.Conn().SetReconnectHandler(func(nc *nats.Conn) {
		log.Println("reconnected to NATS, re-registering...")
		if err := c.registerWithRetry(); err != nil {
			log.Printf("re-registration failed: %v", err)
		}
	})
}

type RegistrationError struct {
	Message string
}

func (e *RegistrationError) Error() string {
	return "registration failed: " + e.Message
}

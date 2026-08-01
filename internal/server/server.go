package server

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/davedotdev/tcp-bridge/internal/config"
	bridgenats "github.com/davedotdev/tcp-bridge/internal/nats"
	"github.com/davedotdev/tcp-bridge/internal/relay"
)

const (
	tokenSize       = 32
	connectionTimeout = 30 * time.Second

	// clientExpiry is how long the active client may go without a
	// heartbeat before its slot is released to other clients.
	clientExpiry = 30 * time.Second
)

// Server is the bridge server that accepts public connections
// and pairs them with authenticated client data connections.
type Server struct {
	cfg      *config.ServerConfig
	nats     *bridgenats.Client
	ctx      context.Context
	cancel   context.CancelFunc

	// Pending connections waiting for client to connect
	pending   map[string]*pendingConn
	pendingMu sync.Mutex

	// The single client currently holding the bridge
	active   *activeClient
	activeMu sync.Mutex
}

type activeClient struct {
	id           string
	registeredAt time.Time
	lastSeen     time.Time
}

type pendingConn struct {
	conn     net.Conn
	token    string
	created  time.Time
	dataChan chan net.Conn
}

// New creates a new bridge server.
func New(ctx context.Context, cfg *config.ServerConfig) (*Server, error) {
	natsClient, err := bridgenats.Connect(ctx, cfg.NATSUrl, cfg.NATSJWT, cfg.NATSSeed, cfg.KVBucket)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(ctx)

	s := &Server{
		cfg:     cfg,
		nats:    natsClient,
		ctx:     ctx,
		cancel:  cancel,
		pending: make(map[string]*pendingConn),
	}

	return s, nil
}

// Run starts the server and blocks until context is cancelled.
func (s *Server) Run() error {
	// Set up registration handler
	_, err := s.nats.HandleRegister(s.cfg.DataPort, s.handleRegister)
	if err != nil {
		return err
	}

	// Track client liveness and explicit disconnects
	if _, err := s.nats.SubscribeHeartbeat(s.handleHeartbeat); err != nil {
		return err
	}
	if _, err := s.nats.SubscribeDeregister(s.handleDeregister); err != nil {
		return err
	}

	// Start cleanup goroutine for stale pending connections
	go s.cleanupStale()

	// Start listeners
	errChan := make(chan error, 2)

	go func() {
		if err := s.startPublicListener(); err != nil {
			errChan <- err
		}
	}()

	go func() {
		if err := s.startDataPortListener(); err != nil {
			errChan <- err
		}
	}()

	log.Printf("server started: public=:%d data=:%d", s.cfg.PublicPort, s.cfg.DataPort)

	select {
	case err := <-errChan:
		return err
	case <-s.ctx.Done():
		return nil
	}
}

// Stop gracefully shuts down the server.
func (s *Server) Stop() {
	s.cancel()
	s.nats.Close()
}

// handleRegister processes a client registration request. Any allowlisted
// client may register, but only one client holds the bridge at a time; a
// second client is rejected with details of the current holder.
func (s *Server) handleRegister(req bridgenats.RegisterRequest) bridgenats.RegisterResponse {
	if !s.cfg.AllowsClient(req.ClientID) {
		log.Printf("rejected registration from unknown client: %s", req.ClientID)
		return bridgenats.RegisterResponse{Status: "error", Error: "unknown client"}
	}

	s.activeMu.Lock()
	defer s.activeMu.Unlock()

	now := time.Now()
	active := s.currentActiveLocked(now)

	if active != nil && active.id != req.ClientID {
		log.Printf("rejected registration from %s: bridge held by %s (since %s)",
			req.ClientID, active.id, active.registeredAt.Format(time.RFC3339))
		return bridgenats.RegisterResponse{
			Status: "busy",
			Error: fmt.Sprintf("another client is already connected: %s (since %s); disconnect it first",
				active.id, active.registeredAt.Format(time.RFC3339)),
			ConnectedClientID: active.id,
			ConnectedSince:    active.registeredAt.Unix(),
		}
	}

	if active != nil {
		// Same client re-registering (e.g. after NATS reconnect)
		active.lastSeen = now
		log.Printf("client re-registered: %s", req.ClientID)
	} else {
		s.active = &activeClient{id: req.ClientID, registeredAt: now, lastSeen: now}
		log.Printf("client registered: %s", req.ClientID)
	}

	return bridgenats.RegisterResponse{Status: "ok"}
}

// handleHeartbeat refreshes the active client's liveness. If no client is
// active (e.g. after a server restart), an allowlisted heartbeat re-adopts
// the client so established sessions survive server restarts.
func (s *Server) handleHeartbeat(msg bridgenats.PresenceMsg) {
	if !s.cfg.AllowsClient(msg.ClientID) {
		return
	}

	s.activeMu.Lock()
	defer s.activeMu.Unlock()

	now := time.Now()
	active := s.currentActiveLocked(now)

	switch {
	case active == nil:
		s.active = &activeClient{id: msg.ClientID, registeredAt: now, lastSeen: now}
		log.Printf("adopted client from heartbeat: %s", msg.ClientID)
	case active.id == msg.ClientID:
		active.lastSeen = now
	}
}

// handleDeregister releases the bridge when the active client disconnects.
func (s *Server) handleDeregister(msg bridgenats.PresenceMsg) {
	s.activeMu.Lock()
	defer s.activeMu.Unlock()

	if s.active != nil && s.active.id == msg.ClientID {
		log.Printf("client deregistered: %s", msg.ClientID)
		s.active = nil
	}
}

// currentActiveLocked returns the active client, clearing it first if its
// heartbeats have lapsed. Caller must hold activeMu.
func (s *Server) currentActiveLocked(now time.Time) *activeClient {
	if s.active != nil && now.Sub(s.active.lastSeen) > clientExpiry {
		log.Printf("active client %s expired (no heartbeat for %s), releasing bridge",
			s.active.id, now.Sub(s.active.lastSeen).Round(time.Second))
		s.active = nil
	}
	return s.active
}

// activeClientID returns the ID of the currently connected client, or ""
// if no client is connected.
func (s *Server) activeClientID() string {
	s.activeMu.Lock()
	defer s.activeMu.Unlock()

	if active := s.currentActiveLocked(time.Now()); active != nil {
		return active.id
	}
	return ""
}

// generateToken creates a cryptographically random token.
func generateToken() (string, error) {
	b := make([]byte, tokenSize)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

// handlePublicConnection processes an incoming public connection.
func (s *Server) handlePublicConnection(conn net.Conn) {
	clientID := s.activeClientID()
	if clientID == "" {
		log.Printf("public connection dropped: no client connected")
		conn.Close()
		return
	}

	token, err := generateToken()
	if err != nil {
		log.Printf("failed to generate token: %v", err)
		conn.Close()
		return
	}

	// Store token in NATS KV for one-time validation
	if err := s.nats.StoreToken(token); err != nil {
		log.Printf("failed to store token: %v", err)
		conn.Close()
		return
	}

	// Create pending connection entry
	pc := &pendingConn{
		conn:     conn,
		token:    token,
		created:  time.Now(),
		dataChan: make(chan net.Conn, 1),
	}

	s.pendingMu.Lock()
	s.pending[token] = pc
	s.pendingMu.Unlock()

	// Signal client
	signal := bridgenats.ConnectSignal{
		Token:     token,
		Timestamp: time.Now().Unix(),
	}
	if err := s.nats.PublishConnect(clientID, signal); err != nil {
		log.Printf("failed to signal client: %v", err)
		s.removePending(token)
		conn.Close()
		return
	}

	// Wait for data connection with timeout
	select {
	case dataConn := <-pc.dataChan:
		relay.Relay(conn, dataConn)
	case <-time.After(connectionTimeout):
		log.Printf("public connection timeout waiting for client")
		s.removePending(token)
		conn.Close()
	case <-s.ctx.Done():
		s.removePending(token)
		conn.Close()
	}
}

// handleDataConnection processes an incoming data port connection.
func (s *Server) handleDataConnection(conn net.Conn) {
	// Read token (first 64 bytes = 32 bytes hex encoded)
	tokenBuf := make([]byte, tokenSize*2)
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	n, err := conn.Read(tokenBuf)
	conn.SetReadDeadline(time.Time{})

	if err != nil || n != tokenSize*2 {
		conn.Close()
		return
	}

	token := string(tokenBuf)

	// Validate and delete token atomically
	if !s.nats.ValidateAndDeleteToken(token) {
		conn.Close()
		return
	}

	// Find pending connection by token
	s.pendingMu.Lock()
	pc, exists := s.pending[token]
	if exists {
		delete(s.pending, token)
	}
	s.pendingMu.Unlock()

	if !exists {
		conn.Close()
		return
	}

	// Send data connection to waiting goroutine
	select {
	case pc.dataChan <- conn:
	default:
		conn.Close()
	}
}

func (s *Server) removePending(token string) {
	s.pendingMu.Lock()
	_, exists := s.pending[token]
	if exists {
		delete(s.pending, token)
		// Clean up the token from NATS KV
		if err := s.nats.DeleteToken(token); err != nil {
			log.Printf("failed to delete token: %v", err)
		}
	}
	s.pendingMu.Unlock()
}

func (s *Server) cleanupStale() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.pendingMu.Lock()
			now := time.Now()
			for token, pc := range s.pending {
				if now.Sub(pc.created) > connectionTimeout {
					delete(s.pending, token)
					pc.conn.Close()
					if err := s.nats.DeleteToken(token); err != nil {
						log.Printf("stale cleanup: failed to delete token: %v", err)
					}
				}
			}
			s.pendingMu.Unlock()
		case <-s.ctx.Done():
			return
		}
	}
}

package server

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log"
	"net"
	"sync"
	"time"

	"github.com/davedotdev/tcp-bridge/internal/config"
	bridgenats "github.com/davedotdev/tcp-bridge/internal/nats"
	"github.com/davedotdev/tcp-bridge/internal/relay"
	"github.com/google/uuid"
)

const (
	tokenSize       = 32
	connectionTimeout = 30 * time.Second
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
}

type pendingConn struct {
	conn     net.Conn
	connID   string
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
	_, err := s.nats.HandleRegister(s.cfg.DataPort, func(req bridgenats.RegisterRequest) bridgenats.RegisterResponse {
		if req.ClientID != s.cfg.ClientID {
			log.Printf("rejected registration from unknown client: %s", req.ClientID)
			return bridgenats.RegisterResponse{Status: "error", Error: "unknown client"}
		}
		log.Printf("client registered: %s", req.ClientID)
		return bridgenats.RegisterResponse{Status: "ok"}
	})
	if err != nil {
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
	connID := uuid.New().String()

	token, err := generateToken()
	if err != nil {
		log.Printf("failed to generate token: %v", err)
		conn.Close()
		return
	}

	// Store token in NATS KV
	if err := s.nats.StoreToken(token, connID); err != nil {
		log.Printf("failed to store token: %v", err)
		conn.Close()
		return
	}

	// Create pending connection entry
	pc := &pendingConn{
		conn:     conn,
		connID:   connID,
		created:  time.Now(),
		dataChan: make(chan net.Conn, 1),
	}

	s.pendingMu.Lock()
	s.pending[connID] = pc
	s.pendingMu.Unlock()

	// Signal client
	signal := bridgenats.ConnectSignal{
		Token:     token,
		ConnID:    connID,
		Timestamp: time.Now().Unix(),
	}
	if err := s.nats.PublishConnect(s.cfg.ClientID, signal); err != nil {
		log.Printf("failed to signal client: %v", err)
		s.removePending(connID)
		conn.Close()
		return
	}

	// Wait for data connection with timeout
	select {
	case dataConn := <-pc.dataChan:
		log.Printf("connection paired: %s", connID)
		relay.Relay(conn, dataConn)
	case <-time.After(connectionTimeout):
		log.Printf("connection timeout: %s", connID)
		s.removePending(connID)
		conn.Close()
	case <-s.ctx.Done():
		s.removePending(connID)
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
	connID, valid := s.nats.ValidateAndDeleteToken(token)
	if !valid {
		conn.Close()
		return
	}

	// Find pending connection
	s.pendingMu.Lock()
	pc, exists := s.pending[connID]
	if exists {
		delete(s.pending, connID)
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

func (s *Server) removePending(connID string) {
	s.pendingMu.Lock()
	delete(s.pending, connID)
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
			for id, pc := range s.pending {
				if now.Sub(pc.created) > connectionTimeout {
					delete(s.pending, id)
					pc.conn.Close()
				}
			}
			s.pendingMu.Unlock()
		case <-s.ctx.Done():
			return
		}
	}
}

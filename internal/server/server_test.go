package server

import (
	"testing"
	"time"

	"github.com/davedotdev/tcp-bridge/internal/config"
	bridgenats "github.com/davedotdev/tcp-bridge/internal/nats"
)

func newTestServer() *Server {
	return &Server{
		cfg: &config.ServerConfig{
			ClientIDs: []string{"laptop", "desktop"},
		},
	}
}

func TestRegisterUnknownClientRejected(t *testing.T) {
	s := newTestServer()

	resp := s.handleRegister(bridgenats.RegisterRequest{ClientID: "intruder"})
	if resp.Status != "error" {
		t.Fatalf("expected error status, got %q", resp.Status)
	}
	if s.activeClientID() != "" {
		t.Fatalf("unknown client must not become active")
	}
}

func TestRegisterFirstClientAccepted(t *testing.T) {
	s := newTestServer()

	resp := s.handleRegister(bridgenats.RegisterRequest{ClientID: "laptop"})
	if resp.Status != "ok" {
		t.Fatalf("expected ok, got %q (%s)", resp.Status, resp.Error)
	}
	if got := s.activeClientID(); got != "laptop" {
		t.Fatalf("expected active client laptop, got %q", got)
	}
}

func TestRegisterSecondClientBlockedAndReported(t *testing.T) {
	s := newTestServer()
	s.handleRegister(bridgenats.RegisterRequest{ClientID: "laptop"})

	resp := s.handleRegister(bridgenats.RegisterRequest{ClientID: "desktop"})
	if resp.Status != "busy" {
		t.Fatalf("expected busy status, got %q", resp.Status)
	}
	if resp.ConnectedClientID != "laptop" {
		t.Fatalf("expected connected_client_id laptop, got %q", resp.ConnectedClientID)
	}
	if resp.ConnectedSince == 0 {
		t.Fatalf("expected connected_since to be set")
	}
	if s.activeClientID() != "laptop" {
		t.Fatalf("active client must remain laptop")
	}
}

func TestSameClientCanReregister(t *testing.T) {
	s := newTestServer()
	s.handleRegister(bridgenats.RegisterRequest{ClientID: "laptop"})

	resp := s.handleRegister(bridgenats.RegisterRequest{ClientID: "laptop"})
	if resp.Status != "ok" {
		t.Fatalf("re-registration of active client should succeed, got %q", resp.Status)
	}
}

func TestDeregisterReleasesBridge(t *testing.T) {
	s := newTestServer()
	s.handleRegister(bridgenats.RegisterRequest{ClientID: "laptop"})

	s.handleDeregister(bridgenats.PresenceMsg{ClientID: "laptop"})
	if s.activeClientID() != "" {
		t.Fatalf("bridge should be released after deregister")
	}

	resp := s.handleRegister(bridgenats.RegisterRequest{ClientID: "desktop"})
	if resp.Status != "ok" {
		t.Fatalf("desktop should register after laptop deregisters, got %q", resp.Status)
	}
}

func TestDeregisterFromNonActiveClientIgnored(t *testing.T) {
	s := newTestServer()
	s.handleRegister(bridgenats.RegisterRequest{ClientID: "laptop"})

	s.handleDeregister(bridgenats.PresenceMsg{ClientID: "desktop"})
	if s.activeClientID() != "laptop" {
		t.Fatalf("deregister from non-active client must not release the bridge")
	}
}

func TestExpiredClientReleasesBridge(t *testing.T) {
	s := newTestServer()
	s.handleRegister(bridgenats.RegisterRequest{ClientID: "laptop"})

	// Simulate missed heartbeats
	s.activeMu.Lock()
	s.active.lastSeen = time.Now().Add(-clientExpiry - time.Second)
	s.activeMu.Unlock()

	resp := s.handleRegister(bridgenats.RegisterRequest{ClientID: "desktop"})
	if resp.Status != "ok" {
		t.Fatalf("desktop should register after laptop expires, got %q", resp.Status)
	}
	if got := s.activeClientID(); got != "desktop" {
		t.Fatalf("expected active client desktop, got %q", got)
	}
}

func TestHeartbeatKeepsClientAlive(t *testing.T) {
	s := newTestServer()
	s.handleRegister(bridgenats.RegisterRequest{ClientID: "laptop"})

	// Nearly expired, then a heartbeat arrives
	s.activeMu.Lock()
	s.active.lastSeen = time.Now().Add(-clientExpiry + time.Second)
	s.activeMu.Unlock()
	s.handleHeartbeat(bridgenats.PresenceMsg{ClientID: "laptop"})

	resp := s.handleRegister(bridgenats.RegisterRequest{ClientID: "desktop"})
	if resp.Status != "busy" {
		t.Fatalf("heartbeat should keep laptop active; desktop got %q", resp.Status)
	}
}

func TestHeartbeatAdoptsClientAfterServerRestart(t *testing.T) {
	// Fresh server (as after a restart) with no active client
	s := newTestServer()

	s.handleHeartbeat(bridgenats.PresenceMsg{ClientID: "laptop"})
	if got := s.activeClientID(); got != "laptop" {
		t.Fatalf("expected heartbeat to adopt laptop, got %q", got)
	}
}

func TestHeartbeatFromUnknownClientIgnored(t *testing.T) {
	s := newTestServer()

	s.handleHeartbeat(bridgenats.PresenceMsg{ClientID: "intruder"})
	if s.activeClientID() != "" {
		t.Fatalf("unknown client heartbeat must not be adopted")
	}
}

package client

import (
	"fmt"
	"log"
	"net"
	"time"

	"github.com/davedotdev/tcp-bridge/internal/config"
	bridgenats "github.com/davedotdev/tcp-bridge/internal/nats"
	"github.com/davedotdev/tcp-bridge/internal/relay"
)

// Connector handles a single connection request from the server.
type Connector struct {
	cfg    *config.ClientConfig
	signal bridgenats.ConnectSignal
}

// Connect establishes the data connection and relays traffic.
func (c *Connector) Connect() {
	// Connect to server data port
	serverAddr := fmt.Sprintf("%s:%d", c.cfg.ServerHost, c.cfg.DataPort)
	serverConn, err := net.DialTimeout("tcp", serverAddr, 10*time.Second)
	if err != nil {
		log.Printf("failed to connect to server data port: %v", err)
		return
	}

	// Send token
	_, err = serverConn.Write([]byte(c.signal.Token))
	if err != nil {
		log.Printf("failed to send token: %v", err)
		serverConn.Close()
		return
	}

	// Connect to local target
	localConn, err := net.DialTimeout("tcp", c.cfg.LocalTarget, 10*time.Second)
	if err != nil {
		log.Printf("failed to connect to local target %s: %v", c.cfg.LocalTarget, err)
		serverConn.Close()
		return
	}

	log.Printf("relaying connection to %s", c.cfg.LocalTarget)

	// Relay traffic
	relay.Relay(serverConn, localConn)

	log.Printf("connection closed to %s", c.cfg.LocalTarget)
}

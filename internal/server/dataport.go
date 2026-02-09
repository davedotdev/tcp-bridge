package server

import (
	"fmt"
	"log"
	"net"
)

// startDataPortListener starts listening on the data port.
func (s *Server) startDataPortListener() error {
	addr := fmt.Sprintf("%s:%d", s.cfg.ListenAddr, s.cfg.DataPort)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on data port %d: %w", s.cfg.DataPort, err)
	}
	defer ln.Close()

	log.Printf("data port listener started on %s", addr)

	for {
		select {
		case <-s.ctx.Done():
			return nil
		default:
		}

		conn, err := ln.Accept()
		if err != nil {
			if s.ctx.Err() != nil {
				return nil
			}
			log.Printf("accept error on data port: %v", err)
			continue
		}

		go s.handleDataConnection(conn)
	}
}

package server

import (
	"fmt"
	"log"
	"net"
)

// startPublicListener starts listening on the public port.
func (s *Server) startPublicListener() error {
	addr := fmt.Sprintf("%s:%d", s.cfg.ListenAddr, s.cfg.PublicPort)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on public port %d: %w", s.cfg.PublicPort, err)
	}
	defer ln.Close()

	log.Printf("public listener started on %s", addr)

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
			log.Printf("accept error on public port: %v", err)
			continue
		}

		go s.handlePublicConnection(conn)
	}
}

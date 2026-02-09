package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/davedotdev/tcp-bridge/internal/config"
	"github.com/davedotdev/tcp-bridge/internal/server"
)

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)

	cfg, err := config.LoadServerConfig()
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv, err := server.New(ctx, cfg)
	if err != nil {
		log.Fatalf("failed to create server: %v", err)
	}

	// Handle shutdown signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		log.Printf("received signal %v, shutting down...", sig)
		srv.Stop()
	}()

	if err := srv.Run(); err != nil {
		log.Fatalf("server error: %v", err)
	}

	log.Println("server stopped")
}

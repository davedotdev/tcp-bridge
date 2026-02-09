package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/davedotdev/tcp-bridge/internal/client"
	"github.com/davedotdev/tcp-bridge/internal/config"
)

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)

	cfg, err := config.LoadClientConfig()
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cli, err := client.New(ctx, cfg)
	if err != nil {
		log.Fatalf("failed to create client: %v", err)
	}

	// Set up reconnect handler
	cli.SetupReconnectHandler()

	// Handle shutdown signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		log.Printf("received signal %v, shutting down...", sig)
		cli.Stop()
	}()

	if err := cli.Run(); err != nil {
		log.Fatalf("client error: %v", err)
	}

	log.Println("client stopped")
}

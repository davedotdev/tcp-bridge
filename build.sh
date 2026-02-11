#!/bin/bash
set -e

# Build script for tcp-bridge
# Builds client and server binaries for multiple platforms

OUTPUT_DIR="bin"
mkdir -p "$OUTPUT_DIR"

echo "Building tcp-bridge binaries..."

# Build bridge-client
echo "Building bridge-client..."
GOOS=darwin GOARCH=amd64 go build -o "$OUTPUT_DIR/bridge-client-darwin-amd64" ./cmd/bridge-client
GOOS=darwin GOARCH=arm64 go build -o "$OUTPUT_DIR/bridge-client-darwin-arm64" ./cmd/bridge-client
GOOS=linux GOARCH=amd64 go build -o "$OUTPUT_DIR/bridge-client-linux-amd64" ./cmd/bridge-client
GOOS=linux GOARCH=arm64 go build -o "$OUTPUT_DIR/bridge-client-linux-arm64" ./cmd/bridge-client
GOOS=windows GOARCH=amd64 go build -o "$OUTPUT_DIR/bridge-client-windows-amd64.exe" ./cmd/bridge-client

# Build bridge-server
echo "Building bridge-server..."
GOOS=darwin GOARCH=amd64 go build -o "$OUTPUT_DIR/bridge-server-darwin-amd64" ./cmd/bridge-server
GOOS=darwin GOARCH=arm64 go build -o "$OUTPUT_DIR/bridge-server-darwin-arm64" ./cmd/bridge-server
GOOS=linux GOARCH=amd64 go build -o "$OUTPUT_DIR/bridge-server-linux-amd64" ./cmd/bridge-server
GOOS=windows GOARCH=amd64 go build -o "$OUTPUT_DIR/bridge-server-windows-amd64.exe" ./cmd/bridge-server

echo "Build complete. Binaries are in $OUTPUT_DIR/"
ls -la "$OUTPUT_DIR/"

# Bridge Server

The bridge server runs on a public-facing machine and accepts incoming TCP connections, pairing them with authenticated clients via NATS.

## How It Works

1. Server starts two TCP listeners: public port and data port
2. When a public connection arrives, server generates a one-time token and stores it in NATS KV
3. Server signals the registered client via NATS with the token
4. Client connects to the data port and presents the token
5. Server validates the token, pairs the connections, and relays traffic

## Configuration

Create `configs/bridge-server.env` from the example:

```bash
cp configs/bridge-server.env.example configs/bridge-server.env
```

Required variables:

| Variable | Description | Example |
|----------|-------------|---------|
| `NATS_URL` | NATS server URL | `nats://nats.example.com:4222` |
| `NATS_JWT` | NATS JWT credential | `eyJ0eXAi...` |
| `NATS_SEED` | NATS seed credential | `SUAM...` |
| `KV_BUCKET` | NATS KV bucket name (requires `--allow-individual-ttl`) | `bridge-sessions` |
| `LISTEN_ADDR` | Address to bind | `0.0.0.0` |
| `PUBLIC_PORT` | Port for public connections | `443` |
| `DATA_PORT` | Port for client connections | `9443` |
| `CLIENT_ID` | Expected client identifier | `my-laptop` |

## Build

```bash
# From project root - current platform
go build -o bin/bridge-server ./cmd/bridge-server

# Linux (amd64)
GOOS=linux GOARCH=amd64 go build -o bin/bridge-server-linux-amd64 ./cmd/bridge-server

# macOS (Intel)
GOOS=darwin GOARCH=amd64 go build -o bin/bridge-server-darwin-amd64 ./cmd/bridge-server

# macOS (Apple Silicon)
GOOS=darwin GOARCH=arm64 go build -o bin/bridge-server-darwin-arm64 ./cmd/bridge-server

# Windows (amd64)
GOOS=windows GOARCH=amd64 go build -o bin/bridge-server-windows-amd64.exe ./cmd/bridge-server
```

## Run

```bash
./bin/bridge-server
```

The server will:
- Load configuration from `configs/bridge-server.env`
- Connect to NATS and set up the registration handler
- Listen on the public port for incoming connections
- Listen on the data port for authenticated client connections
- Clean up stale pending connections automatically

## Systemd Service

A systemd service file is provided at `systemd/bridge-server.service` for running as a daemon.

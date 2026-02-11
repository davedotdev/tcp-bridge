# Bridge Client

The bridge client runs on a machine behind NAT or a firewall and forwards incoming connections to local services.

## How It Works

1. Client connects to NATS and registers with the server
2. When the server receives a public connection, it signals the client via NATS
3. Client establishes an outbound TCP connection to the server's data port
4. Client authenticates using a one-time token from NATS KV
5. Client opens a connection to the local target service
6. Traffic is relayed bidirectionally between the server and local service

## Configuration

Create `configs/bridge-client.env` from the example:

```bash
cp configs/bridge-client.env.example configs/bridge-client.env
```

Required variables:

| Variable | Description | Example |
|----------|-------------|---------|
| `NATS_URL` | NATS server URL | `nats://nats.example.com:4222` |
| `NATS_JWT` | NATS JWT credential | `eyJ0eXAi...` |
| `NATS_SEED` | NATS seed credential | `SUAM...` |
| `KV_BUCKET` | NATS KV bucket name (requires `--allow-individual-ttl`) | `bridge-sessions` |
| `CLIENT_ID` | Client identifier | `my-laptop` |
| `SERVER_HOST` | Bridge server hostname | `server.example.com` |
| `DATA_PORT` | Server's data port | `9443` |
| `LOCAL_TARGET` | Local service address | `localhost:443` |

## Build

```bash
# From project root - current platform
go build -o bin/bridge-client ./cmd/bridge-client

# Linux (amd64)
GOOS=linux GOARCH=amd64 go build -o bin/bridge-client-linux-amd64 ./cmd/bridge-client

# macOS (Intel)
GOOS=darwin GOARCH=amd64 go build -o bin/bridge-client-darwin-amd64 ./cmd/bridge-client

# macOS (Apple Silicon)
GOOS=darwin GOARCH=arm64 go build -o bin/bridge-client-darwin-arm64 ./cmd/bridge-client

# Windows (amd64)
GOOS=windows GOARCH=amd64 go build -o bin/bridge-client-windows-amd64.exe ./cmd/bridge-client
```

## Run

```bash
./bin/bridge-client
```

The client will:
- Load configuration from `configs/bridge-client.env`
- Connect to NATS and register with the server
- Wait for connection signals and handle them automatically
- Automatically re-register if the NATS connection is lost and restored

## Systemd Service

A systemd service file is provided at `systemd/bridge-client.service` for running as a daemon.

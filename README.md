# TCP Bridge

A TCP tunneling solution that enables secure access to services behind NAT or firewalls using NATS as the signaling layer.

## Overview

TCP Bridge consists of two components:

- **bridge-server**: Runs on a public server, accepts incoming TCP connections, and coordinates with the client via NATS
- **bridge-client**: Runs behind NAT/firewall, connects outbound to the server and forwards traffic to local services

The system uses NATS JetStream KV for secure token-based connection pairing, ensuring that only authenticated clients can establish tunnels.

## Architecture

```
┌─────────────┐         ┌─────────────┐         ┌─────────────┐
│   Public    │   TCP   │   Bridge    │  NATS   │   Bridge    │
│   Client    │────────▶│   Server    │◀───────▶│   Client    │
└─────────────┘         └─────────────┘         └─────────────┘
                              │                       │
                              │                       │ TCP
                              │                       ▼
                              │               ┌─────────────┐
                              │               │   Local     │
                              └───────────────│   Service   │
                                   Data       └─────────────┘
```

## Prerequisites

- Go 1.21+
- NATS server with JetStream enabled
- NATS credentials (JWT + Seed) for authentication

## Quick Start

### 1. Configure NATS

Create a KV bucket for session tokens:

```bash
nats kv add bridge-sessions
```

### 2. Configure Environment

Copy the example configuration files:

```bash
cp configs/bridge-server.env.example configs/bridge-server.env
cp configs/bridge-client.env.example configs/bridge-client.env
```

Edit both files with your NATS credentials and settings.

### 3. Build

```bash
# Build both binaries
go build -o bin/bridge-server ./cmd/bridge-server
go build -o bin/bridge-client ./cmd/bridge-client

# Cross-compile for Linux
GOOS=linux GOARCH=amd64 go build -o bin/bridge-server-linux-amd64 ./cmd/bridge-server
GOOS=linux GOARCH=arm64 go build -o bin/bridge-client-linux-arm64 ./cmd/bridge-client
```

### 4. Run

On the public server:

```bash
./bin/bridge-server
```

On the machine behind NAT:

```bash
./bin/bridge-client
```

## Configuration

### Server Configuration

| Variable | Description |
|----------|-------------|
| `NATS_URL` | NATS server URL |
| `NATS_JWT` | NATS JWT credential |
| `NATS_SEED` | NATS seed credential |
| `KV_BUCKET` | NATS KV bucket name for tokens |
| `LISTEN_ADDR` | Address to bind listeners (default: 0.0.0.0) |
| `PUBLIC_PORT` | Port for public connections |
| `DATA_PORT` | Port for client data connections |
| `CLIENT_ID` | Expected client identifier |

### Client Configuration

| Variable | Description |
|----------|-------------|
| `NATS_URL` | NATS server URL |
| `NATS_JWT` | NATS JWT credential |
| `NATS_SEED` | NATS seed credential |
| `KV_BUCKET` | NATS KV bucket name for tokens |
| `CLIENT_ID` | Client identifier (must match server's CLIENT_ID) |
| `SERVER_HOST` | Server hostname |
| `DATA_PORT` | Server's data port |
| `LOCAL_TARGET` | Local service to forward to (e.g., localhost:443) |

## Systemd Integration

Service files for systemd are provided in the `systemd/` directory.

## Project Structure

```
.
├── cmd/
│   ├── bridge-client/     # Client binary entry point
│   └── bridge-server/     # Server binary entry point
├── configs/               # Configuration files
├── internal/
│   ├── client/           # Client implementation
│   ├── config/           # Configuration loading
│   ├── nats/             # NATS client wrapper
│   ├── relay/            # TCP relay logic
│   └── server/           # Server implementation
├── systemd/              # Systemd service files
└── bin/                  # Compiled binaries (git-ignored)
```

## License

MIT

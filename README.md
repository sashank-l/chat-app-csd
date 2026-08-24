# Distributed Secure Chat with Go Load Balancer

This project integrates a secure real-time group chat backend with a custom Go load balancer and benchmark tooling.

The implementation follows the assignment charter goals:
1. Build a custom load balancer in Go for both HTTP and WebSocket traffic.
2. Deploy the secure chat backend across multiple backend instances.
3. Build a concurrent load generator and measure performance under load.
4. Compare single-backend and multi-backend behavior using throughput, error rate, and latency metrics.

## Project Scope

The system combines two parts:
1. Secure messaging backend (Python/Flask/WebSocket) with encryption, signatures, and tamper-evident storage.
2. Distributed traffic management and benchmarking (Go load balancer + load generators).

## Core Features

### Secure Chat Backend
1. Encryption at rest using Fernet (AES-CBC with authentication).
2. ECDSA P-256 signatures for message authenticity.
3. SHA-256 hash chain for tamper evidence in persisted history.
4. Real-time WebSocket messaging with user presence and typing indicators.

### Go Load Balancer
1. Proxies regular HTTP requests to backend instances.
2. Handles WebSocket upgrade requests and full-duplex streaming via TCP hijacking.
3. Supports least-connections and round-robin routing modes.
4. Runs backend health checks using the /health endpoint.
5. Exposes /lb-stats for live backend connection and routing statistics.

### Benchmarking
1. Go load generator: high-concurrency RFC-6455 client implementation.
2. Python benchmark scripts for automated comparison runs.
3. Reports throughput, sent/received totals, errors, and percentile latencies.

## Repository Structure

```text
app.py                         Python secure chat backend
crypto_utils.py                Message encryption/decryption helpers
signatures.py                  ECDSA signing and verification
integrity.py                   Hash chain generation and verification
db.py                          SQLite persistence and query helpers

load_balancer/main.go          Go HTTP + WebSocket load balancer
load_generator/main.go         Go benchmark load generator

benchmark/load_generator.py    Python async load generator
benchmark/run_comparison.py    Python benchmark summary runner

static/index.html              Chat UI
static/app.js                  WebSocket client logic
static/style.css               UI styles

cipher-test.py                 DB ciphertext tampering test
key-tampering.py               DB public-key tampering test
```

## Environment and Ports

Default backend and load balancer ports in this codebase:
1. Load balancer entry: 4209
2. Backend 1: 4210
3. Backend 2: 4211
4. Backend 3: 4212

Typical endpoints:
1. HTTP via load balancer: http://HOST:4209/
2. WebSocket via load balancer: ws://HOST:4209/ws
3. Load balancer stats: http://HOST:4209/lb-stats
4. Backend health check: http://HOST:4210/health (and similarly 4211/4212)

## Setup

### 1. Python dependencies

```bash
pip install -r requirements.txt
```

### 2. Backend app dependencies for benchmark scripts

If you plan to run Python benchmark scripts in benchmark/, install websockets:

```bash
pip install websockets
```

### 3. Go toolchain

Install Go (1.21+ recommended) to build and run:
1. load_balancer/main.go
2. load_generator/main.go

## Running the System

### Start backend instances

Run one backend process per target port (on separate hosts/containers in distributed mode).

Example for one local backend:

```bash
python app.py --port 4210
```

Repeat with adjusted ports/hosts for 4211 and 4212 as needed.

### Start the load balancer

```bash
go run load_balancer/main.go \
  -port 4209 \
  -mode all \
  -algo leastconn \
  -backends "http://172.17.0.11:4210,http://172.17.0.12:4211,http://172.17.0.13:4212"
```

Single-backend comparison mode:

```bash
go run load_balancer/main.go -port 4209 -mode single -algo leastconn
```

### Open the app

Open the load balancer URL in your browser:

```text
http://HOST:4209/
```

The frontend automatically uses the same host for WebSocket upgrades at /ws.

## Running Benchmarks

### Go load generator

```bash
go run load_generator/main.go -url ws://HOST:4209/ws -clients 50 -duration 20 -interval 200
```

### Python benchmark summary

```bash
python benchmark/run_comparison.py --url ws://HOST:4209/ws --clients 50 --duration 20 --interval 0.15
```

### Python async load generator

```bash
python benchmark/load_generator.py --url ws://HOST:4209/ws --clients 50 --duration 20 --interval 0.2
```

## Security Validation Scripts

Use included scripts to demonstrate tamper detection behavior:

```bash
python cipher-test.py
python key-tampering.py
```

Expected outcomes:
1. Ciphertext tampering results in unreadable content and tamper flags.
2. Public key tampering causes signature verification failure.

## Charter Alignment Summary

1. Custom Go load balancer is implemented with WebSocket-aware proxying.
2. Secure chat backend is integrated behind the balancer across multiple nodes.
3. Concurrent load generation is implemented in both Go and Python.
4. Performance comparison workflow (single vs multi backend) is included and reproducible.

## Notes

1. Keep secret.key private and never commit secrets.
2. For distributed runs, use your container/internal IP mapping for backend URLs.
3. For local testing, you can run all components on localhost with different ports.
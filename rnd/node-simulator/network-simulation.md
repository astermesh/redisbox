# Network Simulation Approaches

## Tools Overview

### Toxiproxy (Shopify)

**Type:** External TCP proxy (Go binary + HTTP control API)

**How it works:**
- TCP proxy sits between application and service
- Configure app to connect through Toxiproxy instead of directly to Redis
- Inject faults ("toxics") via HTTP API at runtime

**Capabilities:**
- Latency injection
- Packet loss
- Bandwidth limitation
- Connection drops
- Timeout simulation (connection stays open, data delayed indefinitely)

**Pros:** Language-agnostic, robust, industry standard, L3/L4 level
**Cons:** Requires external process, not in-process

### toxy (Node.js)

**Type:** In-process HTTP proxy middleware

**How it works:**
- Built on top of `rocky` (HTTP proxy)
- Pluggable as connect/express middleware
- Operates primarily at L7, can simulate L3 conditions

**Features:**
- **Poisons** — inject latency, bandwidth limits, errors, jitter
- **Rules** — conditional filtering (headers, query params, method, body)
- Fluent API + HTTP API
- Global or per-route configuration

**Pros:** In-process Node.js, programmatic control, middleware-friendly
**Cons:** HTTP-focused (L7), not ideal for raw TCP simulation

### simulate-network-conditions (npm)

**Type:** In-process Node.js stream transform

**How it works:**
- Wraps Node.js streams with network condition simulation
- Represents one-way network traffic (use two streams + duplexer3 for bidirectional)

**Features:**
- Constant latency, jitter-based latency
- Percentage-based packet loss
- Index-based loss patterns
- Time-based loss patterns

**Pros:** Pure in-process, stream-level, no external dependencies
**Cons:** Low-level, requires manual stream wiring

## Comparison

| Tool | Type | Level | In-Process | TCP Support |
|------|------|-------|------------|-------------|
| **Toxiproxy** | External proxy | L3/L4 | No | Yes |
| **toxy** | HTTP middleware | L7 | Yes | No (HTTP) |
| **simulate-network-conditions** | Stream transform | Stream | Yes | Indirectly |
| **tc (traffic control)** | Linux kernel | L3 | No | Yes |
| **Comcast** | CLI wrapper for tc/pfctl | L3 | No | Yes |

## In-Process Approaches for Node.js

For RedisBox, we need in-process network simulation without external tools. Possible approaches:

### 1. Stream Transform Layer

Wrap `net.Socket` streams with transform streams that introduce:
- Configurable latency (delay before forwarding data)
- Packet loss (randomly drop chunks)
- Bandwidth throttling (rate-limit data flow)
- Connection interruption (close socket after condition)

### 2. Custom net.Server Wrapper

Create a proxy layer around Node.js `net.Server` that intercepts socket operations:
```
Client → SimulatedSocket → Network Conditions → ActualSocket → Redis Engine
```

### 3. Event Loop Manipulation

Use `setTimeout` / `setImmediate` to delay data delivery, simulating latency at the application level.

### 4. Dual-Mode Architecture

- **Direct mode**: Client talks directly to in-memory engine (no network, maximum speed)
- **Server mode**: Real TCP server with optional network simulation layer

## Implications for RedisBox Node Simulator

For the node simulator, the recommended approach:
1. **Primary mode**: In-process direct connection (no actual TCP) for fastest testing
2. **TCP mode**: Real `net.Server` for testing with real Redis clients
3. **Network simulation**: Optional stream transform layer between client socket and engine
4. Use `simulate-network-conditions` patterns as reference for implementing in-process simulation
5. No external tool dependency — everything runs in a single Node.js process

---

[← Back to Node Simulator Research](README.md)

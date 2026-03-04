# RedisBox

In-memory Redis emulator for browser and Node.js.

## What It Does

Lightweight Redis emulator running entirely in-memory. Speaks the real Redis wire protocol (RESP2) so any standard Redis client can connect. Supports dual-mode architecture: RESP proxy over embedded Redis binary (Node.js, 100% coverage) and pure JS engine (browser, incremental coverage).

## Install

```bash
npm install redisbox
```

## Quick Start

```typescript
import { createRedisBox } from 'redisbox';

const box = await createRedisBox();
// Use with any Redis client via Custom Connector or TCP
```

## License

MIT

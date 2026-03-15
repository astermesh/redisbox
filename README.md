# RedisBox

In-memory Redis emulator for browser and Node.js.

## What It Does

Lightweight Redis emulator running entirely in-memory. Full reimplementation of Redis in TypeScript. Speaks the real Redis wire protocol (RESP2) so any standard Redis client can connect. Runs natively on Node.js and in browser via NodeBox.

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

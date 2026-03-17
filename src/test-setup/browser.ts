/**
 * Browser test environment setup.
 *
 * Polyfills Node.js globals that are provided by NodeBox in production
 * but need to be shimmed for vitest browser (Chromium) test runs.
 */
import { Buffer } from 'buffer';
(globalThis as typeof globalThis & { Buffer: typeof Buffer }).Buffer = Buffer;

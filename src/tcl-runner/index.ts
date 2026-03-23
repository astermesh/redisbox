export { parseTclTestOutput } from './tcl-output-parser.ts';
export type { TclTestResult, TclTestSummary } from './tcl-output-parser.ts';
export { startRedisBoxServer } from './redisbox-server.ts';
export type {
  RedisBoxServer,
  RedisBoxServerOptions,
} from './redisbox-server.ts';
export {
  runTclTests,
  discoverTests,
  isTestSuiteAvailable,
  isTclAvailable,
} from './tcl-runner.ts';
export type { TclRunnerOptions, TclRunResult } from './tcl-runner.ts';
export { formatReport, categorizeFailure, passRate } from './report.ts';
export type { FailureCategory, CategorizedFailure } from './report.ts';
export { checkBaseline, formatBaselineResult } from './parity-baseline.ts';
export type { ParityBaseline, BaselineResult } from './parity-baseline.ts';

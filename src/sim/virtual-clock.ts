/**
 * Controllable virtual clock for simulation time control.
 *
 * Wraps a base clock (defaults to Date.now) and supports:
 * - advancing time by a given offset
 * - freezing time at a specific point
 * - setting absolute time
 * - unfreezing to resume from the frozen point
 */
export class VirtualClock {
  private offset = 0;
  private frozen = false;
  private frozenAt = 0;
  private readonly baseClock: () => number;

  constructor(baseClock: () => number = () => Date.now()) {
    this.baseClock = baseClock;
  }

  now(): number {
    if (this.frozen) return this.frozenAt;
    return this.baseClock() + this.offset;
  }

  advanceTime(ms: number): void {
    if (ms < 0) {
      throw new Error('Cannot advance time by a negative amount');
    }
    if (this.frozen) {
      this.frozenAt += ms;
    } else {
      this.offset += ms;
    }
  }

  freezeTime(): void {
    if (!this.frozen) {
      this.frozenAt = this.now();
      this.frozen = true;
    }
  }

  setTime(timestamp: number): void {
    if (this.frozen) {
      this.frozenAt = timestamp;
    } else {
      this.offset = timestamp - this.baseClock();
    }
  }

  unfreezeTime(): void {
    if (this.frozen) {
      this.offset = this.frozenAt - this.baseClock();
      this.frozen = false;
    }
  }

  get isFrozen(): boolean {
    return this.frozen;
  }
}

const SKIPLIST_MAXLEVEL = 32;
const SKIPLIST_P = 0.25;

export interface SkipListLevel {
  forward: SkipListNode | null;
  span: number;
}

export class SkipListNode {
  element: string;
  score: number;
  backward: SkipListNode | null;
  levels: SkipListLevel[];

  constructor(level: number, score: number, element: string) {
    this.element = element;
    this.score = score;
    this.backward = null;
    this.levels = new Array<SkipListLevel>(level);
    for (let i = 0; i < level; i++) {
      this.levels[i] = { forward: null, span: 0 };
    }
  }

  lvl(i: number): SkipListLevel {
    return this.levels[i] as SkipListLevel;
  }
}

function lt(
  aScore: number,
  aElem: string,
  bScore: number,
  bElem: string
): boolean {
  return aScore < bScore || (aScore === bScore && aElem < bElem);
}

function lte(
  aScore: number,
  aElem: string,
  bScore: number,
  bElem: string
): boolean {
  return aScore < bScore || (aScore === bScore && aElem <= bElem);
}

export class SkipList {
  head: SkipListNode;
  tail: SkipListNode | null;
  length: number;
  level: number;
  private readonly rng: () => number;

  constructor(rng: () => number) {
    this.head = new SkipListNode(SKIPLIST_MAXLEVEL, 0, '');
    this.tail = null;
    this.length = 0;
    this.level = 1;
    this.rng = rng;
  }

  private randomLevel(): number {
    let lvl = 1;
    while (lvl < SKIPLIST_MAXLEVEL && this.rng() < SKIPLIST_P) {
      lvl++;
    }
    return lvl;
  }

  insert(score: number, element: string): SkipListNode {
    const update = new Array<SkipListNode>(SKIPLIST_MAXLEVEL);
    const rank = new Array<number>(SKIPLIST_MAXLEVEL).fill(0);

    let x = this.head;
    for (let i = this.level - 1; i >= 0; i--) {
      rank[i] = i === this.level - 1 ? 0 : (rank[i + 1] as number);
      let fwd = x.lvl(i).forward;
      while (fwd && lt(fwd.score, fwd.element, score, element)) {
        rank[i] = (rank[i] as number) + x.lvl(i).span;
        x = fwd;
        fwd = x.lvl(i).forward;
      }
      update[i] = x;
    }

    const lvl = this.randomLevel();
    if (lvl > this.level) {
      for (let i = this.level; i < lvl; i++) {
        rank[i] = 0;
        update[i] = this.head;
        this.head.lvl(i).span = this.length;
      }
      this.level = lvl;
    }

    const node = new SkipListNode(lvl, score, element);

    for (let i = 0; i < lvl; i++) {
      const ui = update[i] as SkipListNode;
      node.lvl(i).forward = ui.lvl(i).forward;
      ui.lvl(i).forward = node;

      node.lvl(i).span =
        ui.lvl(i).span - ((rank[0] as number) - (rank[i] as number));
      ui.lvl(i).span = (rank[0] as number) - (rank[i] as number) + 1;
    }

    for (let i = lvl; i < this.level; i++) {
      (update[i] as SkipListNode).lvl(i).span++;
    }

    const u0 = update[0] as SkipListNode;
    node.backward = u0 === this.head ? null : u0;
    const next0 = node.lvl(0).forward;
    if (next0) {
      next0.backward = node;
    } else {
      this.tail = node;
    }

    this.length++;
    return node;
  }

  delete(score: number, element: string): boolean {
    const update = new Array<SkipListNode>(SKIPLIST_MAXLEVEL);
    let x = this.head;

    for (let i = this.level - 1; i >= 0; i--) {
      let fwd = x.lvl(i).forward;
      while (fwd && lt(fwd.score, fwd.element, score, element)) {
        x = fwd;
        fwd = x.lvl(i).forward;
      }
      update[i] = x;
    }

    const target = (update[0] as SkipListNode).lvl(0).forward;
    if (!target || target.score !== score || target.element !== element) {
      return false;
    }

    this.deleteNode(target, update as SkipListNode[]);
    return true;
  }

  private deleteNode(node: SkipListNode, update: SkipListNode[]): void {
    for (let i = 0; i < this.level; i++) {
      const ui = update[i] as SkipListNode;
      if (ui.lvl(i).forward === node) {
        ui.lvl(i).span += node.lvl(i).span - 1;
        ui.lvl(i).forward = node.lvl(i).forward;
      } else {
        ui.lvl(i).span--;
      }
    }

    const next0 = node.lvl(0).forward;
    if (next0) {
      next0.backward = node.backward;
    } else {
      this.tail = node.backward;
    }

    while (this.level > 1 && !this.head.lvl(this.level - 1).forward) {
      this.level--;
    }

    this.length--;
  }

  find(score: number, element: string): SkipListNode | null {
    let x = this.head;
    for (let i = this.level - 1; i >= 0; i--) {
      let fwd = x.lvl(i).forward;
      while (fwd && lt(fwd.score, fwd.element, score, element)) {
        x = fwd;
        fwd = x.lvl(i).forward;
      }
    }

    const candidate = x.lvl(0).forward;
    if (
      candidate &&
      candidate.score === score &&
      candidate.element === element
    ) {
      return candidate;
    }
    return null;
  }

  /**
   * Get 0-based rank of an element. Returns -1 if not found.
   */
  getRank(score: number, element: string): number {
    let rank = 0;
    let x = this.head;

    for (let i = this.level - 1; i >= 0; i--) {
      let fwd = x.lvl(i).forward;
      while (fwd && lte(fwd.score, fwd.element, score, element)) {
        rank += x.lvl(i).span;
        x = fwd;
        fwd = x.lvl(i).forward;
      }
    }

    if (x !== this.head && x.score === score && x.element === element) {
      return rank - 1; // Convert 1-based to 0-based
    }
    return -1;
  }

  /**
   * Get element by 1-based rank. Returns null if out of range.
   */
  getElementByRank(rank: number): SkipListNode | null {
    if (rank < 1 || rank > this.length) return null;

    let traversed = 0;
    let x = this.head;

    for (let i = this.level - 1; i >= 0; i--) {
      let fwd = x.lvl(i).forward;
      while (fwd && traversed + x.lvl(i).span <= rank) {
        traversed += x.lvl(i).span;
        x = fwd;
        fwd = x.lvl(i).forward;
      }
      if (traversed === rank) return x;
    }
    return null;
  }

  /**
   * Check if any element falls within [min, max] score range.
   */
  isInRange(
    min: number,
    max: number,
    minExclusive = false,
    maxExclusive = false
  ): boolean {
    if (min > max) return false;
    if (this.length === 0) return false;

    const tail = this.tail;
    if (!tail) return false;
    if (minExclusive ? tail.score <= min : tail.score < min) return false;

    const first = this.head.lvl(0).forward;
    if (!first) return false;
    if (maxExclusive ? first.score >= max : first.score > max) return false;

    return true;
  }

  /**
   * Get the first node with score >= min (or > min if minExclusive).
   */
  getFirstInRange(
    min: number,
    max: number,
    minExclusive = false,
    maxExclusive = false
  ): SkipListNode | null {
    if (!this.isInRange(min, max, minExclusive, maxExclusive)) return null;

    let x = this.head;
    for (let i = this.level - 1; i >= 0; i--) {
      let fwd = x.lvl(i).forward;
      while (fwd && (minExclusive ? fwd.score <= min : fwd.score < min)) {
        x = fwd;
        fwd = x.lvl(i).forward;
      }
    }

    const node = x.lvl(0).forward;
    if (!node) return null;

    if (maxExclusive ? node.score >= max : node.score > max) return null;
    return node;
  }

  /**
   * Get the last node with score <= max (or < max if maxExclusive).
   */
  getLastInRange(
    min: number,
    max: number,
    minExclusive = false,
    maxExclusive = false
  ): SkipListNode | null {
    if (!this.isInRange(min, max, minExclusive, maxExclusive)) return null;

    let x = this.head;
    for (let i = this.level - 1; i >= 0; i--) {
      let fwd = x.lvl(i).forward;
      while (fwd && (maxExclusive ? fwd.score < max : fwd.score <= max)) {
        x = fwd;
        fwd = x.lvl(i).forward;
      }
    }

    if (minExclusive ? x.score <= min : x.score < min) return null;
    if (x === this.head) return null;
    return x;
  }

  /**
   * Count elements in score range.
   */
  countInRange(
    min: number,
    max: number,
    minExclusive = false,
    maxExclusive = false
  ): number {
    const first = this.getFirstInRange(min, max, minExclusive, maxExclusive);
    if (!first) return 0;

    let count = 0;
    let node: SkipListNode | null = first;
    while (node) {
      if (maxExclusive ? node.score >= max : node.score > max) break;
      count++;
      node = node.lvl(0).forward;
    }

    return count;
  }

  /**
   * Delete all elements in score range. Returns deleted element names.
   */
  deleteRangeByScore(
    min: number,
    max: number,
    minExclusive = false,
    maxExclusive = false
  ): string[] {
    const update = new Array<SkipListNode>(SKIPLIST_MAXLEVEL);
    let x = this.head;

    for (let i = this.level - 1; i >= 0; i--) {
      let fwd = x.lvl(i).forward;
      while (fwd && (minExclusive ? fwd.score <= min : fwd.score < min)) {
        x = fwd;
        fwd = x.lvl(i).forward;
      }
      update[i] = x;
    }

    const removed: string[] = [];
    let node = (update[0] as SkipListNode).lvl(0).forward;

    while (node) {
      if (maxExclusive ? node.score >= max : node.score > max) break;
      const next = node.lvl(0).forward;
      removed.push(node.element);
      this.deleteNode(node, update as SkipListNode[]);
      node = next;
    }

    return removed;
  }

  /**
   * Delete elements by 0-based rank range [start, end] inclusive.
   */
  deleteRangeByRank(start: number, end: number): string[] {
    const update = new Array<SkipListNode>(SKIPLIST_MAXLEVEL);
    let traversed = 0;
    let x = this.head;

    for (let i = this.level - 1; i >= 0; i--) {
      let fwd = x.lvl(i).forward;
      while (fwd && traversed + x.lvl(i).span <= start) {
        traversed += x.lvl(i).span;
        x = fwd;
        fwd = x.lvl(i).forward;
      }
      update[i] = x;
    }

    const removed: string[] = [];
    let node = (update[0] as SkipListNode).lvl(0).forward;
    let rank = traversed;

    while (node && rank <= end) {
      const next = node.lvl(0).forward;
      removed.push(node.element);
      this.deleteNode(node, update as SkipListNode[]);
      node = next;
      rank++;
    }

    return removed;
  }
}

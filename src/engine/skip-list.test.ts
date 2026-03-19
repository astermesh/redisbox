import { describe, it, expect } from 'vitest';
import { SkipList } from './skip-list.ts';

function createList(rng?: () => number): SkipList {
  return new SkipList(rng ?? (() => 0.5));
}

describe('SkipList', () => {
  describe('insert and find', () => {
    it('starts empty', () => {
      const sl = createList();
      expect(sl.length).toBe(0);
      expect(sl.find(1, 'a')).toBe(null);
    });

    it('inserts a single element', () => {
      const sl = createList();
      sl.insert(1.0, 'a');
      expect(sl.length).toBe(1);
      const node = sl.find(1.0, 'a');
      expect(node).not.toBeNull();
      expect(node?.score).toBe(1.0);
      expect(node?.element).toBe('a');
    });

    it('inserts multiple elements with different scores', () => {
      const sl = createList();
      sl.insert(3, 'c');
      sl.insert(1, 'a');
      sl.insert(2, 'b');
      expect(sl.length).toBe(3);
      expect(sl.find(1, 'a')).not.toBeNull();
      expect(sl.find(2, 'b')).not.toBeNull();
      expect(sl.find(3, 'c')).not.toBeNull();
    });

    it('inserts elements with same score, ordered lexicographically', () => {
      const sl = createList();
      sl.insert(1, 'banana');
      sl.insert(1, 'apple');
      sl.insert(1, 'cherry');
      expect(sl.length).toBe(3);

      // Verify order: apple < banana < cherry (all score=1)
      const elements = collectForward(sl);
      expect(elements).toEqual([
        { score: 1, element: 'apple' },
        { score: 1, element: 'banana' },
        { score: 1, element: 'cherry' },
      ]);
    });

    it('handles -Infinity and +Infinity scores', () => {
      const sl = createList();
      sl.insert(-Infinity, 'min');
      sl.insert(Infinity, 'max');
      sl.insert(0, 'mid');
      expect(sl.length).toBe(3);

      const elements = collectForward(sl);
      expect(elements).toEqual([
        { score: -Infinity, element: 'min' },
        { score: 0, element: 'mid' },
        { score: Infinity, element: 'max' },
      ]);
    });

    it('returns null for non-existing element', () => {
      const sl = createList();
      sl.insert(1, 'a');
      expect(sl.find(2, 'a')).toBeNull();
      expect(sl.find(1, 'b')).toBeNull();
    });
  });

  describe('delete', () => {
    it('deletes a single element', () => {
      const sl = createList();
      sl.insert(1, 'a');
      expect(sl.delete(1, 'a')).toBe(true);
      expect(sl.length).toBe(0);
      expect(sl.find(1, 'a')).toBeNull();
    });

    it('returns false when deleting non-existing element', () => {
      const sl = createList();
      sl.insert(1, 'a');
      expect(sl.delete(2, 'a')).toBe(false);
      expect(sl.delete(1, 'b')).toBe(false);
      expect(sl.length).toBe(1);
    });

    it('deletes from middle of list', () => {
      const sl = createList();
      sl.insert(1, 'a');
      sl.insert(2, 'b');
      sl.insert(3, 'c');
      expect(sl.delete(2, 'b')).toBe(true);
      expect(sl.length).toBe(2);
      expect(sl.find(2, 'b')).toBeNull();

      const elements = collectForward(sl);
      expect(elements).toEqual([
        { score: 1, element: 'a' },
        { score: 3, element: 'c' },
      ]);
    });

    it('deletes first element', () => {
      const sl = createList();
      sl.insert(1, 'a');
      sl.insert(2, 'b');
      expect(sl.delete(1, 'a')).toBe(true);
      expect(sl.length).toBe(1);

      const elements = collectForward(sl);
      expect(elements).toEqual([{ score: 2, element: 'b' }]);
    });

    it('deletes last element (updates tail)', () => {
      const sl = createList();
      sl.insert(1, 'a');
      sl.insert(2, 'b');
      expect(sl.delete(2, 'b')).toBe(true);
      expect(sl.length).toBe(1);
      expect(sl.tail?.element).toBe('a');
    });

    it('deletes with same-score elements', () => {
      const sl = createList();
      sl.insert(1, 'a');
      sl.insert(1, 'b');
      sl.insert(1, 'c');
      expect(sl.delete(1, 'b')).toBe(true);
      expect(sl.length).toBe(2);

      const elements = collectForward(sl);
      expect(elements).toEqual([
        { score: 1, element: 'a' },
        { score: 1, element: 'c' },
      ]);
    });
  });

  describe('update score', () => {
    it('updates score and reorders', () => {
      const sl = createList();
      sl.insert(1, 'a');
      sl.insert(2, 'b');
      sl.insert(3, 'c');

      // Move 'a' from score 1 to score 4
      sl.delete(1, 'a');
      sl.insert(4, 'a');

      const elements = collectForward(sl);
      expect(elements).toEqual([
        { score: 2, element: 'b' },
        { score: 3, element: 'c' },
        { score: 4, element: 'a' },
      ]);
    });
  });

  describe('rank (0-based)', () => {
    it('returns correct rank for each element', () => {
      const sl = createList();
      sl.insert(10, 'a');
      sl.insert(20, 'b');
      sl.insert(30, 'c');
      expect(sl.getRank(10, 'a')).toBe(0);
      expect(sl.getRank(20, 'b')).toBe(1);
      expect(sl.getRank(30, 'c')).toBe(2);
    });

    it('returns -1 for non-existing element', () => {
      const sl = createList();
      sl.insert(10, 'a');
      expect(sl.getRank(20, 'b')).toBe(-1);
    });

    it('ranks correctly with same-score elements', () => {
      const sl = createList();
      sl.insert(1, 'b');
      sl.insert(1, 'a');
      sl.insert(1, 'c');
      // Order: a, b, c (lexicographic within same score)
      expect(sl.getRank(1, 'a')).toBe(0);
      expect(sl.getRank(1, 'b')).toBe(1);
      expect(sl.getRank(1, 'c')).toBe(2);
    });

    it('spans are correct after inserts and deletes', () => {
      const sl = createList();
      for (let i = 0; i < 10; i++) {
        sl.insert(i, `e${String(i).padStart(2, '0')}`);
      }
      for (let i = 0; i < 10; i++) {
        expect(sl.getRank(i, `e${String(i).padStart(2, '0')}`)).toBe(i);
      }

      // Delete element at rank 5
      sl.delete(5, 'e05');
      // Elements after deletion should shift ranks
      expect(sl.getRank(6, 'e06')).toBe(5);
      expect(sl.getRank(9, 'e09')).toBe(8);
    });
  });

  describe('getElementByRank', () => {
    it('returns element at given rank', () => {
      const sl = createList();
      sl.insert(10, 'a');
      sl.insert(20, 'b');
      sl.insert(30, 'c');

      const n0 = sl.getElementByRank(1); // 1-based rank
      expect(n0?.element).toBe('a');

      const n1 = sl.getElementByRank(2);
      expect(n1?.element).toBe('b');

      const n2 = sl.getElementByRank(3);
      expect(n2?.element).toBe('c');
    });

    it('returns null for out-of-range rank', () => {
      const sl = createList();
      sl.insert(10, 'a');
      expect(sl.getElementByRank(0)).toBeNull();
      expect(sl.getElementByRank(2)).toBeNull();
    });
  });

  describe('backward pointers', () => {
    it('supports reverse iteration', () => {
      const sl = createList();
      sl.insert(1, 'a');
      sl.insert(2, 'b');
      sl.insert(3, 'c');

      const elements = collectBackward(sl);
      expect(elements).toEqual([
        { score: 3, element: 'c' },
        { score: 2, element: 'b' },
        { score: 1, element: 'a' },
      ]);
    });

    it('backward pointers correct after delete', () => {
      const sl = createList();
      sl.insert(1, 'a');
      sl.insert(2, 'b');
      sl.insert(3, 'c');
      sl.delete(2, 'b');

      const elements = collectBackward(sl);
      expect(elements).toEqual([
        { score: 3, element: 'c' },
        { score: 1, element: 'a' },
      ]);
    });
  });

  describe('first and last nodes', () => {
    it('returns first and last nodes', () => {
      const sl = createList();
      sl.insert(5, 'b');
      sl.insert(1, 'a');
      sl.insert(9, 'c');

      expect(sl.head.lvl(0).forward?.element).toBe('a');
      expect(sl.tail?.element).toBe('c');
    });

    it('tail is null for empty list', () => {
      const sl = createList();
      expect(sl.tail).toBeNull();
    });
  });

  describe('comparison semantics', () => {
    it('uses byte-order comparison for elements with same score', () => {
      const sl = createList();
      // In byte order: 'A' (65) < 'a' (97) < 'b' (98)
      sl.insert(1, 'b');
      sl.insert(1, 'A');
      sl.insert(1, 'a');

      const elements = collectForward(sl);
      expect(elements.map((e) => e.element)).toEqual(['A', 'a', 'b']);
    });

    it('score comparison takes priority over element comparison', () => {
      const sl = createList();
      sl.insert(2, 'a');
      sl.insert(1, 'z');

      const elements = collectForward(sl);
      expect(elements).toEqual([
        { score: 1, element: 'z' },
        { score: 2, element: 'a' },
      ]);
    });
  });

  describe('randomized level generation', () => {
    it('generates level 1 when rng always returns >= 0.25', () => {
      const sl = new SkipList(() => 0.5);
      sl.insert(1, 'a');
      // Should only have level 1 (rng=0.5 >= 0.25, so no promotion)
      expect(sl.level).toBe(1);
    });

    it('generates higher levels when rng returns < 0.25', () => {
      let callCount = 0;
      const rng = (): number => {
        callCount++;
        // Return < 0.25 for first 3 calls during level generation, then >= 0.25
        return callCount <= 3 ? 0.1 : 0.5;
      };
      const sl = new SkipList(rng);
      sl.insert(1, 'a');
      // Should have 4 levels (base + 3 promotions)
      expect(sl.level).toBe(4);
    });

    it('caps level at 32', () => {
      // Always returns < 0.25 to maximize level
      const sl = new SkipList(() => 0.0);
      sl.insert(1, 'a');
      expect(sl.level).toBe(32);
    });
  });

  describe('large dataset', () => {
    it('maintains correct order and ranks with 1000 elements', () => {
      let seed = 42;
      const rng = (): number => {
        seed = (seed * 1103515245 + 12345) & 0x7fffffff;
        return seed / 0x7fffffff;
      };

      const sl = new SkipList(rng);
      const entries: { score: number; element: string }[] = [];

      for (let i = 0; i < 1000; i++) {
        const score = Math.floor(rng() * 10000);
        const element = `e${String(i).padStart(4, '0')}`;
        sl.insert(score, element);
        entries.push({ score, element });
      }

      expect(sl.length).toBe(1000);

      // Sort entries the same way the skip list should
      entries.sort((a, b) => {
        if (a.score !== b.score) return a.score - b.score;
        return a.element < b.element ? -1 : a.element > b.element ? 1 : 0;
      });

      // Verify forward order matches sorted entries
      const forward = collectForward(sl);
      expect(forward).toEqual(entries);

      // Verify ranks
      for (let i = 0; i < entries.length; i++) {
        const e = entries[i] as { score: number; element: string };
        expect(sl.getRank(e.score, e.element)).toBe(i);
      }

      // Delete first 100 elements and verify
      for (let i = 0; i < 100; i++) {
        const e = entries[i] as { score: number; element: string };
        expect(sl.delete(e.score, e.element)).toBe(true);
      }
      expect(sl.length).toBe(900);

      const remaining = entries.slice(100);
      const afterDelete = collectForward(sl);
      expect(afterDelete).toEqual(remaining);
    });
  });

  describe('getFirstInRange / getLastInRange', () => {
    it('returns first node in score range', () => {
      const sl = createList();
      sl.insert(1, 'a');
      sl.insert(2, 'b');
      sl.insert(3, 'c');
      sl.insert(4, 'd');

      const node = sl.getFirstInRange(2, 3);
      expect(node?.element).toBe('b');
    });

    it('returns last node in score range', () => {
      const sl = createList();
      sl.insert(1, 'a');
      sl.insert(2, 'b');
      sl.insert(3, 'c');
      sl.insert(4, 'd');

      const node = sl.getLastInRange(2, 3);
      expect(node?.element).toBe('c');
    });

    it('returns null when range has no elements', () => {
      const sl = createList();
      sl.insert(1, 'a');
      sl.insert(5, 'b');

      expect(sl.getFirstInRange(2, 4)).toBeNull();
      expect(sl.getLastInRange(2, 4)).toBeNull();
    });

    it('handles exclusive ranges', () => {
      const sl = createList();
      sl.insert(1, 'a');
      sl.insert(2, 'b');
      sl.insert(3, 'c');

      // Exclusive min: skip score 2
      const first = sl.getFirstInRange(2, 3, true, false);
      expect(first?.element).toBe('c');

      // Exclusive max: skip score 3
      const last = sl.getLastInRange(2, 3, false, true);
      expect(last?.element).toBe('b');
    });

    it('returns null for empty list', () => {
      const sl = createList();
      expect(sl.getFirstInRange(0, 10)).toBeNull();
      expect(sl.getLastInRange(0, 10)).toBeNull();
    });

    it('handles -Infinity to +Infinity range', () => {
      const sl = createList();
      sl.insert(1, 'a');
      sl.insert(2, 'b');

      const first = sl.getFirstInRange(-Infinity, Infinity);
      expect(first?.element).toBe('a');

      const last = sl.getLastInRange(-Infinity, Infinity);
      expect(last?.element).toBe('b');
    });
  });

  describe('isInRange', () => {
    it('returns false for empty list', () => {
      const sl = createList();
      expect(sl.isInRange(0, 10)).toBe(false);
    });

    it('returns true when range overlaps with list', () => {
      const sl = createList();
      sl.insert(5, 'a');
      sl.insert(10, 'b');
      expect(sl.isInRange(3, 7)).toBe(true);
      expect(sl.isInRange(0, 100)).toBe(true);
    });

    it('returns false when range does not overlap', () => {
      const sl = createList();
      sl.insert(5, 'a');
      sl.insert(10, 'b');
      expect(sl.isInRange(11, 20)).toBe(false);
      expect(sl.isInRange(0, 4)).toBe(false);
    });
  });

  describe('countInRange', () => {
    it('counts elements in score range', () => {
      const sl = createList();
      sl.insert(1, 'a');
      sl.insert(2, 'b');
      sl.insert(3, 'c');
      sl.insert(4, 'd');
      sl.insert(5, 'e');

      expect(sl.countInRange(2, 4)).toBe(3);
      expect(sl.countInRange(1, 5)).toBe(5);
      expect(sl.countInRange(3, 3)).toBe(1);
    });

    it('counts with exclusive bounds', () => {
      const sl = createList();
      sl.insert(1, 'a');
      sl.insert(2, 'b');
      sl.insert(3, 'c');

      expect(sl.countInRange(1, 3, true, false)).toBe(2); // (1, 3] = b, c
      expect(sl.countInRange(1, 3, false, true)).toBe(2); // [1, 3) = a, b
      expect(sl.countInRange(1, 3, true, true)).toBe(1); // (1, 3) = b
    });

    it('returns 0 for empty range', () => {
      const sl = createList();
      sl.insert(5, 'a');
      expect(sl.countInRange(1, 4)).toBe(0);
    });
  });

  describe('deleteRangeByScore', () => {
    it('deletes elements in score range', () => {
      const sl = createList();
      sl.insert(1, 'a');
      sl.insert(2, 'b');
      sl.insert(3, 'c');
      sl.insert(4, 'd');
      sl.insert(5, 'e');

      const removed = sl.deleteRangeByScore(2, 4);
      expect(removed).toEqual(['b', 'c', 'd']);
      expect(sl.length).toBe(2);

      const elements = collectForward(sl);
      expect(elements).toEqual([
        { score: 1, element: 'a' },
        { score: 5, element: 'e' },
      ]);
    });

    it('handles exclusive bounds', () => {
      const sl = createList();
      sl.insert(1, 'a');
      sl.insert(2, 'b');
      sl.insert(3, 'c');

      const removed = sl.deleteRangeByScore(1, 3, true, true);
      expect(removed).toEqual(['b']);
      expect(sl.length).toBe(2);
    });
  });

  describe('deleteRangeByRank', () => {
    it('deletes elements in rank range (0-based)', () => {
      const sl = createList();
      sl.insert(1, 'a');
      sl.insert(2, 'b');
      sl.insert(3, 'c');
      sl.insert(4, 'd');
      sl.insert(5, 'e');

      const removed = sl.deleteRangeByRank(1, 3);
      expect(removed).toEqual(['b', 'c', 'd']);
      expect(sl.length).toBe(2);
    });

    it('deletes first element only', () => {
      const sl = createList();
      sl.insert(1, 'a');
      sl.insert(2, 'b');
      sl.insert(3, 'c');

      const removed = sl.deleteRangeByRank(0, 0);
      expect(removed).toEqual(['a']);
      expect(sl.length).toBe(2);
      expect(sl.head.lvl(0).forward?.element).toBe('b');
    });

    it('deletes last element only', () => {
      const sl = createList();
      sl.insert(1, 'a');
      sl.insert(2, 'b');
      sl.insert(3, 'c');

      const removed = sl.deleteRangeByRank(2, 2);
      expect(removed).toEqual(['c']);
      expect(sl.length).toBe(2);
      expect(sl.tail?.element).toBe('b');
    });

    it('deletes all elements', () => {
      const sl = createList();
      sl.insert(1, 'a');
      sl.insert(2, 'b');
      sl.insert(3, 'c');

      const removed = sl.deleteRangeByRank(0, 2);
      expect(removed).toEqual(['a', 'b', 'c']);
      expect(sl.length).toBe(0);
      expect(sl.tail).toBeNull();
    });

    it('returns empty when start > end', () => {
      const sl = createList();
      sl.insert(1, 'a');
      sl.insert(2, 'b');

      const removed = sl.deleteRangeByRank(2, 1);
      expect(removed).toEqual([]);
      expect(sl.length).toBe(2);
    });

    it('returns empty when start is beyond list length', () => {
      const sl = createList();
      sl.insert(1, 'a');

      const removed = sl.deleteRangeByRank(5, 10);
      expect(removed).toEqual([]);
      expect(sl.length).toBe(1);
    });
  });

  describe('deleteRangeByScore with infinities', () => {
    it('deletes all elements with -Infinity to +Infinity', () => {
      const sl = createList();
      sl.insert(1, 'a');
      sl.insert(2, 'b');
      sl.insert(3, 'c');

      const removed = sl.deleteRangeByScore(-Infinity, Infinity);
      expect(removed).toEqual(['a', 'b', 'c']);
      expect(sl.length).toBe(0);
      expect(sl.tail).toBeNull();
    });

    it('deletes from -Infinity to a score', () => {
      const sl = createList();
      sl.insert(-Infinity, 'neg');
      sl.insert(0, 'zero');
      sl.insert(5, 'five');

      const removed = sl.deleteRangeByScore(-Infinity, 0);
      expect(removed).toEqual(['neg', 'zero']);
      expect(sl.length).toBe(1);
    });

    it('deletes from a score to +Infinity', () => {
      const sl = createList();
      sl.insert(1, 'a');
      sl.insert(5, 'b');
      sl.insert(Infinity, 'inf');

      const removed = sl.deleteRangeByScore(5, Infinity);
      expect(removed).toEqual(['b', 'inf']);
      expect(sl.length).toBe(1);
    });
  });

  describe('ranks after bulk deletion', () => {
    it('ranks are correct after deleteRangeByScore', () => {
      const sl = createList();
      sl.insert(1, 'a');
      sl.insert(2, 'b');
      sl.insert(3, 'c');
      sl.insert(4, 'd');
      sl.insert(5, 'e');

      sl.deleteRangeByScore(2, 4);
      // Remaining: a(1), e(5)
      expect(sl.getRank(1, 'a')).toBe(0);
      expect(sl.getRank(5, 'e')).toBe(1);
      expect(sl.getElementByRank(1)?.element).toBe('a');
      expect(sl.getElementByRank(2)?.element).toBe('e');
    });

    it('ranks are correct after deleteRangeByRank', () => {
      const sl = createList();
      sl.insert(1, 'a');
      sl.insert(2, 'b');
      sl.insert(3, 'c');
      sl.insert(4, 'd');
      sl.insert(5, 'e');

      sl.deleteRangeByRank(1, 3);
      // Remaining: a(1), e(5)
      expect(sl.getRank(1, 'a')).toBe(0);
      expect(sl.getRank(5, 'e')).toBe(1);
      expect(sl.getElementByRank(1)?.element).toBe('a');
      expect(sl.getElementByRank(2)?.element).toBe('e');
    });
  });

  describe('backward pointers after bulk deletion', () => {
    it('backward chain intact after deleteRangeByScore', () => {
      const sl = createList();
      sl.insert(1, 'a');
      sl.insert(2, 'b');
      sl.insert(3, 'c');
      sl.insert(4, 'd');
      sl.insert(5, 'e');

      sl.deleteRangeByScore(2, 4);
      const backward = collectBackward(sl);
      expect(backward).toEqual([
        { score: 5, element: 'e' },
        { score: 1, element: 'a' },
      ]);
    });

    it('backward chain intact after deleteRangeByRank', () => {
      const sl = createList();
      sl.insert(1, 'a');
      sl.insert(2, 'b');
      sl.insert(3, 'c');
      sl.insert(4, 'd');
      sl.insert(5, 'e');

      sl.deleteRangeByRank(0, 2);
      const backward = collectBackward(sl);
      expect(backward).toEqual([
        { score: 5, element: 'e' },
        { score: 4, element: 'd' },
      ]);
      // First remaining node's backward should be null
      expect(sl.head.lvl(0).forward?.backward).toBeNull();
    });
  });

  describe('getElementByRank after deletions', () => {
    it('returns correct elements after single delete', () => {
      const sl = createList();
      sl.insert(1, 'a');
      sl.insert(2, 'b');
      sl.insert(3, 'c');

      sl.delete(2, 'b');
      expect(sl.getElementByRank(1)?.element).toBe('a');
      expect(sl.getElementByRank(2)?.element).toBe('c');
      expect(sl.getElementByRank(3)).toBeNull();
    });

    it('returns correct elements after deleting first', () => {
      const sl = createList();
      sl.insert(1, 'a');
      sl.insert(2, 'b');
      sl.insert(3, 'c');

      sl.delete(1, 'a');
      expect(sl.getElementByRank(1)?.element).toBe('b');
      expect(sl.getElementByRank(2)?.element).toBe('c');
    });
  });

  describe('stress test: bulk operations with invariant checks', () => {
    it('all invariants hold after mixed inserts and bulk deletes', () => {
      let seed = 123;
      const rng = (): number => {
        seed = (seed * 1103515245 + 12345) & 0x7fffffff;
        return seed / 0x7fffffff;
      };

      const sl = new SkipList(rng);
      const entries: { score: number; element: string }[] = [];

      // Insert 200 elements
      for (let i = 0; i < 200; i++) {
        const score = Math.floor(rng() * 1000);
        const element = `e${String(i).padStart(4, '0')}`;
        sl.insert(score, element);
        entries.push({ score, element });
      }

      entries.sort((a, b) =>
        a.score !== b.score
          ? a.score - b.score
          : a.element < b.element
            ? -1
            : a.element > b.element
              ? 1
              : 0
      );

      // Delete by score range [200, 500]
      const removed = sl.deleteRangeByScore(200, 500);
      const remaining = entries.filter((e) => !removed.includes(e.element));

      expect(sl.length).toBe(remaining.length);

      // Verify forward order
      const forward = collectForward(sl);
      expect(forward).toEqual(remaining);

      // Verify backward order
      const backward = collectBackward(sl);
      expect(backward).toEqual([...remaining].reverse());

      // Verify all ranks
      for (let i = 0; i < remaining.length; i++) {
        const e = remaining[i] as { score: number; element: string };
        expect(sl.getRank(e.score, e.element)).toBe(i);
        expect(sl.getElementByRank(i + 1)?.element).toBe(e.element);
      }

      // Delete by rank range [0, 9] (first 10 remaining)
      const removed2 = sl.deleteRangeByRank(0, 9);
      expect(removed2.length).toBe(Math.min(10, remaining.length));
      const remaining2 = remaining.slice(removed2.length);

      expect(sl.length).toBe(remaining2.length);

      // Verify ranks again
      for (let i = 0; i < remaining2.length; i++) {
        const e = remaining2[i] as { score: number; element: string };
        expect(sl.getRank(e.score, e.element)).toBe(i);
      }
    });
  });
});

// --- Helpers ---

function collectForward(sl: SkipList): { score: number; element: string }[] {
  const result: { score: number; element: string }[] = [];
  let node = sl.head.lvl(0).forward;
  while (node) {
    result.push({ score: node.score, element: node.element });
    node = node.lvl(0).forward;
  }
  return result;
}

function collectBackward(sl: SkipList): { score: number; element: string }[] {
  const result: { score: number; element: string }[] = [];
  let node = sl.tail;
  while (node) {
    result.push({ score: node.score, element: node.element });
    node = node.backward;
  }
  return result;
}

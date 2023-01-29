import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class Main {
    public static void main(String[] args) {
        final FakeRingBuffer frb = new FakeRingBuffer(256);
        frb.test(193, 262, 193, 262);
        frb.test(193, 262, 193, 263);
        frb.test(250, 260, 256, 306);
        frb.test(250, 260, 3000, 3040);
        frb.test(250, 260, 512, 514);
        frb.test(254, 258, 384, 388);
        frb.test(254, 258, 258, 258);
        frb.test(0, 127, 1024+128, 1024+256);
        frb.test(0, 128, 1024+128, 1024+256);
        frb.test(192, 256, 1024, 1024 + 192);
        frb.test(192, 256, 1024, 1024 + 191);
    }
}

class FakeRingBuffer {
    private final int capacity;
    private final int mask;
    private long calcedHead = 0;
    private long calcedTail = 0;
    private long head = 0;
    private long tail = 0;

    public FakeRingBuffer(int capacity) {
        this.capacity = capacity;
        this.mask = capacity - 1;
        if ((capacity & mask) != 0) {
            throw new RuntimeException("Capacity expected to be power of 2");
        }
    }

    public void test(long newCalcedHead, long newCalcedTail, long newHead, long newTail) {
        this.calcedHead = newCalcedHead;
        this.calcedTail = newCalcedTail;
        this.head = newHead;
        this.tail = newTail;
        System.out.printf("===== calcRange = [%d,%d), thisRange = [%d,%d) =====\n",
                calcedHead, calcedTail, head, tail);
        evaluate();
    }

    public void evaluate() {
        final long intersectionSize = calcedTail > head ? calcedTail - head : 0;

        long r1Head = calcedHead;
        long r1Tail = calcedTail - intersectionSize;

        long r2Head = head + intersectionSize;
        long r2Tail = tail;

        final long newBase = r1Head;
        r1Head -= newBase;
        r1Tail -= newBase;
        r2Head -= newBase;
        r2Tail -= newBase;

        final long r2Size = r2Tail - r2Head;
        r2Head = r2Head & mask;  // aka r2Head % capacity
        r2Tail = r2Head + r2Size;

        if (r2Tail <= capacity) {
            // R2 is a single segment in the "normal" direction
            // with no wrapping. You're in one of these cases
            // [----------) R1
            //      [--)            case 1 (subsume)
            //           [----)     case 2 (extend)
            //                 [--) case 3 (two ranges)

            if (r2Tail <= r1Tail) {  // case 1: subsume
                r2Head = r2Tail = 0;  // empty
            } else if (r2Head <= r1Tail) {  // case 2: extend
                r1Tail = r2Tail;
                r2Head = r2Tail = 0;  // empty
            }
            // else : R1 and R2 are correct as is.
       } else {
            // R2 crosses the modulus. We can think of it in two parts as
            // part 1: [r2ModHead, capacity)
            // part 2: [capacity, r2ModTail)
            // If we shift it left by capacity (note: values become negative which is ok)
            // then we have
            // part 1: [r2ModHead - capacity, 0)
            // part 2: [0, r2ModTail - capacity)

            // Because it's now centered at 0, it is suitable for extending
            // R1 on both sides. We do all this (adjust R2, extend R1, reverse the adjustment)
            // as a oneliner.
            r1Head = Math.min(r1Head, r2Head - capacity) + capacity;
            r1Tail = Math.max(r2Tail, r2Tail - capacity) + capacity;
            r2Head = r2Tail = 0;  // empty
        }

        // Reverse base adjustment
        r1Head += newBase;
        r1Tail += newBase;
        r2Head += newBase;
        r2Tail += newBase;

        fixTree(r1Head, r1Tail, r2Head, r2Tail, capacity);
        calcedHead = head;
        calcedTail = tail;
    }

    private static class Range implements Comparable<Range> {
        public Range(int begin, int end) {
            this.begin = begin;
            this.end = end;
        }

        int begin;
        int end;

        @Override
        public int compareTo(Range o) {
            return Integer.compare(this.begin, o.begin);
        }
    }

    private static void fixTree(long r1Head, long r1Tail, long r2Head, long r2Tail, int capacityForThisLevel) {
        ArrayList<Range> ranges = new ArrayList<>();
        addRange(ranges, r1Head, r1Tail, capacityForThisLevel);
        addRange(ranges, r2Head, r2Tail, capacityForThisLevel);
        Collections.sort(ranges);

        while (capacityForThisLevel != 0) {
            // Merge ranges and delete empty ranges
            int destIndex = 0;
            for (int srcIndex = 0; srcIndex != ranges.size(); ++srcIndex) {
                Range rSrc = ranges.get(srcIndex);
                if (rSrc.begin == rSrc.end) {
                    continue;
                }
                if (destIndex > 0 && ranges.get(destIndex - 1).end == rSrc.begin) {
                    ranges.get(destIndex - 1).end = rSrc.end;
                    continue;
                }
                ranges.set(destIndex, ranges.get(srcIndex));
                ++destIndex;
            }
            if (destIndex == 0) {
                break;
            }
            if (destIndex != ranges.size()) {
                ranges.subList(destIndex, ranges.size()).clear();
            }

            for (final Range r : ranges) {
                treeFixup(r.begin, r.end, capacityForThisLevel);
                r.begin /= 2;  // round down
                r.end = (r.end + 1) / 2;
            }
            capacityForThisLevel /= 2;
        }
    }
    private static void treeFixup(int head, int tail, int capacityForThisRow) {
        if (head == tail) {
            return;
        }
        System.out.printf("At level %d, recalculating values [%d,%d)\n", capacityForThisRow, head, tail);
    }

    private static void addRange(ArrayList<Range> ranges, long head, long tail, int capacity) {
        final long size = tail - head;
        if (size == 0) {
            return;
        }
        int mask = capacity - 1;
        int normalizedHead = (int)(head & mask);
        int normalizedTail = (int)(normalizedHead + size);
        if (normalizedTail <= capacity) {
            ranges.add(new Range(normalizedHead, normalizedTail));
        } else {
            ranges.add(new Range(0, normalizedTail - capacity));
            ranges.add(new Range(normalizedHead, capacity));
        }
    }
}

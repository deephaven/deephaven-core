package io.deephaven.db.v2.utils.rsp.container;

import org.junit.Assert;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;
import java.util.function.*;

import static java.lang.Short.toUnsignedInt;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertFalse;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

class ContainerTestCommon {
    static Container populate(String msg, int[] vs, Container container, String name,
        ArrayList<Integer> ranges) {
        ranges = getRanges(vs, ranges);
        final Iterator<Integer> it = ranges.iterator();
        while (it.hasNext()) {
            int start = it.next();
            int end = it.next();
            assertTrue(start < end);
            assertEquals(msg, start, start & 0xFFFF);
            assertEquals(msg, end - 1, (end - 1) & 0xFFFF);
            container = container.add(start, end);
        }
        assertEquals(msg, name, container.getContainerName());
        return container;
    }

    static Container populate(int[] vs, Container container, String name,
        ArrayList<Integer> ranges) {
        return populate("", vs, container, name, ranges);
    }

    static int[][] vss = new int[][] {
            // A negative number marks the end of a range starting int he previous element, for its
            // absolute value (inclusive).
            new int[] {10, 20, 30, 40, 50, 32767, -32768, 65535},
            new int[] {1, -3, 27, -28, 111, 345, 347, 349, 360, 16000, 32767, 65535},
            new int[] {0, 65, 129, -132, 255, -257, 32768, 65533, -65534},
            new int[] {0, -1},
            new int[] {0, -1, 63, -65, 128, 255, 513, 1024, -1025, 2047, 4191, 8192,
                    -(8192 + 64 * 4 - 1)},
            new int[] {0},
            new int[] {1},
            new int[] {65534},
            new int[] {65535},
            new int[] {0, -1, 62, -64, 127, -128, 188, -190, 193, 300, 639, -640},
            new int[] {1, -2, 4, -11, 13, -26},
            new int[] {1, -1000, 1002, -2000, 2002, -3000, 3002, -4000, 4002},
            new int[] {1, 3, 5, 7, 9, 11, 15, 17, 19},
            new int[] {1, -100, 102, 104, 106, 108, 110, -200, 202, 204, 206, 208, 210, -300},
            new int[] {1, -2, 4, -5, 7, -8, 10, -11, 13, -15, 17, -19},
            new int[] {0, 2, 4, 6, 65534, -65535},
            new int[] {0, -1, 3, 5, 7, -100, 65535},
            new int[] {0, 3, -5},
    };

    static void doTestFind(Supplier<Container> containerFactory, String containerName) {
        for (int[] vs : vss) {
            doTestFind(vs, containerFactory.get(), containerName);
        }
    }

    static void doTestFind(int[] vs, Container container, String name) {
        final ArrayList<Integer> ranges = new ArrayList<>(2 * vs.length);
        container = populate(vs, container, name, ranges);
        int preNon = 0;
        int i;
        int offset = 0;
        final Iterator<Integer> it = ranges.iterator();
        while (it.hasNext()) {
            final int start = it.next();
            final int end = it.next();
            for (i = start; i < end; ++i) {
                while (preNon < i) {
                    assertEquals("prenNon=" + preNon + ", i=" + i, -offset - 1,
                        container.find(ContainerUtil.lowbits(preNon)));
                    ++preNon;
                }
                assertEquals("i=" + i, offset, container.find(ContainerUtil.lowbits(i)));
                preNon = i + 1;
                ++offset;
            }
        }
    }

    static ArrayList<Integer> getRanges(int[] vs) {
        return getRanges(vs, null);
    }

    static ArrayList<Integer> getRanges(int[] vs, ArrayList<Integer> ranges) {
        if (ranges == null) {
            ranges = new ArrayList<>(2 * vs.length);
        }
        int i = 0;
        int lastStart = -1;
        int lastEnd = 0;
        while (i < vs.length) {
            int vsi = vs[i];
            if (vsi < 0) {
                // validate the input, in particular ensure non-adjacent ranges.
                assertNotEquals(-1, lastStart);
                assertTrue("i=" + i + ", vs[i]=" + vsi + ", lastEnd=" + lastEnd,
                    lastStart < -vsi);
                ranges.add(lastStart);
                ranges.add(-vsi + 1);
                lastStart = -1;
                lastEnd = -vsi;
            } else {
                if (lastEnd != 0) {
                    // more input validation as above.
                    assertTrue("i=" + i + ", vs[i]=" + vsi + ", lastEnd=" + lastEnd,
                        lastEnd < vsi - 1);
                }
                if (lastStart != -1) {
                    ranges.add(lastStart);
                    ranges.add(lastStart + 1);
                }
                lastStart = lastEnd = vsi;
            }
            ++i;
        }
        if (lastStart != -1) {
            ranges.add(lastStart);
            ranges.add(lastStart + 1);
        }
        return ranges;
    }

    static void doTestRangeIterator(Supplier<Container> containerFactory, String containerName) {
        for (int vi = 0; vi < vss.length; ++vi) {
            doTestRangeIterator(vi, containerFactory.get(), containerName);
        }
    }

    private static ArrayList<Integer> rangesSeek(
        final ArrayList<Integer> ranges, final int skip) {
        int remaining = skip;
        final ArrayList<Integer> rs = new ArrayList<>(remaining);
        final Iterator<Integer> it = ranges.iterator();
        while (remaining > 0) {
            assertTrue(it.hasNext());
            final int start = it.next();
            assertTrue(it.hasNext());
            final int end = it.next();
            final int count = end - start;
            if (remaining < count) {
                rs.add(start + remaining);
                rs.add(end);
                break;
            }
            remaining -= count;
        }
        while (it.hasNext()) {
            rs.add(it.next());
        }
        return rs;
    }

    static void doTestRangeIterator(int vi, Container container, String name) {
        final int[] vs = vss[vi];
        final ArrayList<Integer> ranges = new ArrayList<>(2 * vs.length);
        container = populate(vs, container, name, ranges);
        for (int skip = 0; skip < container.getCardinality(); ++skip) {
            final ArrayList<Integer> rs =
                rangesSeek(ranges, Math.min(container.getCardinality(), skip));
            doTestRangeIterator(vi, container, skip, rs);
        }

    }

    static void doTestRangeIterator(int vi, Container container, final int seek,
        final ArrayList<Integer> ranges) {
        final RangeIterator cit = container.getShortRangeIterator(seek);
        final Iterator<Integer> ait = ranges.iterator();
        int r = 0;
        final String m = "vi=" + vi + ", seek=" + seek;
        while (cit.hasNext()) {
            cit.next();
            int cstart = cit.start();
            int cend = cit.end();
            final String m2 = m + ", r=" + r + ", cstart=" + cstart + ", cend=" + cend;
            assertTrue(m2, ait.hasNext());
            int astart = ait.next();
            assertTrue(m2 + ", astart=" + astart, ait.hasNext());
            int aend = ait.next();
            assertEquals(m2, astart, cstart);
            assertEquals(m2, aend, cend);
            ++r;
        }
        assertFalse(ait.hasNext());
    }

    static void doTestRangeIteratorNextBuffer(final Supplier<Container> containerFactory,
        final String containerName) {
        for (int vi = 0; vi < vss.length; ++vi) {
            doTestRangeIteratorNextBuffer(vi, containerFactory.get(), containerName);
        }
    }

    private static void doTestRangeIteratorNextBuffer(final int vi, final Container container,
        final String name) {
        final int[] vs = vss[vi];
        final String m = "vi=" + vi;
        final ArrayList<Integer> ranges = new ArrayList<>(2 * vs.length);
        final Container c = populate(vs, container, name, ranges);
        final RangeIterator rit = c.getShortRangeIterator(0);
        final Iterator<Integer> ait = ranges.iterator();
        final int offset = 2;
        final short[] buf = new short[offset + 2 * 3];
        final int maxShorts = buf.length - offset;
        int r = 0;
        while (rit.hasNext()) {
            final int count = rit.next(buf, offset, maxShorts / 2);
            final int n = 2 * count;
            int i = 0;
            final String m2 = m + ", r=" + r;
            while (i < n) {
                assertTrue(m2, ait.hasNext());
                final int s = ait.next();
                assertTrue(m2, ait.hasNext());
                final int e = ait.next();
                assertEquals(m2, (short) s, buf[offset + i++]);
                assertEquals(m2, (short) (e - 1), buf[offset + i++]);
            }
            assertEquals(m2, i, n);
            assertEquals(m2, (short) rit.start(), buf[offset + i - 2]);
            assertEquals(m2, (short) (rit.end() - 1), buf[offset + i - 1]);
            ++r;
        }
        assertFalse(m, ait.hasNext());
    }

    static void doTestContainerShortBatchIterator(Supplier<Container> containerFactory,
        String containerName) {
        for (int i = 2; i < vss.length; ++i) {
            final Container c = containerFactory.get();
            doTestContainerShortBatchIterator(i, c, containerName);
            doTestContainerShortBatchIteratorForEach(i, c, containerName);
        }
    }

    static void doTestContainerShortBatchIterator(int vi, Container container, String name) {
        int[] vs = vss[vi];
        final String m = "vi=" + vi;
        final ArrayList<Integer> ranges = new ArrayList<>(2 * vs.length);
        container = populate(vs, container, name, ranges);
        final ContainerShortBatchIterator cit = container.getShortBatchIterator(0);
        final Iterator<Integer> ait = ranges.iterator();
        final short[] buf = new short[7];
        final int[] count = new int[1];
        final int offset = 2;
        final int[] voffset = new int[1];
        final Predicate<ContainerShortBatchIterator> hasNextForContainerIter =
            (it) -> (count[0] > 0) || it.hasNext();
        final Function<ContainerShortBatchIterator, Integer> nextForContainerIter =
            (it) -> {
                if (count[0] == 0) {
                    voffset[0] = offset;
                    count[0] = it.next(buf, offset, buf.length - offset);
                    assertTrue(count[0] > 0);
                }
                final int v = toUnsignedInt(buf[voffset[0]]);
                ++voffset[0];
                --count[0];
                return v;
            };

        final int[] start = new int[1];
        final int[] end = new int[1];
        final int[] curr = new int[1];
        final Predicate<Iterator<Integer>> hasNextForRanges =
            (it) -> ((curr[0] < end[0]) || it.hasNext());
        final Function<Iterator<Integer>, Integer> nextForRanges =
            (it) -> {
                if (curr[0] >= end[0]) {
                    start[0] = curr[0] = it.next();
                    end[0] = it.next();
                }
                return curr[0]++;
            };
        int r = 0;
        while (hasNextForContainerIter.test(cit)) {
            final String m2 = m + ", r=" + r;
            assertTrue(m2, hasNextForRanges.test(ait));
            final int cv = nextForContainerIter.apply(cit);
            final int av = nextForRanges.apply(ait);
            assertEquals(m2, av, cv);
            ++r;
        }
        assertFalse(hasNextForRanges.test(ait));
    }

    static void doTestContainerShortBatchIteratorForEach(int vi, Container container, String name) {
        int[] vs = vss[vi];
        final ArrayList<Integer> ranges = new ArrayList<>(2 * vs.length);
        container = populate(vs, container, name, ranges);
        for (int skip = 0; skip < container.getCardinality(); ++skip) {
            final ArrayList<Integer> rs =
                rangesSeek(ranges, Math.min(container.getCardinality(), skip));
            final String m = "vi=" + vi + ", skip=" + skip;
            doTestContainerShortBatchIteratorForEach(m, container, skip, rs);
        }
    }

    private static void doTestContainerShortBatchIteratorForEach(
        final String m, final Container container, final int skip,
        final ArrayList<Integer> ranges) {
        final ContainerShortBatchIterator cit = container.getShortBatchIterator(skip);
        final Iterator<Integer> ait = ranges.iterator();
        final short[] buf = new short[7];
        final int[] count = new int[1];
        final int offset = 2;
        final int[] voffset = new int[1];
        final Predicate<ContainerShortBatchIterator> hasNextForContainerIter =
            (it) -> (count[0] > 0) || it.hasNext();
        final Function<ContainerShortBatchIterator, Integer> nextForContainerIter =
            (it) -> {
                if (count[0] == 0) {
                    voffset[0] = offset;
                    it.forEach((short v) -> {
                        buf[voffset[0] + count[0]] = v;
                        ++count[0];
                        return count[0] < buf.length - offset;
                    });
                    assertTrue(count[0] > 0);
                }
                final int v = toUnsignedInt(buf[voffset[0]]);
                ++voffset[0];
                --count[0];
                return v;
            };

        final int[] end = new int[1];
        final int[] curr = new int[1];
        final Predicate<Iterator<Integer>> hasNextForRanges =
            (it) -> ((curr[0] < end[0]) || it.hasNext());
        final Function<Iterator<Integer>, Integer> nextForRanges =
            (it) -> {
                if (curr[0] >= end[0]) {
                    curr[0] = it.next();
                    end[0] = it.next();
                }
                return curr[0]++;
            };
        int r = 0;
        while (hasNextForContainerIter.test(cit)) {
            final String m2 = m + ", r=" + r;
            assertTrue(m2, hasNextForRanges.test(ait));
            final int cv = nextForContainerIter.apply(cit);
            final int av = nextForRanges.apply(ait);
            assertEquals(m2, av, cv);
            ++r;
        }
        assertFalse(hasNextForRanges.test(ait));
    }

    static void doTestRangeIteratorAdvance(Supplier<Container> containerFactory,
        String containerName) {
        for (int i = 0; i < vss.length; ++i) {
            doTestRangeIteratorAdvance(i, containerFactory.get(), containerName);
        }
    }

    static final int seed0 = 1;

    static void doTestRangeIteratorAdvance(int iv, Container container, String name) {
        final int[] vs = vss[iv];
        final ArrayList<Integer> ranges = new ArrayList<>(2 * vs.length);
        container = populate(vs, container, name, ranges);
        final int ntests = 10;
        final Random r = new Random(seed0);
        final String m1 = "iv=" + iv;
        for (int t = 0; t < ntests; ++t) {
            final SearchRangeIterator cit = container.getShortRangeIterator(0);
            final Iterator<Integer> ait = ranges.iterator();
            int range = 0;
            final float pPickRange = 0.2F;
            final String m2 = m1 + ", t=" + t;
            while (ait.hasNext()) {
                final boolean doThisRange = r.nextFloat() < pPickRange;
                if (!doThisRange) {
                    continue;
                }
                int astart = ait.next();
                int aend = ait.next() - 1;
                int v = astart - 1 + r.nextInt(aend - astart + 2);
                if (v < 0) {
                    v = 0;
                }
                final String m3 =
                    m2 + ", range=" + range + ", astart=" + astart + ", aend=" + aend + ", v=" + v;
                final boolean result = cit.advance(v);
                assertTrue(m3, result);
                assertEquals(m3, Math.max(v, astart), cit.start());
                assertEquals(m3, aend + 1, cit.end());
                ++range;
            }
            final int last = container.last();
            assertEquals(m2, false, cit.advance(last + 1));
            assertFalse(m2, cit.hasNext());
        }
    }

    static void doTestRangeIteratorSearch(Supplier<Container> containerFactory,
        String containerName) {
        for (int i = 0; i < vss.length; ++i) {
            doTestRangeIteratorSearch(i, containerFactory.get(), containerName);
            doTestRangeIteratorSearch2(i, containerFactory.get(), containerName);
        }
    }

    // expectedEnd is inclusive.
    private static void doSingleSearch(final String m, final Container compContainer,
        final SearchRangeIterator cit, final int v,
        final boolean expectedResult, final int expectedStart, final int expectedEnd) {
        ContainerUtil.TargetComparator comp = (k) -> {
            final boolean check = compContainer.contains((short) k);
            assertTrue(check);
            if (k <= v) {
                return 1;
            }
            return -1;
        };
        final boolean b = cit.search(comp);
        final String m2 = m + ", v=" + v;
        assertEquals(m2, expectedResult, b);
        if (b) {
            assertEquals(m2, expectedStart, cit.start());
            assertEquals(m2, expectedEnd + 1, cit.end());
        }
    }

    static void doTestRangeIteratorSearch(int iv, Container container, String name) {
        final int[] vs = vss[iv];
        final ArrayList<Integer> ranges = new ArrayList<>(2 * vs.length);
        container = populate(vs, container, name, ranges);
        final Random r = new Random(seed0);
        final String m1 = "iv=" + iv;
        final SearchRangeIterator cit = container.getShortRangeIterator(0);
        final Iterator<Integer> ait = ranges.iterator();
        int range = 0;
        final float pPickRange = 0.2F;
        int lastEnd = -1;
        while (ait.hasNext()) {
            final boolean doThisRange = r.nextFloat() < pPickRange;
            if (!doThisRange) {
                continue;
            }
            final int astart = ait.next();
            final int aend = ait.next() - 1; // inclusive.
            final String m2 = m1 + ", range=" + range + ", astart=" + astart + ", aend=" + aend;
            if (lastEnd + 1 < astart) {
                doSingleSearch(m2, container, cit, lastEnd + 1, lastEnd != -1, lastEnd, lastEnd);
                if (astart - 1 > 0 && astart - 1 < lastEnd + 1) {
                    doSingleSearch(m2, container, cit, astart - 1, true, lastEnd, lastEnd);
                }
            }
            for (int v = astart; v <= aend; ++v) {
                doSingleSearch(m2, container, cit, v, true, v, aend);
            }
            lastEnd = aend;
            ++range;
        }
        final int last = container.last();
        ContainerUtil.TargetComparator comp = (k) -> Integer.compare(last, k);
        assertTrue(m1, cit.search(comp));
        assertEquals(m1, last, cit.start());
        assertEquals(m1, last + 1, cit.end());
        comp = (k) -> Integer.compare(last + 1, k);
        assertTrue(m1, cit.search(comp));
        assertEquals(m1, last, cit.start());
        assertEquals(m1, last + 1, cit.end());
    }

    static void doTestRangeIteratorSearch2(int iv, Container container, String name) {
        final int[] vs = vss[iv];
        final ArrayList<Integer> ranges = new ArrayList<>(2 * vs.length);
        container = populate(vs, container, name, ranges);
        final String m1 = "iv=" + iv;
        final SearchRangeIterator cit = container.getShortRangeIterator(0);
        final Iterator<Integer> ait = ranges.iterator();
        int range = 0;
        int prevLast = -1;
        while (ait.hasNext()) {
            final int astart = ait.next();
            final int alast = ait.next() - 1; // inclusive.
            final String m2 = m1 + ", range=" + range + ", astart=" + astart + ", alast=" + alast;
            final int[] searches;
            if (alast != astart) {
                searches = new int[] {astart - 1, astart, astart + 1, alast - 1, alast};
            } else {
                searches = new int[] {astart - 1, astart};
            }
            for (int j = 0; j < searches.length; ++j) {
                final int v = searches[j];
                if (v < 0)
                    continue;
                final String m3 = m2 + ", j=" + j;
                if (container.contains((short) v) && cit.start() <= v) {
                    doSingleSearch(m3, container, cit, v, true, v, alast);
                } else {
                    doSingleSearch(m3, container, cit, v,
                        cit.start() <= v && !(prevLast == -1 && j == 0), prevLast, prevLast);
                }
                prevLast = cit.end() - 1;
            }
            ++range;
        }
        final int last = container.last();
        ContainerUtil.TargetComparator comp = (k) -> Integer.compare(last, k);
        assertTrue(m1, cit.search(comp));
        assertEquals(m1, last, cit.start());
        assertEquals(m1, last + 1, cit.end());
        comp = (k) -> Integer.compare(last + 1, k);
        assertTrue(m1, cit.search(comp));
        assertEquals(m1, last, cit.start());
        assertEquals(m1, last + 1, cit.end());
    }

    static void doTestSelect(Supplier<Container> containerFactory, String containerName) {
        for (int[] vs : vss) {
            doTestSelect(vs, containerFactory.get(), containerName);
        }
    }

    static void doTestSelect(int[] vs, Container container, String name) {
        final ArrayList<Integer> ranges = new ArrayList<>(2 * vs.length);
        container = populate(vs, container, name, ranges);
        int offset = 0;
        final Iterator<Integer> it = ranges.iterator();
        while (it.hasNext()) {
            int start = it.next();
            int end = it.next();
            for (int i = start; i < end; ++i) {
                assertEquals("i=" + i + ", offset=" + offset, ContainerUtil.lowbits(i),
                    container.select(offset));
                ++offset;
            }
        }
    }

    private static class ToArrayRangeConsumer implements RangeConsumer {
        ArrayList<Integer> ranges = new ArrayList<>(16);

        @Override
        public void accept(int begin, int end) {
            ranges.add(begin);
            ranges.add(end);
        }

        public ArrayList<Integer> getRanges() {
            return ranges;
        }
    }

    static void doTestSelectRanges(Supplier<Container> containerFactory, String containerName) {
        for (int i = 0; i < vss.length; ++i) {
            doTestSelectRanges(i, containerFactory.get(), containerName);
        }
    }

    static void doTestSelectContainer(Supplier<Container> containerFactory, String containerName) {
        for (int i = 0; i < vss.length; ++i) {
            doTestSelectContainer(i, containerFactory.get(), containerName);
        }
    }

    static void doTestSelectContainer(final int vi, final Container inContainer,
        final String name) {
        final int[] vs = vss[vi];
        final String m = "vi==" + vi;
        final ArrayList<Integer> ranges = new ArrayList<>(2 * vs.length);
        final Container container = populate(vs, inContainer, name, ranges);
        final int nRanges = ranges.size() / 2;
        final IntFunction<Integer> rangeStart = (int i) -> ranges.get(2 * i);
        final IntFunction<Integer> rangeEnd = (int i) -> ranges.get(2 * i + 1);
        final Container empty = new ArrayContainer();
        final int lastRank = container.getCardinality() - 1;
        for (int ir = 0; ir < nRanges; ++ir) {
            for (int jr = ir; jr < nRanges; ++jr) {
                final int is = rangeStart.apply(ir);
                final int je = rangeEnd.apply(jr);
                final int sRank = container.find((short) is);
                final int eRank = container.find((short) je);
                for (int sr = sRank - 1; sr <= sRank + 1; ++sr) {
                    if (sr < 0) {
                        continue;
                    }
                    for (int er = eRank + 1; er >= eRank - 1; --er) {
                        if (er < 0 || er < sr || er > lastRank) {
                            continue;
                        }
                        final String m2 = m + " && sr==" + sr + " && er==" + er;
                        final Container pre = (sr == 0) ? empty : container.select(0, sr);
                        assertEquals(m2, sr, pre.getCardinality());
                        final Container sc = container.select(sr, er + 1);
                        assertEquals(m2, er - sr + 1, sc.getCardinality());
                        assertEquals(m2, toUnsignedInt(container.select(sr)), sc.first());
                        assertEquals(m2, toUnsignedInt(container.select(er)), sc.last());
                        final Container pos =
                            (er + 1 > lastRank) ? empty : container.select(er + 1, lastRank + 1);
                        assertEquals(m2, (er + 1 > lastRank) ? 0 : lastRank - er,
                            pos.getCardinality());
                        assertFalse(m2, pre.intersects(sc));
                        assertFalse(m2, sc.intersects(pos));
                        final Container u = pre.or(sc).or(pos);
                        assertEquals(m2, container.getCardinality(),
                            container.and(u).getCardinality());
                    }
                }
            }
        }

    }

    private static class ArrayListRangeIterator extends RangeIterator.ArrayBacked {
        public ArrayListRangeIterator(ArrayList<Integer> ranges) {
            super(toArray(ranges));

        }

        private static int[] toArray(ArrayList<Integer> alranges) {
            final int[] ranges = new int[alranges.size()];
            for (int i = 0; i < alranges.size(); ++i) {
                ranges[i] = alranges.get(i);
            }
            return ranges;
        }
    }

    private static RangeIterator makeRangeIterator(int[] vs) {
        return new ArrayListRangeIterator(getRanges(vs));
    }

    private static RangeIterator makeRangeIterator(ArrayList<Integer> ranges) {
        return new ArrayListRangeIterator(ranges);
    }

    static void doTestSelectRanges(int vi, Container container, String name) {
        final int[] vs = vss[vi];
        final String m = "vi=" + vi;
        final ArrayList<Integer> ranges = new ArrayList<>(2 * vs.length);
        container = populate(vs, container, name, ranges);
        int offset = 0;
        // first try all single offset values as a singleton range.
        final Iterator<Integer> it = ranges.iterator();
        while (it.hasNext()) {
            int start = it.next();
            int end = it.next();
            for (int key = start; key < end; ++key) {
                final RangeIterator in = makeRangeIterator(new int[] {offset});
                final ToArrayRangeConsumer out = new ToArrayRangeConsumer();
                container.selectRanges(out, in);
                final ArrayList<Integer> oranges = out.getRanges();
                final String m2 = m + ", key=" + key + ", offset=" + offset;
                assertEquals(m2, 2, oranges.size());
                assertEquals(m2, 1, oranges.get(1) - oranges.get(0));
                final int apples = key;
                assertEquals(m2, apples, (int) oranges.get(0));
                ++offset;
            }
        }
        if (vs.length == 1) {
            return;
        }
        // Now select all the ranges.
        final RangeIterator in =
            makeRangeIterator(new int[] {0, -(container.getCardinality() - 1)});
        final ToArrayRangeConsumer out = new ToArrayRangeConsumer();
        container.selectRanges(out, in);
        final ArrayList<Integer> oranges = out.getRanges();
        final ArrayList<Integer> apples = ranges;
        assertEquals(m, apples, oranges);
    }

    static void doTestFindRanges(Supplier<Container> containerFactory, String containerName) {
        for (int vi = 0; vi < vss.length; ++vi) {
            doTestFindRanges(vi, containerFactory.get(), containerName);
        }
    }

    static void doTestFindRanges(int vi, Container container, String name) {
        final int[] vs = vss[vi];
        final ArrayList<Integer> ranges = new ArrayList<>(2 * vs.length);
        container = populate(vs, container, name, ranges);
        int offset = 0;
        // first try all single key values as a singleton range.
        final Iterator<Integer> it = ranges.iterator();
        while (it.hasNext()) {
            int start = it.next();
            int end = it.next();
            for (int i = start; i < end; ++i) {
                final RangeIterator in = makeRangeIterator(new int[] {i});
                final ToArrayRangeConsumer out = new ToArrayRangeConsumer();
                container.findRanges(out, in, 0xFFFF);
                final ArrayList<Integer> oranges = out.getRanges();
                final String msg =
                    "vi=" + vi + ", name=" + name + ", i=" + i + ", offset=" + offset;
                assertEquals(msg, 2, oranges.size());
                assertEquals(msg, 1, oranges.get(1) - oranges.get(0));
                final int apples = offset;
                assertEquals(msg, apples, (int) oranges.get(0));
                ++offset;
            }
        }
        // Now find all the ranges.
        final RangeIterator in = makeRangeIterator(ranges);
        final ToArrayRangeConsumer out = new ToArrayRangeConsumer();
        container.findRanges(out, in, 0xFFFF);
        final ArrayList<Integer> oranges = out.getRanges();
        assertEquals(2, oranges.size());
        assertEquals(0, (int) oranges.get(0));
        assertEquals(container.getCardinality(), (int) oranges.get(1));
    }

    static void doTestFindRangesWithMaxPos(Supplier<Container> containerFactory, String name) {
        doTestFindRangesWithMaxPos1(containerFactory, name);
        doTestFindRangesWithMaxPos2(containerFactory, name);
    }

    static void doTestFindRangesWithMaxPos1(Supplier<Container> containerFactory, String name) {
        Container ac = containerFactory.get();
        ac = ac.add(1, 4);
        ac = ac.add(6, 9);
        ac = ac.add(11, 13);
        ac = ac.iset((short) 14);
        ac = ac.add(19, 22);
        assertEquals(name, ac.getContainerName());
        final ArrayList<Integer> vs = new ArrayList<>();
        RangeConsumer rc = (int begin, int end) -> {
            for (int i = begin; i < end; ++i) {
                vs.add(i);
            }
        };
        ac.findRanges(rc, new RangeIterator.Single(11, 13), 7);
        assertEquals(vs.size(), 2);
        Assert.assertTrue(vs.contains(6));
        Assert.assertTrue(vs.contains(7));
    }

    static Container addValues(Container c, final int... vs) {
        for (int v : vs) {
            short w = (short) v;
            assertEquals(v, (int) w);
            c = c.iset(w);
        }
        return c;
    }

    static void doTestFindRangesWithMaxPos2(Supplier<Container> containerFactory, String name) {
        Container ac = containerFactory.get();
        ac = addValues(ac, 1, 4, 7, 9, 10);
        assertEquals(name, ac.getContainerName());
        final ArrayList<Integer> vs = new ArrayList<>();
        RangeConsumer rc = (int begin, int end) -> {
            for (int i = begin; i < end; ++i) {
                vs.add(i);
            }
        };
        ac.findRanges(rc, new RangeIterator.ArrayBacked(new int[] {4, 5, 7, 8, 10, 11}), 3);
        assertEquals(vs.size(), 2);
        Assert.assertTrue(vs.contains(1));
        Assert.assertTrue(vs.contains(2));
    }

    static int[][] vss2 = new int[][] {
            new int[] {0, -2, 4, -10, 64, -68, 127, -128, 65500, -65535},
            new int[] {3, 11, -63, 69, -126, 129, 59900, -59999, 65535},
            new int[] {0, -2, 4, -10, 65300, -65535},
            new int[] {0, -2, 4, -10, 64, -68, 127, -128, 65499, -65535},
            new int[] {0},
            new int[] {65535},
            new int[] {},
    };

    interface BoolContainerOp {
        boolean op(Container c1, Container c2);
    }

    static void doTestBoolOp(
        int vi, int vj,
        BoolContainerOp testOp, BoolContainerOp validateOp) {
        int[] vs1 = vss2[vi];
        int[] vs2 = vss2[vj];
        String pfx = "vi=" + vi + ", vj=" + vj;
        final ArrayList<Integer> r1 = new ArrayList<>(2 * vs1.length);
        final ArrayList<Integer> r2 = new ArrayList<>(2 * vs2.length);
        Container[] cs = new Container[] {
                new ArrayContainer(), new BitmapContainer(), new RunContainer(),
        };
        String[] cNames = new String[] {"array", "bitmap", "run"};
        for (int ci = 0; ci < cs.length; ++ci) {
            for (int cj = 0; cj < cs.length; ++cj) {
                String s = pfx + ", ci=" + ci + ", cj=" + cj;
                Container c1 = populate(s, vs1, cs[ci], cNames[ci], r1);
                Container c2 = populate(s, vs2, cs[cj], cNames[cj], r2);
                boolean result = testOp.op(c1, c2);
                boolean expected = validateOp.op(c1, c2);
                assertEquals(s, expected, result);
                result = testOp.op(c2, c1);
                expected = validateOp.op(c2, c1);
                assertEquals(s, expected, result);
            }
        }
    }

    static void doTestBoolOp(
        BoolContainerOp testOp, BoolContainerOp validateOp) {
        for (int i = 0; i < vss2.length; ++i) {
            for (int j = i + 1; j < vss2.length; ++j) {
                doTestBoolOp(i, j, testOp, validateOp);
            }
        }
    }

    static void doTestRemoveRange(final Supplier<Container> containerFactory,
        final String containerName) {
        for (int vi = 0; vi < vss.length; ++vi) {
            doTestRemoveRange(vi, containerFactory.get(), containerName);
        }
    }

    static final int removeTestCardThreshold = 1000;

    static void doTestRemoveRange(final int vi, Container container, final String name) {
        final int[] vs = vss[vi];
        final ArrayList<Integer> ranges = new ArrayList<>(2 * vs.length);
        container = populate(vs, container, name, ranges);
        container.validate();
        int card = container.getCardinality();
        if (card > removeTestCardThreshold) {
            // the algorithm below is n^2...
            return;
        }
        final String m = "vi==" + vi;
        for (int i = 0; i < card; ++i) {
            for (int j = i; j < card; ++j) {
                final String m2 = m + " && i==" + i + " && j==" + j;
                final int ki = toUnsignedInt(container.select(i));
                final int kj = toUnsignedInt(container.select(j));
                Container c2 = container.deepCopy();
                c2 = c2.iremove(ki, kj + 1);
                c2.validate();
                Container r1 = Container.rangeOfOnes(ki, kj + 1);
                r1 = r1.and(container);
                r1.validate();
                Container r2 = r1.and(c2);
                r2.validate();
                assertTrue(m2, r2.isEmpty());
                Container r3 = r1.or(c2);
                r3.validate();
                // since we don't have a specialized Container.sameContents().
                assertTrue(m2, r3.subsetOf(container));
                assertTrue(m2, container.subsetOf(r3));
            }
        }
    }

    static void doTestCopyOnWrite(final Supplier<Container> containerFactory,
        final String containerName) {
        Container c = containerFactory.get();
        c = c.add(10, 100);
        Container c2 = c.deepCopy();
        c2.setCopyOnWrite();
        Container c22 = c2.iset((short) 100);
        assertTrue(c22 != c2);
        c2 = c.deepCopy();
        c2.setCopyOnWrite();
        c22 = c2.iadd(100, 102);
        assertTrue(c22 != c2);
        c2 = c.deepCopy();
        c2.setCopyOnWrite();
        c22 = c2.iremove(87, 99);
        assertTrue(c22 != c2);
        c2 = c.deepCopy();
        c2.setCopyOnWrite();
        Container c3 = containerFactory.get();
        c3 = c3.add(50, 150);
        c22 = c2.ior(c3);
        assertTrue(c22 != c2);
        c2 = c.deepCopy();
        c2.setCopyOnWrite();
        c22 = c2.iand(c3);
        assertTrue(c22 != c2);
        c2 = c.deepCopy();
        c2.setCopyOnWrite();
        c22 = c2.iandNot(c3);
        assertTrue(c22 != c2);
        c2 = c.deepCopy();
        c2.setCopyOnWrite();
    }

    static void doTestForEachWithRankOffset(final Supplier<Container> containerFactory,
        final String containerName) {
        for (int vi = 0; vi < vss.length; ++vi) {
            doTestForEachWithRankOffset(vi, containerFactory.get(), containerName);
        }
    }

    private static void doTestForEachWithRankOffset(final int vi, Container container,
        final String name) {
        final int[] vs = vss[vi];
        final ArrayList<Integer> ranges = new ArrayList<>(2 * vs.length);
        container = populate(vs, container, name, ranges);
        int card = container.getCardinality();
        final String m = "vi==" + vi;
        for (int i = 0; i < card; ++i) {
            final String m2 = m + " && i==" + i;
            final Container c2 = container.select(i, card);
            final int[] j = new int[1];
            j[0] = 0;
            container.forEach(i, (final short v) -> {
                assertEquals(c2.select(j[0]), v);
                ++j[0];
                return true;
            });
        }
    }

    static void doTestForEachRange(final Supplier<Container> containerFactory,
        final String containerName) {
        for (int vi = 0; vi < vss.length; ++vi) {
            doTestForEachRange(vi, containerFactory.get(), containerName);
        }
    }

    private static void doTestForEachRange(final int vi, Container container, final String name) {
        final int[] vs = vss[vi];
        final ArrayList<Integer> ranges = new ArrayList<>(2 * vs.length);
        container = populate(vs, container, name, ranges);
        int card = container.getCardinality();
        final String m = "vi==" + vi;
        for (int i = 0; i < card; ++i) {
            final String m2 = m + " && i==" + i;
            final Container c2 = container.select(i, card);
            final Container[] res = {new ArrayContainer()};
            final int[] prevRangeEnd = {-2};
            final boolean b = container.forEachRange(i, (final short s, final short e) -> {
                final int is = toUnsignedInt(s);
                final int ie = toUnsignedInt(e);
                final String m3 = m2 + "is==" + is + " && ie==" + ie;
                assertTrue(m3, is <= ie);
                assertTrue(m3, prevRangeEnd[0] + 1 < is);
                res[0] = res[0].add(is, ie + 1);
                return true;
            });
            assertTrue(m2, b);
            assertEquals(m2, c2.getCardinality(), res[0].getCardinality());
            assertTrue(m2, c2.subsetOf(res[0]));
        }
    }

    static void doTestOverlapsRange(final Supplier<Container> containerFactory,
        final String containerName) {
        for (int vi = 0; vi < vss.length; ++vi) {
            doTestOverlapsRange(vi, containerFactory.get(), containerName);
        }
    }

    private static void doTestOverlapsRange(final int vi, Container container, final String name) {
        final int[] vs = vss[vi];
        final ArrayList<Integer> ranges = new ArrayList<>(2 * vs.length);
        container = populate(vs, container, name, ranges);
        final String m = "vi==" + vi;
        for (int i = 0; i < ranges.size() - 1; ++i) {
            int k1 = ranges.get(i);
            int k2 = ranges.get(i + 1) - 1;
            for (int k3 = k1 - 1; k3 <= k1 + 1; ++k3) {
                for (int k4 = k2 - 1; k4 <= k2 + 1; ++k4) {
                    if (k3 < 0 || k4 < 0 || k4 > 0xFFFF || k4 < k3) {
                        continue;
                    }
                    for (int start : new int[] {k3, k4}) {
                        final int end = k4 + 1;
                        final String m2 = m + " && start==" + start + " && end==" + end;
                        final boolean r = container.overlapsRange(start, end);
                        final Container rangeAsContainer = Container.rangeOfOnes(start, end);
                        final boolean expected = rangeAsContainer.overlaps(container);
                        assertEquals(m2, expected, r);
                    }
                }
            }
        }
    }

    static void doTestContainsRange(final Supplier<Container> containerFactory,
        final String containerName) {
        for (int vi = 0; vi < vss.length; ++vi) {
            doTestContainsRange(vi, containerFactory.get(), containerName);
        }
    }

    private static void doTestContainsRange(final int vi, Container container, final String name) {
        final int[] vs = vss[vi];
        final ArrayList<Integer> ranges = new ArrayList<>(2 * vs.length);
        container = populate(vs, container, name, ranges);
        final String m = "vi==" + vi;
        final RangeIterator it = container.getShortRangeIterator(0);
        while (it.hasNext()) {
            it.next();
            final int first = it.start();
            final int last = it.end() - 1;
            for (int df = -1; df <= 1; ++df) {
                for (int dl = -1; dl <= 1; ++dl) {
                    int start = first + df;
                    if (start < 0) {
                        continue;
                    }
                    int end = last + dl + 1;
                    if (end <= start || end > 0xFFFF) {
                        continue;
                    }
                    String m2 = m + " && start==" + start + " && end==" + end;
                    boolean r = container.contains(start, end);
                    final Container c2 = Container.rangeOfOnes(start, end);
                    final Container c3 = container.and(c2);
                    final boolean expected =
                        c3.subsetOf(c2) && c3.getCardinality() == c2.getCardinality();
                    assertEquals(m2, expected, r);
                }
            }
        }
    }

    static void doTestAppend(final Supplier<Container> containerFactory,
        final String containerName) {
        for (int vi = 0; vi < vss.length; ++vi) {
            doTestAppend(vi, containerFactory.get(), containerName);
        }
    }

    private static void doTestAppend(final int vi, Container container,
        final String containerName) {
        final int[] vs = vss[vi];
        ArrayList<Integer> ranges = new ArrayList<>(2 * vs.length);
        final Container expected = populate(vs, container, containerName, ranges);
        final Iterator<Integer> it = ranges.iterator();
        while (it.hasNext()) {
            final int start = it.next();
            assertTrue(it.hasNext());
            final int end = it.next();
            container = container.iappend(start, end);
        }
        assertEquals(containerName, container.getContainerName());
        assertEquals(expected.getCardinality(), container.getCardinality());
        TestContainerBase.assertSameContents(expected, container);
    }

    static void doTestReverseIteratorAdvance(final Supplier<Container> containerFactory,
        final String containerName) {
        for (int vi = 0; vi < vss.length; ++vi) {
            doTestReverseIteratorAdvance(vi, containerFactory.get(), containerName);
        }
        doTestReverseIteratorAdvanceEmpty(containerFactory.get(), containerName);
    }

    private static void doTestReverseIteratorAdvance(final int vi, final Container container,
        final String containerName) {
        final int[] vs = vss[vi];
        final String m = "vi==" + vi;
        ArrayList<Integer> ranges = new ArrayList<>(2 * vs.length);
        final Container c = populate(vs, container, containerName, ranges);
        final Iterator<Integer> it = ranges.iterator();
        long prevLast = -1;
        while (it.hasNext()) {
            final int start = it.next();
            assertTrue(it.hasNext());
            final int end = it.next();
            for (int v = start - 1; v <= end; ++v) {
                if (v < 0) {
                    continue;
                }
                final String m2 = m + " && v==" + v;
                final ShortAdvanceIterator sai = c.getReverseShortIterator();
                final boolean r = sai.advance(v);
                if (start <= v && v < end) {
                    assertTrue(m2, r);
                    assertEquals(m2, v, sai.currAsInt());
                } else if (v == start - 1) {
                    if (prevLast == -1) {
                        assertFalse(m2, r);
                    } else {
                        assertTrue(m2, r);
                        assertEquals(prevLast, sai.currAsInt());
                    }
                } else { // v == end
                    assertTrue(m2, r);
                    assertEquals(end - 1, sai.currAsInt());
                }
            }
            prevLast = end - 1;
        }
    }

    private static void doTestReverseIteratorAdvanceEmpty(final Container container,
        final String containerName) {
        final ShortAdvanceIterator reverseIter = container.getReverseShortIterator();
        assertFalse(reverseIter.advance(1));
    }

    static void doTestAddRange(final Supplier<Container> containerFactory,
        final String containerName) {
        Container c = containerFactory.get();
        c = c.iadd(5, 7);
        c = c.iadd(8, 10);
        final int min = 3;
        final int max = 12;
        for (int first = min; first <= max; ++first) {
            for (int last = first; last <= max; ++last) {
                final String m = "first==" + first + " && last==" + last;
                final Container addRangeResult = c.andRange(first, last + 1);
                final Container range = new RunContainer(first, last + 1);
                final Container expected = range.and(c);
                assertEquals(m, expected.getCardinality(), addRangeResult.getCardinality());
                assertTrue(m, expected.subsetOf(addRangeResult));
            }
        }
    }

    static void doTestAndRange(final Supplier<Container> containerFactory,
        final String containerName) {
        for (int i = 0; i < vss.length; ++i) {
            doTestAndRange(i, containerFactory.get(), containerName);
        }
    }

    private static final int nruns = 1000;

    private static void doTestAndRange(
        final int vi, Container container, final String containerName) {
        final int[] vs = vss[vi];
        final String m = "vi==" + vi;
        final Container c = populate(vs, container, containerName, null);
        final Random rand = new Random(seed0);
        for (int i = 0; i < nruns; ++i) {
            final String m2 = m + " && i==" + i;
            final int first = rand.nextInt(65535);
            final int last = first + rand.nextInt(65536 - first);
            final Container r = c.andRange(first, last + 1);
            final Container expected = c.and(Container.rangeOfOnes(first, last + 1));
            assertEquals(m2, expected.getCardinality(), r.getCardinality());
            assertTrue(m2, expected.subsetOf(r));
        }
    }
}

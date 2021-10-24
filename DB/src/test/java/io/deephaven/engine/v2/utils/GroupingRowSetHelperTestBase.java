/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.v2.utils;

import io.deephaven.base.Pair;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.v2.sources.chunk.Attributes;
import io.deephaven.engine.v2.sources.chunk.Attributes.OrderedRowKeys;
import io.deephaven.engine.v2.sources.chunk.LongChunk;
import io.deephaven.engine.v2.sources.chunk.WritableLongChunk;
import io.deephaven.engine.v2.utils.rsp.RspArray;
import io.deephaven.engine.v2.utils.rsp.RspBitmap;
import io.deephaven.engine.v2.utils.singlerange.SingleRange;
import io.deephaven.engine.v2.utils.sortedranges.SortedRanges;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.list.TLongList;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.set.hash.TLongHashSet;
import io.deephaven.test.types.OutOfBandTest;
import junit.framework.TestCase;
import org.jetbrains.annotations.NotNull;
import io.deephaven.engine.v2.utils.rsp.container.ArrayContainer;

import java.util.Arrays;
import java.util.Random;
import java.util.function.LongConsumer;
import java.util.function.Supplier;
import java.util.stream.LongStream;
import org.junit.experimental.categories.Category;

import static java.lang.Math.*;

@SuppressWarnings("ForLoopReplaceableByForEach")
@Category(OutOfBandTest.class)
public abstract class GroupingRowSetHelperTestBase extends TestCase {

    private final long[][] KEYS = new long[][] {
            new long[0], {0L}, {1L},
            {0L, 1L, 2L},
            {0L, 1L, 2L, 4L},
            {0L, 2L, 4L},
            {0L, 2L, 4L, 5L},
            {0L, 2L, 3L, 4L, 5L},
            {0L, 1L, 2L, 3L, 4L, 5L},
            {0L, 2L, 3L, 4L, 5L, 10L},
            {0L, 2L, 3L, 4L, 5L, 8L, 9L, 10L},
            {0L, 2L, 3L, 4L, 5L, 8L, 9L, 10L, 20L},
            {2L, 3L, 4L, 5L, 8L, 9L, 10L, 20L}};

    public void testFind() {
        for (long[] keys : KEYS) {
            testFind(keys);
        }
    }

    private void testFind(long... keys) {
        final TrackingMutableRowSet rowSet = getSortedIndex(keys);
        for (int i = 0; i < keys.length; i++) {
            try {
                assertEquals(i, rowSet.find(keys[i]));
            } catch (Throwable t) {
                rowSet.find(keys[i]);
            }
            if (i < keys.length - 1) {
                for (long j = keys[i] + 1; j < keys[i + 1]; j++) {
                    try {
                        assertEquals(-i - 2, rowSet.find(j));
                    } catch (Throwable t) {
                        rowSet.find(j);
                    }
                }
            }
        }
        if (keys.length > 0) {
            if (keys[0] > 0) {
                try {
                    assertEquals(-1, rowSet.find(keys[0] - 1));
                } catch (Throwable t) {
                    rowSet.find(keys[0] - 1);
                }
            }
            try {
                assertEquals(-keys.length - 1, rowSet.find(keys[keys.length - 1] + 1));
            } catch (Throwable t) {
                rowSet.find(keys[keys.length - 1] + 1);
            }
        } else {
            assertEquals(-1, rowSet.find(10));
        }

    }

    public void testInvert() {
        final TrackingMutableRowSet rowSet = getSortedIndex(1, 4, 7, 9, 10);
        final TrackingMutableRowSet inverted = rowSet.invert(getSortedIndex(4, 7, 10));
        System.out.println("Inverted: " + inverted);
        compareIndexAndKeyValues(inverted, new long[] {1, 2, 4});

        final int maxSize = 20000;
        final int iterations = 400;


        for (int iteration = 0; iteration < iterations; ++iteration) {
            final long seed = 42 + iteration;
            final Random generator = new Random(seed);

            final long[] fullKeys = generateFullKeys(maxSize, generator);
            final TrackingMutableRowSet fullRowSet = getSortedIndex(fullKeys);

            final Pair<TrackingMutableRowSet, TLongList> pp = generateSubset(fullKeys, fullRowSet, Integer.MAX_VALUE, generator);
            final TrackingMutableRowSet subsetRowSet = pp.first;
            final TLongList expected = pp.second;
            TestCase.assertEquals(subsetRowSet.size(), expected.size());

            final TrackingMutableRowSet invertedRowSet = fullRowSet.invert(subsetRowSet);
            TestCase.assertEquals(subsetRowSet.size(), invertedRowSet.size());
            TestCase.assertEquals(expected.size(), invertedRowSet.size());

            for (int ii = 0; ii < invertedRowSet.intSize(); ++ii) {
                final long expectedPosition = expected.get(ii);
                final long actualPosition = invertedRowSet.get(ii);
                TestCase.assertEquals(expectedPosition, actualPosition);
            }
        }
    }


    public void testInvertWithMax() {
        final TrackingMutableRowSet rowSet = getSortedIndex(1, 4, 7, 9, 10);
        final TrackingMutableRowSet inverted = rowSet.invert(getSortedIndex(4, 7, 10), 3);
        System.out.println("Inverted: " + inverted);
        compareIndexAndKeyValues(inverted, new long[] {1, 2});

        final int maxSize = 20000;
        final int iterations = 100;

        for (int iteration = 0; iteration < iterations; ++iteration) {
            final long seed = 42 + iteration;
            final Random generator = new Random(seed);

            final long[] fullKeys = generateFullKeys(maxSize, generator);
            final TrackingMutableRowSet fullRowSet = getSortedIndex(fullKeys);
            final int maxPosition = generator.nextInt(fullRowSet.intSize());
            final Pair<TrackingMutableRowSet, TLongList> pp = generateSubset(fullKeys, fullRowSet, maxPosition, generator);
            final TrackingMutableRowSet subsetRowSet = pp.first;
            final TLongList expected = pp.second;

            final TrackingMutableRowSet invertedRowSet = fullRowSet.invert(subsetRowSet, maxPosition);

            TestCase.assertEquals("iteration=" + iteration, expected.size(), invertedRowSet.size());

            for (int ii = 0; ii < invertedRowSet.intSize(); ++ii) {
                final long expectedPosition = expected.get(ii);
                final long actualPosition = invertedRowSet.get(ii);
                TestCase.assertEquals(expectedPosition, actualPosition);
            }
        }
    }

    private long[] generateFullKeys(@SuppressWarnings("SameParameterValue") int maxSize, Random generator) {
        final long[] fullKeys;
        switch (generator.nextInt(2)) {
            case 0:
                fullKeys = generateKeysMethod1(maxSize, generator);
                break;
            case 1:
                fullKeys = generateKeysMethod2(maxSize, generator);
                break;
            default:
                throw new UnsupportedOperationException();
        }
        return fullKeys;
    }

    /**
     * This generates random keys from [0, maxsize), randomly flipping the ranges on and off.
     */
    private long[] generateKeysMethod1(int maxSize, Random generator) {
        final boolean[] fullSet = new boolean[maxSize];
        final int rangeCount = generator.nextInt(1000);
        for (int ii = 0; ii < rangeCount; ++ii) {
            final boolean value = generator.nextBoolean();
            final int rangeStart = generator.nextInt(maxSize);
            final int rangeEnd = rangeStart + generator.nextInt(maxSize - rangeStart);
            for (int jj = rangeStart; jj < rangeEnd; ++jj)
                fullSet[ii] = value;
        }

        return booleanSetToKeys(fullSet);
    }

    /**
     * This version picks a size, and generates ranges until we exceed that size
     */
    private long[] generateKeysMethod2(int maxSize, Random generator) {
        final TLongArrayList keys = new TLongArrayList();
        long lastKey = 0;

        while (keys.size() < maxSize) {
            final long skip = generator.nextInt(1000);
            if (skip == 0) {
                continue;
            }
            lastKey += skip;
            final long count = generator.nextInt(1000);
            for (long ll = lastKey; ll < lastKey + count; ++ll) {
                keys.add(ll);
            }
            lastKey += count;
        }

        return keys.toArray();
    }

    /**
     * Generate a subset of the keys in fullKeys up to maxPosition positions in using generator. Returns a pair
     * containing the subset of fullKeys as an TrackingMutableRowSet and the expected positions as a TLongList.
     */
    private Pair<TrackingMutableRowSet, TLongList> generateSubset(long[] fullKeys, TrackingMutableRowSet fullRowSet, int maxPosition, Random generator) {
        switch (generator.nextInt(2)) {
            case 0:
                return generateSubsetMethod1(fullKeys, fullRowSet, maxPosition, generator);
            case 1:
                return generateSubsetMethod2(fullKeys, fullRowSet, maxPosition, generator);
            default:
                throw new UnsupportedOperationException();
        }
    }

    /**
     * For each key, randomly flip a count as to whether it belongs in the output.
     */
    private Pair<TrackingMutableRowSet, TLongList> generateSubsetMethod1(long[] fullKeys, @SuppressWarnings("unused") TrackingMutableRowSet fullRowSet,
                                                                         int maxPosition, Random generator) {
        final boolean subset[] = new boolean[(int) fullRowSet.lastRowKey() + 1];

        final double density = generator.nextDouble();

        final TLongList expected = new TLongArrayList();
        int included = 0;
        int included2 = 0;
        for (int ii = 0; ii < fullKeys.length; ++ii) {
            if (generator.nextDouble() < density) {
                if (ii <= maxPosition) {
                    expected.add(ii);
                    included++;
                }
                assertFalse(subset[(int) fullKeys[ii]]);
                subset[(int) fullKeys[ii]] = true;
                included2++;
            }
        }

        final long[] subsetKeys = booleanSetToKeys(subset);
        assertEquals(included2, subsetKeys.length);

        final TrackingMutableRowSet subsetRowSet = getSortedIndex(subsetKeys);

        assertEquals(subsetKeys.length, subsetRowSet.size());
        assertEquals(included, expected.size());
        if (maxPosition >= fullRowSet.size()) {
            assertEquals(included, included2);
            assertEquals(subsetRowSet.size(), expected.size());
        }

        return new Pair<>(subsetRowSet, expected);
    }

    /**
     * For each run of the rowSet, flip a coin to determine if it is included; then randomly select a start and end
     * within each range.
     */
    private Pair<TrackingMutableRowSet, TLongList> generateSubsetMethod2(@SuppressWarnings("unused") long[] fullKeys, TrackingMutableRowSet fullRowSet,
                                                                         int maxPosition, Random generator) {
        final boolean subset[] = new boolean[(int) fullKeys[fullKeys.length - 1] + 1];

        final TLongList expected = new TLongArrayList();
        long runningPosition = 0;

        final double inclusionThreshold = generator.nextDouble();
        for (final TrackingMutableRowSet.RangeIterator rit = fullRowSet.rangeIterator(); rit.hasNext();) {
            rit.next();

            final long rangeSize = rit.currentRangeEnd() - rit.currentRangeStart() + 1;
            if (generator.nextDouble() < inclusionThreshold) {
                // figure out a start and an end

                final int start = generator.nextInt((int) rangeSize);
                if (start == rangeSize) {
                    continue;
                }
                final int end = generator.nextInt((int) rangeSize - start);

                for (int ii = start; ii < start + end; ++ii) {
                    final long position = runningPosition + (ii - start);
                    if (position <= maxPosition) {
                        expected.add(position);
                    }
                    subset[(int) fullKeys[(int) position]] = true;
                }
            }

            runningPosition += rangeSize;
        }

        final long[] subsetKeys = booleanSetToKeys(subset);

        final TrackingMutableRowSet subsetRowSet = getSortedIndex(subsetKeys);

        System.out.println(subsetRowSet);

        return new Pair<>(subsetRowSet, expected);
    }

    public void testIteration() {
        for (long[] key : KEYS) {
            testIteration(key);
        }
    }

    public void testInsertion() {
        for (long[] keys : KEYS) {
            testInsertionAlreadyThere(keys);
            testInsertionNotThere(keys);
        }
    }

    private void testInsertionAlreadyThere(long... keys) {
        final TrackingMutableRowSet rowSet = getSortedIndex(keys);
        for (int i = 0; i < keys.length; i++) {
            final long preSize = rowSet.size();
            rowSet.insert(keys[i]);
            final long postSize = rowSet.size();
            assertEquals(preSize, postSize);
        }
        compareIndexAndKeyValues(rowSet, keys);
    }

    private void testInsertionNotThere(long... keys) {
        final TLongHashSet notThere = new TLongHashSet();
        for (int i = 0, key = 0; i < keys.length;) {
            if (key > keys[i]) {
                i++;
                continue;
            }
            if (key != keys[i]) {
                notThere.add(key);
            }
            key++;
        }
        if (keys.length > 0) {
            notThere.add(keys[keys.length - 1] + 1);
            notThere.add(keys[keys.length - 1] + 2);
        } else {
            notThere.add(0);
            notThere.add(1);
            notThere.add(10);
        }
        for (final TLongIterator iterator = notThere.iterator(); iterator.hasNext();) {
            final TrackingMutableRowSet rowSet = getSortedIndex(keys);
            final long key = iterator.next();
            rowSet.insert(key);
            final TLongArrayList al = new TLongArrayList(keys);
            al.add(key);
            al.sort();
            compareIndexAndKeyValues(rowSet, al.toArray());
        }
        for (int i = 1; i < notThere.size() + 1; i++) {
            TrackingMutableRowSet rowSet = getSortedIndex(keys);
            int steps = 0;
            TLongArrayList al = new TLongArrayList(keys);
            for (final TLongIterator iterator = notThere.iterator(); iterator.hasNext();) {
                if (steps % i == 0) {
                    al = new TLongArrayList(keys);
                    rowSet = getSortedIndex(keys);
                }
                final long key = iterator.next();
                rowSet.insert(key);
                al.add(key);
                al.sort();
                compareIndexAndKeyValues(rowSet, al.toArray());
                steps++;
            }
        }
    }

    public void testRangeByPos() {
        for (long[] keys : KEYS) {
            testRangeByPos(keys);
        }
    }

    private void testRangeByPos(long... keys) {
        final TrackingMutableRowSet rowSet = getSortedIndex(keys);
        for (int i = 0; i < keys.length + 2; i++) {
            for (int j = i; j < keys.length + 3; j++) {
                final int start = min(i, keys.length);
                final long[] range = Arrays.copyOfRange(keys, start, max(start, min(j, keys.length)));
                final TrackingMutableRowSet subRowSet = rowSet.subSetByPositionRange(i, j);
                try {
                    compareIndexAndKeyValues(subRowSet, range);
                } catch (AssertionError assertionError) {
                    System.err.println("rowSet=" + rowSet + ", subRowSet=" + subRowSet + ", i=" + i + ", j=" + j);
                    throw assertionError;
                }
            }
        }

    }

    public void testRangeByKey() {
        for (int i = 0; i < KEYS.length; ++i) {
            final long[] keys = KEYS[i];
            final String m = "i==" + i;
            testRangeByKey(m, keys);
        }
    }

    public void testMinusSimple() {
        final long[] keys = {1, 2, 3};

        TrackingMutableRowSet rowSet = getSortedIndex(keys);
        TrackingMutableRowSet result = rowSet.minus(getFactory().getEmptyRowSet());
        compareIndexAndKeyValues(result, keys);

        result = rowSet.minus(rowSet);
        compareIndexAndKeyValues(result, new long[] {});

        long[] subKeys = {2, 5};
        TrackingMutableRowSet subRowSet = getSortedIndex(subKeys);

        result = rowSet.minus(subRowSet);
        compareIndexAndKeyValues(result, new long[] {1, 3});


        final long[] allKeys = new long[105339];
        for (int ii = 0; ii < 105339; ++ii) {
            allKeys[ii] = ii;
        }
        rowSet = getSortedIndex(allKeys);
        System.out.println(rowSet);

        result = rowSet.minus(subRowSet);
        compareIndexAndKeyValues(result, doMinusSimple(allKeys, subKeys));

        subKeys = stringToKeys(
                "0-12159,12162-12163,12166-12167,12172-12175,12178-12179,12182-12325,12368-33805,33918-33977,33980-34109,34168-34169,34192-34193,34309-34312,34314,34317-34323,34356-34491,34494-34495,34502-34503,34506-34509,34512-34515,34520-34521,34524-34525,34528-34529,34540-34541,34544-34545,34548-34549,34552-34553,34574-34589,34602-34675,34678-34679,34688-34689,34694-34695,34700-34705,34716-34717,34722-34723,34732-34733,34738-34739,34774,34785,34791-34794,34796-34799,34801-34803,34807-34808,34813,34816,34828-34829,34856-34857,34869,34875-34884,34892-34899,34902-34925,34930-34932,34934-34938,34958-34959,34966-34973,35038-35065,35068-35075,35212-35363,35496-35511,35542-44097,44104-54271,54291,54304,54308-54310,54373-54749,54751-54756,54758-55040,55112,55114-55115,55117,55120-55213,55321-55322,55325-55326,55627,55630-55631,55634-55635,55638,55640-55643,55646-55647,55650-55651,55654-55655,55658-55659,55661-55690,55692-55698,55702-55710,55712-55713,55716-55717,55719-55960,56059-56134,56185-56186,56255-56257,56259,56341-56628,56695-56866,56878-56880,56882-57082,57105-65108,64977-66622,66625-66658,66661-66662,66665-66668,66671-66834,66837-66840");
        subRowSet = getSortedIndex(subKeys);
        result = rowSet.minus(subRowSet);
        compareIndexAndKeyValues(result, doMinusSimple(allKeys, subKeys));
    }

    public void testUnionIntoFullLeaf() {
        final RowSetBuilder rowSetBuilder1 = getFactory().getBuilder();
        for (int ii = 0; ii < 4; ++ii) {
            rowSetBuilder1.addRange(ii * 128, ii * 128 + 64);
        }

        long start = 8192;
        // Leave some room, so that we can go back and fill in the right node
        for (int ii = 0; ii < 16; ++ii) {
            rowSetBuilder1.addRange(start + ii * 3, start + ii * 3 + 1);
        }

        // This, actually forces the split. We'll have short nodes (rather than ints) with the packing, because this
        // range is less than 2^15.
        rowSetBuilder1.addRange(32000, 32001);

        // Now we fill in the ranges in the first node to make it full.
        for (int ii = 0; ii < 4 - 1; ++ii) {
            rowSetBuilder1.addRange((16 + ii) * 128, (16 + ii) * 127 + 64);
        }

        start = 8192 + 64;
        // And lets fill in most of the ranges in the second node.
        for (int ii = 0; ii < 18; ++ii) {
            rowSetBuilder1.addRange(start + ii * 3, start + ii * 3 + 1);
        }

        rowSetBuilder1.addRange(8260, 33000);

        final TrackingMutableRowSet idx = rowSetBuilder1.build();

        // Now try to force an overflow.
        final RowSetBuilder rowSetBuilder2 = getFactory().getBuilder();
        rowSetBuilder2.addRange(7900, 8265);
        final TrackingMutableRowSet idx2 = rowSetBuilder2.build();

        System.out.println(idx);
        System.out.println(idx2);

        idx.insert(idx2);

        System.out.println(idx);

        idx.validate();
    }

    public void testFunnyOverLap() {
        // doTestFunnyOverlap("0-12159,12162-12163,12166-12167,12172-12175,12178-12179,12182-12325,12368-33805,33918-33977,33980-34109,34168-34169,34192-34193,34309-34312,34314,34317-34323,34356-34491,34494-34495,34502-34503,34506-34509,34512-34515,34520-34521,34524-34525,34528-34529,34540-34541,34544-34545,34548-34549,34552-34553,34574-34589,34602-34675,34678-34679,34688-34689,34694-34695,34700-34705,34716-34717,34722-34723,34732-34733,34738-34739,34774,34785,34791-34794,34796-34799,34801-34803,34807-34808,34813,34816,34828-34829,34856-34857,34869,34875-34884,34892-34899,34902-34925,34930-34932,34934-34938,34958-34959,34966-34973,35038-35065,35068-35075,35212-35363,35496-35511,35542-44097,44104-54271,54291,54304,54308-54310,54373-54749,54751-54756,54758-55040,55112,55114-55115,55117,55120-55213,55321-55322,55325-55326,55627,55630-55631,55634-55635,55638,55640-55643,55646-55647,55650-55651,55654-55655,55658-55659,55661-55690,55692-55698,55702-55710,55712-55713,55716-55717,55719-55960,56059-56134,56185-56186,56255-56257,56259,56341-56628,56695-56866,56878-56880,56882-57082,57105-65108,64977-66622,66625-66658,66661-66662,66665-66668,66671-66834,66837-66840");
        doTestFunnyOverlap(
                "0-6509,6510-6619,6620-17383,17384-18031,18158-47065,47082-47099,47104-47593,47616-56079,56080-71737,71858-83613,83616-83701,83719,83721-83749,83752-83761,83764,83769-86307,86308-87746,87762-87770,87774-87841,87845-87847,87853-87878,87880,87882-87933,87936-87950,87954-87956,87958-87967,87972-87980,87982,87984-87988,87991-88137,88139-88140,88167-88198,88228,88231-88289,88293,88299-88362,88364,88378-88381,88388-88389,88394-88395,88398-88399,88402-88405,88408-88415,88420-88427,88430-88437,88440-88441,88519,88521-88588,88597-92547,92672-93207,93224-95745,95630-102119,102284-106111,106124-106125,106134-106135,106137-106141,106157-106173,106323-106326,106330-106377,106379-106380,106382-106384,106386,106390-106395,106454-106665,106788-106855,106932-108809,108830-113235,113420-113547,113580-113587,113596-113643,113646-113771");
    }

    private void doTestFunnyOverlap(@SuppressWarnings("SameParameterValue") String input) {
        final RowSetBuilder rowSetBuilder1 = getFactory().getBuilder();

        final TLongArrayList keyList = new TLongArrayList();

        final String[] splitInput = input.split(",");
        for (String range : splitInput) {
            final int dash = range.indexOf("-");
            if (dash > 0) {
                final String strStart = range.substring(0, dash);
                final String strEnd = range.substring(dash + 1);
                final long start = Long.parseLong(strStart);
                final long end = Long.parseLong(strEnd);

                rowSetBuilder1.addRange(start, end);

            } else {
                rowSetBuilder1.addKey(Long.parseLong(range));
            }
        }

        final TrackingMutableRowSet rowSet1 = rowSetBuilder1.build();
        rowSet1.validate();

        final RowSetBuilder rowSetBuilder2 = getFactory().getBuilder();

        for (String range : splitInput) {
            final int dash = range.indexOf("-");
            if (dash > 0) {
                final String strStart = range.substring(0, dash);
                final String strEnd = range.substring(dash + 1);
                final long start = Long.parseLong(strStart);
                final long end = Long.parseLong(strEnd);

                for (long key = start; key <= end; ++key) {
                    rowSetBuilder2.addKey(key);
                    keyList.add(key);
                }
            } else {
                final long key = Long.parseLong(range);
                rowSetBuilder2.addKey(key);
                keyList.add(key);
            }
        }

        final TrackingMutableRowSet rowSet2 = rowSetBuilder2.build();
        rowSet2.validate();

        // Try inserting them in a random order
        for (int iterations = 0; iterations < 10; ++iterations) {
            final int seed = 100042 + iterations * 10;
            final Random random = new Random(seed);
            System.out.println("Seed: " + seed);

            for (int ii = keyList.size() - 1; ii > 0; ii--) {
                final int jj = random.nextInt(ii);
                final long newKey = keyList.get(jj);
                final long oldKey = keyList.get(ii);
                keyList.set(jj, oldKey);
                keyList.set(ii, newKey);
            }
            final RowSetBuilder rowSetBuilder3 = getFactory().getBuilder();
            for (int ii = 0; ii < keyList.size(); ++ii) {
                rowSetBuilder3.addKey(keyList.get(ii));
            }
            final TrackingMutableRowSet rowSet3 = rowSetBuilder3.build();
            rowSet3.validate();
        }
    }

    public void testMinusRandom() {
        final int maxSize = 128 * 1024;
        final boolean[] fullSet = new boolean[maxSize];
        final boolean[] subSet = new boolean[maxSize];

        final long seed = 42;
        System.out.println("Seed: " + seed);

        final Random generator = new Random(seed);
        // initialize the arrays
        for (int ii = 0; ii < maxSize; ++ii) {
            fullSet[ii] = generator.nextBoolean();
            subSet[ii] = generator.nextBoolean();
        }

        for (int run = 0; run < 50; ++run) {
            final String m = "run==" + run;
            int flipCount = generator.nextInt(maxSize);
            for (int ii = 0; ii < flipCount; ++ii) {
                fullSet[ii] = fullSet[ii] ^ generator.nextBoolean();
            }

            flipCount = generator.nextInt(maxSize);
            for (int ii = 0; ii < flipCount; ++ii) {
                subSet[ii] = subSet[ii] ^ generator.nextBoolean();
            }

            final long[] fullKeys = booleanSetToKeys(fullSet);
            final long[] subKeys = booleanSetToKeys(subSet);

            final TrackingMutableRowSet rowSet = getSortedIndex(fullKeys);
            compareIndexAndKeyValues(m, rowSet, fullKeys);
            final TrackingMutableRowSet subRowSet = getSortedIndex(subKeys);
            compareIndexAndKeyValues(m, subRowSet, subKeys);

            final TrackingMutableRowSet result = rowSet.minus(subRowSet);
            compareIndexAndKeyValues(m, result, doMinusSimple(fullKeys, subKeys));
        }
    }

    public void testMinusRandomRanges() {
        final int maxSize = 128 * 1024;
        final boolean[] fullSet = new boolean[maxSize];
        final boolean[] subSet = new boolean[maxSize];

        final long seed = 42;
        System.out.println("Seed: " + seed);

        final Random generator = new Random(seed);
        // initialize the arrays
        for (int ii = 0; ii < maxSize; ++ii) {
            fullSet[ii] = generator.nextBoolean();
            subSet[ii] = generator.nextBoolean();
        }

        for (int iterations = 0; iterations < 50; ++iterations) {
            System.out.println("Iteration: " + iterations);
            int rangeCount = generator.nextInt(100);
            for (int ii = 0; ii < rangeCount; ++ii) {
                final boolean value = generator.nextBoolean();
                final int rangeStart = generator.nextInt(maxSize);
                final int rangeEnd = rangeStart + generator.nextInt(maxSize - rangeStart);
                for (int jj = rangeStart; jj < rangeEnd; ++jj)
                    fullSet[ii] = value;
            }

            rangeCount = generator.nextInt(100);
            for (int ii = 0; ii < rangeCount; ++ii) {
                final boolean value = generator.nextBoolean();
                final int rangeStart = generator.nextInt(maxSize);
                final int rangeEnd = rangeStart + generator.nextInt(maxSize - rangeStart);
                for (int jj = rangeStart; jj < rangeEnd; ++jj)
                    subSet[ii] = value;
            }

            final long[] fullKeys = booleanSetToKeys(fullSet);
            final long[] subKeys = booleanSetToKeys(subSet);

            final TrackingMutableRowSet rowSet = getSortedIndex(fullKeys);
            final TrackingMutableRowSet subRowSet = getSortedIndex(subKeys);

            final TrackingMutableRowSet result = rowSet.minus(subRowSet);
            compareIndexAndKeyValues(result, doMinusSimple(fullKeys, subKeys));
        }
    }

    public void testMinusIndexOps() {
        final int maxSize = 64 * 1024;
        final int numRanges = 10;
        final boolean[] fullSet = new boolean[maxSize];
        final boolean[] subSet = new boolean[maxSize];

        final long seed = 42;
        final String m1 = "seed==" + seed;

        final Random generator = new Random(seed);
        // initialize the arrays
        for (int ii = 0; ii < maxSize; ++ii) {
            fullSet[ii] = generator.nextBoolean();
            subSet[ii] = generator.nextBoolean();
        }

        final TrackingMutableRowSet fullRowSet = getSortedIndex(booleanSetToKeys(fullSet));
        final TrackingMutableRowSet subRowSet = getSortedIndex(booleanSetToKeys(subSet));

        for (int iteration = 0; iteration < 100; ++iteration) {
            final String m2 = m1 + " && iteration==" + iteration;
            int rangeCount = generator.nextInt(numRanges);
            for (int ii = 0; ii < rangeCount; ++ii) {
                final String m3 = m2 + " && ii=" + ii;
                final boolean value = generator.nextBoolean();
                final int rangeStart = generator.nextInt(maxSize);
                final int rangeEnd = rangeStart + generator.nextInt(maxSize - rangeStart);

                if (value) {
                    fullRowSet.insertRange(rangeStart, rangeEnd);
                    assertTrue(m3, fullRowSet.containsRange(rangeStart, rangeEnd));
                    for (int jj = rangeStart; jj <= rangeEnd; ++jj) {
                        fullSet[jj] = true;
                    }
                } else {
                    for (int jj = rangeStart; jj <= rangeEnd; ++jj) {
                        fullRowSet.remove(jj);
                        assertFalse(m3 + " && jj==" + jj, fullRowSet.find(jj) >= 0);
                        fullSet[jj] = false;
                    }
                }
            }

            rangeCount = generator.nextInt(numRanges);
            for (int ii = 0; ii < rangeCount; ++ii) {
                final String m3 = m2 + " && ii==" + ii;
                final boolean value = generator.nextBoolean();
                final int rangeStart = generator.nextInt(maxSize);
                final int rangeEnd = rangeStart + generator.nextInt(maxSize - rangeStart);

                if (value) {
                    subRowSet.insertRange(rangeStart, rangeEnd);
                    assertTrue(m3, subRowSet.containsRange(rangeStart, rangeEnd));
                    for (int jj = rangeStart; jj <= rangeEnd; ++jj) {
                        subSet[jj] = true;
                    }
                } else {
                    for (int jj = rangeStart; jj <= rangeEnd; ++jj) {
                        subSet[jj] = false;
                        subRowSet.remove(jj);
                        assertFalse(m3 + " && jj==" + jj, subRowSet.find(jj) >= 0);
                    }
                }
            }

            final TrackingMutableRowSet result = fullRowSet.minus(subRowSet);

            compareIndexAndKeyValues(m2, result, doMinusSimple(booleanSetToKeys(fullSet), booleanSetToKeys(subSet)));
        }
    }

    private long[] booleanSetToKeys(boolean[] fullSet) {
        final TLongArrayList resultArray = new TLongArrayList();
        for (int ii = 0; ii < fullSet.length; ++ii) {
            if (fullSet[ii]) {
                resultArray.add(ii);
            }
        }
        return resultArray.toArray();
    }

    private long[] stringToKeys(@SuppressWarnings("SameParameterValue") String input) {
        final TLongArrayList resultArrayList = new TLongArrayList();
        final String[] splitInput = input.split(",");
        for (String range : splitInput) {
            final int dash = range.indexOf("-");
            if (dash > 0) {
                final String strStart = range.substring(0, dash);
                final String strEnd = range.substring(dash + 1);
                final long start = Long.parseLong(strStart);
                final long end = Long.parseLong(strEnd);

                for (long ii = start; ii <= end; ++ii) {
                    resultArrayList.add(ii);
                }
            } else {
                resultArrayList.add(Long.parseLong(range));
            }
        }
        return resultArrayList.toArray();
    }

    private long[] doMinusSimple(long[] allKeys, long[] subKeys) {
        final TLongArrayList resultArrayList = new TLongArrayList();
        final TLongHashSet longHashSet = new TLongHashSet(subKeys);
        for (int ii = 0; ii < allKeys.length; ++ii) {
            if (!longHashSet.contains(allKeys[ii])) {
                resultArrayList.add(allKeys[ii]);
            }
        }
        return resultArrayList.toArray();
    }

    private void testRangeByKey(final String m, long... keys) {
        final TrackingMutableRowSet rowSet = getSortedIndex(keys);
        for (long i = (keys.length > 0 ? keys[0] - 2 : 1); i < (keys.length > 0 ? keys[keys.length - 1] : 0) + 3; i++) {
            for (long j = i; j < (keys.length > 0 ? keys[keys.length - 1] : 0) + 3; j++) {
                final TLongArrayList data = new TLongArrayList();
                for (int k = 0; k < keys.length; k++) {
                    final long key = keys[k];
                    if (key >= i && key <= j) {
                        data.add(key);
                    }
                }
                final long[] range = data.toArray();
                final TrackingMutableRowSet subRowSet = rowSet.subSetByKeyRange(i, j);
                try {
                    compareIndexAndKeyValues(m, subRowSet, range);
                } catch (AssertionError assertionError) {
                    System.err.println("rowSet=" + rowSet + ", subRowSet=" + subRowSet + ", i=" + i + ", j=" + j);
                    throw assertionError;
                }
            }
        }
    }


    private void testIteration(long... keys) {
        final TrackingMutableRowSet rowSet = getSortedIndex(keys);
        compareIndexAndKeyValues(rowSet, keys);
    }

    private static final boolean debugDetail = false;

    private static String a2s(final long[] vs) {
        if (!debugDetail) {
            return "[... size=" + vs.length + "]";
        }
        final StringBuilder sb = new StringBuilder("[");
        boolean first = true;
        for (long v : vs) {
            if (!first) {
                sb.append(", ");
            }
            sb.append(v);
            first = false;
        }
        sb.append("]");
        return sb.toString();
    }

    private void compareIndexAndKeyValues(final TrackingMutableRowSet rowSet, final long[] keys) {
        compareIndexAndKeyValues("", rowSet, keys);
    }

    private void compareIndexAndKeyValues(final String pfx, final TrackingMutableRowSet rowSet, final long[] keys) {
        final String m =
                ((pfx != null && pfx.length() > 0) ? pfx + " " : "") + "rowSet=" + rowSet + ", keys=" + a2s(keys);
        final GroupingRowSetHelper.SearchIterator iterator = rowSet.searchIterator();
        for (int i = 0; i < keys.length; i++) {
            assertTrue(m + " iterator shouldbn't be empty", iterator.hasNext());
            final long next = iterator.nextLong();
            final String msg = m + " key mismatch i=" + i;
            assertEquals(msg, keys[i], next);
            assertEquals(msg, keys[i], iterator.currentValue());
            assertEquals(msg, keys[i], rowSet.get(i));
        }
        assertFalse(m + " iterator should be empty", iterator.hasNext());
        assertEquals(m + " length mismatch", keys.length, rowSet.size());

        final GroupingRowSetHelper.SearchIterator reverse = rowSet.reverseIterator();
        for (int i = 0; i < keys.length; i++) {
            assertTrue(m + ", i=" + i, reverse.hasNext());
            final long next = reverse.nextLong();
            assertEquals(keys[keys.length - i - 1], next);
            assertEquals(keys[keys.length - i - 1], reverse.currentValue());
        }
        assertFalse(reverse.hasNext());

        int i = 0;
        for (long checkKey : rowSet) {
            assertEquals(keys[i++], checkKey);
        }

        rowSet.iterator().forEachRemaining(new LongConsumer() {
            int ai = 0;

            @Override
            public void accept(long value) {
                assertEquals(keys[ai++], value);
            }
        });
    }

    public void testRandomInsertMinus() {
        final int printInterval = 100;
        final int maxRange = 20;
        final int maxValue = 1 << 24;

        final TrackingMutableRowSet check = getFactory().getEmptyRowSet();

        final Random random = new Random(1);

        final long startTime = System.currentTimeMillis();

        for (int ii = 0; ii < 500; ++ii) {
            if (ii % printInterval == printInterval - 1) {
                System.out.println(ii + ": " + (System.currentTimeMillis() - startTime) + "ms: " + check);
            }

            final RowSetBuilder builder = getFactory().getRandomBuilder();
            for (int jj = 0; jj < 128; ++jj) {
                final int start = random.nextInt(maxValue);
                final int end = start + random.nextInt(maxRange);
                builder.addRange(start, end);
            }

            final String m = "ii=" + ii;

            final TrackingMutableRowSet operand = builder.build();
            operand.validate(m);

            final boolean insert = random.nextBoolean();
            if (insert) {
                check.insert(operand);
                check.validate(m);
            } else {
                check.remove(operand);
                check.validate(m);
            }

            final SequentialRowSetBuilder builder2 = getFactory().getSequentialBuilder();
            for (final TrackingMutableRowSet.Iterator it = check.iterator(); it.hasNext();) {
                final long next = it.nextLong();
                final boolean partA = random.nextBoolean();
                if (partA) {
                    builder2.appendKey(next);
                }
            }
            final TrackingMutableRowSet subsetA = builder2.build();
            subsetA.validate(m);

            final TrackingMutableRowSet checkA = check.intersect(subsetA);
            checkA.validate(m);
            final TrackingMutableRowSet checkB = check.minus(subsetA);
            checkB.validate(m);
            checkA.insert(checkB);
            checkA.validate(m);
            Assert.assertion(checkA.equals(check), "checkA.equals(check)", check, "check", checkA, "checkA");
        }
    }

    public void testChunkInsertAndRemove() {
        // noinspection unchecked
        final Supplier<TreeIndexImpl>[] suppliers = new Supplier[] {
                () -> TreeIndexImpl.EMPTY

                , () -> SingleRange.make(0, 0), () -> SingleRange.make(4_000_000_000L, 4_000_000_000L),
                () -> SingleRange.make(RspArray.BLOCK_SIZE, 2 * RspArray.BLOCK_SIZE - 1),
                () -> SingleRange.make(RspArray.BLOCK_SIZE, 4 * RspArray.BLOCK_SIZE - 1),
                () -> SingleRange.make(0, 9_999), () -> SingleRange.make(4_000_000_000L, 4_000_009_999L)

                , SortedRanges::makeEmpty, () -> SortedRanges.makeSingleElement(0),
                () -> SortedRanges.makeSingleElement(4_000_000_000L),
                () -> SortedRanges.makeSingleRange(RspArray.BLOCK_SIZE, 2 * RspArray.BLOCK_SIZE - 1),
                () -> SortedRanges.makeSingleRange(RspArray.BLOCK_SIZE, 2 * RspArray.BLOCK_SIZE - 100),
                () -> SortedRanges.makeSingleRange(2 * RspArray.BLOCK_SIZE - 100, 2 * RspArray.BLOCK_SIZE),
                () -> SortedRanges.makeSingleRange(RspArray.BLOCK_SIZE, 4 * RspArray.BLOCK_SIZE - 1),
                () -> SortedRanges.makeSingleRange(0, 9_999),
                () -> SortedRanges.makeSingleRange(4_000_000_000L, 4_000_009_999L), () -> {
                    final TreeIndexImpl r = SortedRanges.tryMakeForKnownRangeKnownCount(100, 10, 10_010);
                    r.ixInsertRange(0, 100);
                    r.ixInsert(256);
                    r.ixInsertRange(1024, 9000);
                    return r;
                }, () -> TreeIndexImpl.fromChunk(LongChunk.chunkWrap(new long[] {}), 0, 0, true),
                () -> TreeIndexImpl.fromChunk(LongChunk.chunkWrap(new long[] {0, 1, 2, 3}), 0, 4, true),
                () -> TreeIndexImpl.fromChunk(LongChunk.chunkWrap(new long[] {0, 1, 2, 3, 4, 5, 6}), 2, 3, true)

                , RspBitmap::makeEmpty, () -> RspBitmap.makeSingleRange(0, 0),
                () -> RspBitmap.makeSingleRange(4_000_000_000L, 4_000_000_000L),
                () -> RspBitmap.makeSingleRange(RspArray.BLOCK_SIZE, 2 * RspArray.BLOCK_SIZE - 1),
                () -> RspBitmap.makeSingleRange(RspArray.BLOCK_SIZE, 2 * RspArray.BLOCK_SIZE - 100),
                () -> RspBitmap.makeSingleRange(2 * RspArray.BLOCK_SIZE - 100, 2 * RspArray.BLOCK_SIZE),
                () -> RspBitmap.makeSingleRange(RspArray.BLOCK_SIZE, 4 * RspArray.BLOCK_SIZE - 1),
                () -> RspBitmap.makeEmpty().ixInsert(4_000_000_000L).ixInsert(4_000_000_002L),
                () -> RspBitmap.makeSingleRange(0, 9_999),
                () -> RspBitmap.makeSingleRange(4_000_000_000L, 4_000_009_999L), () -> {
                    final TreeIndexImpl r = RspBitmap.makeEmpty();
                    r.ixInsertRange(0, 100);
                    r.ixInsert(256);
                    r.ixInsertRange(1024, 2048);
                    return r;
                }, () -> {
                    final TreeIndexImpl r = RspBitmap.makeEmpty();
                    r.ixInsertRange(0, 100);
                    r.ixInsert(256);
                    r.ixInsertRange(1024, 9000);
                    return r;
                }, () -> {
                    final TreeIndexImpl r = RspBitmap.makeEmpty();
                    LongStream
                            .rangeClosed(RspArray.BLOCK_SIZE * 4,
                                    RspArray.BLOCK_SIZE * 4 + ArrayContainer.SWITCH_CONTAINER_CARDINALITY_THRESHOLD * 2)
                            .filter(l -> (l & 1) == 0).forEach(r::ixInsert);
                    return r;
                }
        };

        int step = 0;
        for (final Supplier<TreeIndexImpl> lhs : suppliers) {
            for (final Supplier<TreeIndexImpl> rhs : suppliers) {
                ++step;
                final String m = "step=" + step;
                final TrackingMutableRowSet rowSet = fromTreeIndexImpl(rhs.get());
                final LongChunk<OrderedRowKeys> asKeyIndicesChunk = rowSet.asRowKeyChunk();

                final TrackingMutableRowSet expectedAfterInsert = fromTreeIndexImpl(lhs.get());
                expectedAfterInsert.validate(m);
                expectedAfterInsert.insert(rowSet);
                expectedAfterInsert.validate(m);

                final TrackingMutableRowSet actualAfterInsert1 = fromTreeIndexImpl(lhs.get());
                actualAfterInsert1.insert(asKeyIndicesChunk, 0, asKeyIndicesChunk.size());
                actualAfterInsert1.validate(m);
                assertEquals(m, expectedAfterInsert, actualAfterInsert1);

                final TrackingMutableRowSet actualAfterInsert2 = fromTreeIndexImpl(lhs.get());
                try (final WritableLongChunk<OrderedRowKeys> toBeSliced =
                        WritableLongChunk.makeWritableChunk(asKeyIndicesChunk.size() + 2048)) {
                    toBeSliced.copyFromChunk(asKeyIndicesChunk, 0, 1024, asKeyIndicesChunk.size());
                    actualAfterInsert2.insert(toBeSliced, 1024, asKeyIndicesChunk.size());
                }
                actualAfterInsert2.validate(m);
                assertEquals(m, expectedAfterInsert, actualAfterInsert2);

                final TrackingMutableRowSet expectedAfterRemove = fromTreeIndexImpl(lhs.get());
                expectedAfterRemove.validate(m);
                expectedAfterRemove.remove(rowSet);

                final TrackingMutableRowSet actualAfterRemove1 = fromTreeIndexImpl(lhs.get());
                actualAfterRemove1.remove(asKeyIndicesChunk, 0, asKeyIndicesChunk.size());
                actualAfterRemove1.validate(m);
                assertEquals(m, expectedAfterRemove, actualAfterRemove1);

                final TrackingMutableRowSet actualAfterRemove2 = fromTreeIndexImpl(lhs.get());
                try (final WritableLongChunk<Attributes.OrderedRowKeys> toBeSliced =
                        WritableLongChunk.makeWritableChunk(asKeyIndicesChunk.size() + 2048)) {
                    toBeSliced.copyFromChunk(asKeyIndicesChunk, 0, 1024, asKeyIndicesChunk.size());
                    actualAfterRemove2.remove(toBeSliced, 1024, asKeyIndicesChunk.size());
                }
                actualAfterRemove2.validate(m);
                assertEquals(m, expectedAfterRemove, actualAfterRemove2);
            }
        }
        for (final Supplier<TreeIndexImpl> lhs : suppliers) {
            for (final Supplier<TreeIndexImpl> rhs : suppliers) {
                final TreeIndexImpl lhsTreeIndexImpl = lhs.get();
                if (lhsTreeIndexImpl instanceof RspBitmap) {
                    final TrackingMutableRowSet lhsRowSet = fromTreeIndexImpl(lhs.get());
                    final TrackingMutableRowSet rhsRowSet = fromTreeIndexImpl(rhs.get());

                    lhsRowSet.insert(rhsRowSet);
                    lhsTreeIndexImpl.ixInsertSecondHalf(rhsRowSet.asRowKeyChunk(), 0, rhsRowSet.intSize());

                    assertEquals(lhsRowSet, fromTreeIndexImpl(lhsTreeIndexImpl));
                }
            }
        }
    }

    @NotNull
    protected abstract TrackingMutableRowSet fromTreeIndexImpl(@NotNull TreeIndexImpl treeIndexImpl);

    @NotNull
    protected abstract RowSetFactory getFactory();

    @NotNull
    protected abstract TrackingMutableRowSet getSortedIndex(long... keys);
}

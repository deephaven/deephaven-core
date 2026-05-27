//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit TestCharSegmentedSortedMultiset and run "./gradlew replicateSegmentedSortedMultisetTests" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.ssms;

import io.deephaven.base.verify.AssertionFailure;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.ShiftObliviousListener;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.testutil.ColumnInfo;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.GenerateTableUpdates;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.engine.util.TableTools;
import io.deephaven.util.compare.LongComparisons;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import io.deephaven.engine.liveness.LivenessScope;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.table.impl.*;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.impl.ssa.SsaTestHelpers;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.vector.LongVectorDirect;
import io.deephaven.vector.ObjectVectorDirect;
import io.deephaven.engine.table.impl.util.compact.LongCompactKernel;
import io.deephaven.test.types.ParallelTest;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.mutable.MutableInt;
import junit.framework.TestCase;
import org.jetbrains.annotations.NotNull;
import org.junit.experimental.categories.Category;

import java.util.Arrays;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import static io.deephaven.engine.testutil.TstUtils.getTable;
import static io.deephaven.engine.testutil.TstUtils.initColumnInfos;
import static io.deephaven.util.QueryConstants.NULL_LONG;
import static org.junit.Assert.assertArrayEquals;

@Category(ParallelTest.class)
public class TestLongSegmentedSortedMultiset extends RefreshingTableTestCase {

    public void testInsertion() {
        final SsaTestHelpers.TestDescriptor desc = new SsaTestHelpers.TestDescriptor();
        for (int seed = 0; seed < 10; ++seed) {
            for (int tableSize = 10; tableSize <= 1000; tableSize *= 10) {
                for (int nodeSize = 8; nodeSize <= 2048; nodeSize *= 2) {
                    testUpdates(desc.reset(seed, tableSize, nodeSize), true, false, true);
                }
            }
        }
    }

    public void testRemove() {
        final SsaTestHelpers.TestDescriptor desc = new SsaTestHelpers.TestDescriptor();
        for (int seed = 0; seed < 10; ++seed) {
            for (int tableSize = 10; tableSize <= 1000; tableSize *= 10) {
                for (int nodeSize = 8; nodeSize <= 2048; nodeSize *= 2) {
                    testUpdates(desc.reset(seed, tableSize, nodeSize), false, true, true);
                }
            }
        }
    }

    public void testInsertAndRemove() {
        final SsaTestHelpers.TestDescriptor desc = new SsaTestHelpers.TestDescriptor();
        final int nSeeds = scaleToDesiredTestLength(100);
        for (int tableSize = 10; tableSize <= 1000; tableSize *= 2) {
            for (int nodeSize = 8; nodeSize <= 2048; nodeSize *= 2) {
                for (int seed = 0; seed < nSeeds; ++seed) {
                    testUpdates(desc.reset(seed, tableSize, nodeSize), true, true, true);
                }
            }
        }
    }

    public void testMove() {
        final SsaTestHelpers.TestDescriptor desc = new SsaTestHelpers.TestDescriptor();
        final int nSeeds = scaleToDesiredTestLength(200);
        for (int tableSize = 10; tableSize <= 10000; tableSize *= 2) {
            for (int nodeSize = 8; nodeSize <= 2048; nodeSize *= 2) {
                for (int seed = 0; seed < nSeeds; ++seed) {
                    testMove(desc.reset(seed, tableSize, nodeSize), true);
                }
            }
        }
    }

    public void testEqualsArray() {
        // exercise the singleton (size == 1), single-leaf, and multi-leaf representations
        checkEqualsArray(1);
        checkEqualsArray(3);
        checkEqualsArray(20);
    }

    public void testMoveSingletonSource() {
        final int nodeSize = 4;
        final SsaTestHelpers.TestDescriptor desc = new SsaTestHelpers.TestDescriptor();
        // destination states: empty, singleton, partial leaf, full single leaf, multi-leaf (last leaf partial and full)
        for (final int destCount : new int[] {0, 1, 3, 4, 6, 8}) {
            checkAppendMaximum(nodeSize, destCount, desc);
            checkPrependMinimum(nodeSize, destCount, desc);
        }
    }

    public void testMoveSingletonMerge() {
        final int nodeSize = 4;
        final SsaTestHelpers.TestDescriptor desc = new SsaTestHelpers.TestDescriptor();
        final long v = (long) ('a' + 5);
        final long w = (long) ('a' + 6);

        // moveFrontToBack: a singleton whose value equals the destination's (singleton) maximum merges via addMaxCount
        {
            final LongSegmentedSortedMultiset source = makeSsm(nodeSize, new long[] {v}, new int[] {2});
            final LongSegmentedSortedMultiset dest = makeSsm(nodeSize, new long[] {v}, new int[] {3});
            source.moveFrontToBack(dest, source.totalSize());
            verifySsm(source, new long[0], desc);
            verifySsm(dest, new long[] {v, v, v, v, v}, desc);
        }

        // moveBackToFront: a singleton whose value equals the destination's (singleton) minimum merges via addMinCount
        {
            final LongSegmentedSortedMultiset source = makeSsm(nodeSize, new long[] {v}, new int[] {2});
            final LongSegmentedSortedMultiset dest = makeSsm(nodeSize, new long[] {v}, new int[] {3});
            source.moveBackToFront(dest, source.totalSize());
            verifySsm(source, new long[0], desc);
            verifySsm(dest, new long[] {v, v, v, v, v}, desc);
        }

        // moveFrontToBack: a non-singleton source whose minimum equals the destination's maximum, moving fewer than the
        // minimum's count, reduces the source minimum in place (addMinCount(-count)) rather than removing it
        {
            final LongSegmentedSortedMultiset source = makeSsm(nodeSize, new long[] {v, w}, new int[] {3, 1});
            final LongSegmentedSortedMultiset dest = makeSsm(nodeSize, new long[] {v}, new int[] {1});
            source.moveFrontToBack(dest, 1);
            verifySsm(source, new long[] {v, v, w}, desc);
            verifySsm(dest, new long[] {v, v}, desc);
        }

        // moveBackToFront: the symmetric case, reducing the source maximum in place (addMaxCount(-count))
        {
            final LongSegmentedSortedMultiset source = makeSsm(nodeSize, new long[] {v, w}, new int[] {1, 3});
            final LongSegmentedSortedMultiset dest = makeSsm(nodeSize, new long[] {w}, new int[] {1});
            source.moveBackToFront(dest, 1);
            verifySsm(source, new long[] {v, w, w}, desc);
            verifySsm(dest, new long[] {w, w}, desc);
        }
    }

    public void testPartialCopy() {
        final int nodeSize = 8;
        final LongSegmentedSortedMultiset ssm = new LongSegmentedSortedMultiset(nodeSize);

        final long[] data = new long[24];
        try (final WritableLongChunk<Values> valuesChunk = WritableLongChunk.makeWritableChunk(24);
             final WritableIntChunk<ChunkLengths> countsChunk = WritableIntChunk.makeWritableChunk(24)) {

            for (int ii = 0; ii < 24; ii++) {
                data[ii] = (long) ('a' + ii);
                countsChunk.set(ii, 1);
                valuesChunk.set(ii, data[ii]);
            }

            ssm.insert(valuesChunk, countsChunk);
        }

        assertArrayEquals(data, ssm.toArray()/*EXTRA*/);
        assertArrayEquals(data, ssm.subVector(0, 23).toArray()/*EXTRA*/);
        assertArrayEquals(Arrays.copyOfRange(data,0, 4), ssm.subVector(0, 3).toArray()/*EXTRA*/);
        assertArrayEquals(Arrays.copyOfRange(data, 0, 8), ssm.subVector(0, 7).toArray()/*EXTRA*/);
        assertArrayEquals(Arrays.copyOfRange(data, 0, 16), ssm.subVector(0, 15).toArray()/*EXTRA*/);

        assertArrayEquals(Arrays.copyOfRange(data, 2, 6), ssm.subVector(2, 5).toArray()/*EXTRA*/);
        assertArrayEquals(Arrays.copyOfRange(data, 2, 12), ssm.subVector(2, 11).toArray()/*EXTRA*/);
        assertArrayEquals(Arrays.copyOfRange(data, 7, 12), ssm.subVector(7, 11).toArray()/*EXTRA*/);
        assertArrayEquals(Arrays.copyOfRange(data, 7, 16), ssm.subVector(7, 15).toArray()/*EXTRA*/);
        assertArrayEquals(Arrays.copyOfRange(data, 11, 16), ssm.subVector(11, 15).toArray()/*EXTRA*/);
        assertArrayEquals(Arrays.copyOfRange(data, 2, 20), ssm.subVector(2, 19).toArray()/*EXTRA*/);
    }

    // region SortFixupSanityCheck
    public void testSanity() {
        QueryTable john = TstUtils.testRefreshingTable(TableTools.longCol("John", NULL_LONG, NULL_LONG, (long)0x0, (long)0x1, Long.MAX_VALUE, Long.MAX_VALUE));
        final ColumnSource<Long> valueSource = john.getColumnSource("John");
        try (final ColumnSource.FillContext fillContext = valueSource.makeFillContext(1024);
             final WritableLongChunk<Values> chunk = WritableLongChunk.makeWritableChunk(1024);
             final WritableIntChunk<ChunkLengths> counts = WritableIntChunk.makeWritableChunk(1024)
        ) {
            valueSource.fillChunk(fillContext, chunk, john.getRowSet());
            LongCompactKernel.compactAndCount(chunk, counts, true, true);
        }
    }
    //endregion SortFixupSanityCheck

    private void testUpdates(@NotNull final SsaTestHelpers.TestDescriptor desc, boolean allowAddition, boolean allowRemoval, boolean countNullNaN) {
        final Random random = new Random(desc.seed());
        final ColumnInfo[] columnInfo;
        final QueryTable table = getTable(desc.tableSize(), random, columnInfo = initColumnInfos(new String[]{"Value"},
                SsaTestHelpers.getGeneratorForLong()));

        final Table asLong = SsaTestHelpers.prepareTestTableForLong(table);

        final LongSegmentedSortedMultiset ssm = new LongSegmentedSortedMultiset(desc.nodeSize());

        //noinspection unchecked
        final ColumnSource<Long> valueSource = asLong.getColumnSource("Value");

        checkSsmInitial(asLong, ssm, valueSource, countNullNaN, desc);

        try (final SafeCloseable ignored = LivenessScopeStack.open(new LivenessScope(true), true)) {
            final ShiftObliviousListener asLongListener = new ShiftObliviousInstrumentedListenerAdapter(asLong, false) {
                @Override
                public void onUpdate(RowSet added, RowSet removed, RowSet modified) {
                    final int maxSize = Math.max(Math.max(added.intSize(), removed.intSize()), modified.intSize());
                    try (final ColumnSource.FillContext fillContext = valueSource.makeFillContext(maxSize);
                         final WritableLongChunk<Values> chunk = WritableLongChunk.makeWritableChunk(maxSize);
                         final WritableIntChunk<ChunkLengths> counts = WritableIntChunk.makeWritableChunk(maxSize)
                    ) {
                        final SegmentedSortedMultiSet.RemoveContext removeContext = SegmentedSortedMultiSet.makeRemoveContext(desc.nodeSize());

                        if (removed.isNonempty()) {
                            valueSource.fillPrevChunk(fillContext, chunk, removed);
                            LongCompactKernel.compactAndCount(chunk, counts, countNullNaN, countNullNaN);
                            ssm.remove(removeContext, chunk, counts);
                        }


                        if (added.isNonempty()) {
                            valueSource.fillChunk(fillContext, chunk, added);
                            LongCompactKernel.compactAndCount(chunk, counts, countNullNaN, countNullNaN);
                            ssm.insert(chunk, counts);
                        }
                    }
                }
            };
            asLong.addUpdateListener(asLongListener);

            final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
            while (desc.advance(50)) {
                updateGraph.runWithinUnitTestCycle(() -> {
                    final RowSet[] notify = GenerateTableUpdates.computeTableUpdates(desc.tableSize(), random, table, columnInfo, allowAddition, allowRemoval, false);
                    assertTrue(notify[2].isEmpty());
                    table.notifyListeners(notify[0], notify[1], notify[2]);
                });

                try (final ColumnSource.GetContext getContext = valueSource.makeGetContext(asLong.intSize())) {
                    checkSsm(ssm, valueSource.getChunk(getContext, asLong.getRowSet()).asLongChunk(), countNullNaN, desc);
                }

                if (!allowAddition && table.size() == 0) {
                    break;
                }
            }
        }
    }

    private void testMove(@NotNull final SsaTestHelpers.TestDescriptor desc, boolean countNull) {
        final Random random = new Random(desc.seed());
        final QueryTable table = getTable(desc.tableSize(), random, initColumnInfos(new String[]{"Value"},
                SsaTestHelpers.getGeneratorForLong()));

        final Table asLong = SsaTestHelpers.prepareTestTableForLong(table);

        final LongSegmentedSortedMultiset ssmLo = new LongSegmentedSortedMultiset(desc.nodeSize());
        final LongSegmentedSortedMultiset ssmHi = new LongSegmentedSortedMultiset(desc.nodeSize());

        //noinspection unchecked
        final ColumnSource<Long> valueSource = asLong.getColumnSource("Value");

        checkSsmInitial(asLong, ssmLo, valueSource, countNull, desc);
        final long totalExpectedSize = ssmLo.totalSize();

        while (ssmLo.size() > 0) {
            desc.advance();
            try {
                final long count = random.nextInt(LongSizedDataStructure.intSize("ssmLo", ssmLo.totalSize()) + 1);
                final long newLoCount = ssmLo.totalSize() - count;
                final long newHiCount = ssmHi.totalSize() + count;
                if (printTableUpdates) {
                    System.out.println("Moving " + count + " of " + ssmLo.totalSize() + " elements.");
                }
                ssmLo.moveBackToFront(ssmHi, count);

                assertEquals(newLoCount, ssmLo.totalSize());
                assertEquals(newHiCount, ssmHi.totalSize());
                assertEquals(totalExpectedSize, ssmLo.totalSize() + ssmHi.totalSize());

            } catch (AssertionFailure e) {
                TestCase.fail("Moving lo to hi failed at " + desc + ": " + e.getMessage());
            }

            try (final ColumnSource.FillContext fillContext = valueSource.makeFillContext(asLong.intSize());
                 final WritableLongChunk<Values> valueChunk = WritableLongChunk.makeWritableChunk(asLong.intSize())) {
                valueSource.fillChunk(fillContext, valueChunk, asLong.getRowSet());
                valueChunk.sort();
                final LongChunk<? extends Values> loValues = valueChunk.slice(0, LongSizedDataStructure.intSize("ssmLo", ssmLo.totalSize()));
                final LongChunk<? extends Values> hiValues = valueChunk.slice(LongSizedDataStructure.intSize("ssmLo", ssmLo.totalSize()), LongSizedDataStructure.intSize("ssmHi", ssmHi.totalSize()));
                checkSsm(ssmLo, loValues, countNull, desc);
                checkSsm(ssmHi, hiValues, countNull, desc);
            }
        }

        checkSsm(asLong, ssmHi, valueSource, countNull, desc);

        while (ssmHi.size() > 0) {
            desc.advance();
            try {
                final long count = random.nextInt(LongSizedDataStructure.intSize("ssmHi", ssmHi.totalSize()) + 1);

                final long newLoCount = ssmLo.totalSize() + count;
                final long newHiCount = ssmHi.totalSize() - count;

                if (printTableUpdates) {
                    System.out.println("Moving " + count + " of " + ssmHi.totalSize() + " elements.");
                }
                ssmHi.moveFrontToBack(ssmLo, count);

                assertEquals(newLoCount, ssmLo.totalSize());
                assertEquals(newHiCount, ssmHi.totalSize());
                assertEquals(totalExpectedSize, ssmLo.totalSize() + ssmHi.totalSize());
            } catch (AssertionFailure e) {
                TestCase.fail("Moving hi to lo failed at " + desc + ": " + e.getMessage());
            }
        }

        checkSsm(asLong, ssmLo, valueSource, countNull, desc);
    }

    private void checkSsmInitial(Table asLong, LongSegmentedSortedMultiset ssm, ColumnSource<?> valueSource, boolean countNullNaN, @NotNull final SsaTestHelpers.TestDescriptor desc) {
        try (final ColumnSource.FillContext fillContext = valueSource.makeFillContext(asLong.intSize());
             final WritableLongChunk<Values> valueChunk = WritableLongChunk.makeWritableChunk(asLong.intSize());
             final WritableIntChunk<ChunkLengths> counts = WritableIntChunk.makeWritableChunk(asLong.intSize())) {
            valueSource.fillChunk(fillContext, valueChunk, asLong.getRowSet());
            valueChunk.sort();

            LongCompactKernel.compactAndCount(valueChunk, counts, countNullNaN, countNullNaN);

            ssm.insert(valueChunk, counts);

            valueSource.fillChunk(fillContext, valueChunk, asLong.getRowSet());
            checkSsm(ssm, valueChunk, countNullNaN, desc);
        }
    }

    private void checkSsm(Table asLong, LongSegmentedSortedMultiset ssm, ColumnSource<?> valueSource, boolean countNull, @NotNull final SsaTestHelpers.TestDescriptor desc) {
        try (final ColumnSource.FillContext fillContext = valueSource.makeFillContext(asLong.intSize());
             final WritableLongChunk<Values> valueChunk = WritableLongChunk.makeWritableChunk(asLong.intSize())) {
            valueSource.fillChunk(fillContext, valueChunk, asLong.getRowSet());
            checkSsm(ssm, valueChunk, countNull, desc);
        }
    }

    private void checkSsm(LongSegmentedSortedMultiset ssm, LongChunk<? extends Values> valueChunk, boolean countNull, @NotNull final SsaTestHelpers.TestDescriptor desc) {
        try {
            ssm.validate();
            try (final WritableLongChunk<?> keys = ssm.keyChunk();
                 final WritableLongChunk<?> counts = ssm.countChunk()) {

                int totalSize = 0;

                final Map<Long, Integer> checkMap = new TreeMap<>(LongComparisons::compare);
                for (int ii = 0; ii < valueChunk.size(); ++ii) {
                    final long value = valueChunk.get(ii);
                    if (value == NULL_LONG && !countNull) {
                        continue;
                    }
                    totalSize++;
                    checkMap.compute(value, (key, cnt) -> {
                        if (cnt == null) return 1;
                        else return cnt + 1;
                    });
                }

                assertEquals(checkMap.size(), ssm.size());
                assertEquals(totalSize, ssm.totalSize());
                assertEquals(checkMap.size(), keys.size());
                assertEquals(checkMap.size(), counts.size());

                final MutableInt offset = new MutableInt(0);
                checkMap.forEach((key, count) -> {
                    assertEquals((long) key, keys.get(offset.get()));
                    assertEquals((long) count, counts.get(offset.get()));
                    offset.increment();
                });
            }
        } catch (AssertionFailure e) {
            TestCase.fail("Check failed at " + desc + ": " + e.getMessage());
        }
    }

    private LongSegmentedSortedMultiset makeSsm(int nodeSize, long[] values) {
        final int[] counts = new int[values.length];
        Arrays.fill(counts, 1);
        return makeSsm(nodeSize, values, counts);
    }

    private LongSegmentedSortedMultiset makeSsm(int nodeSize, long[] values, int[] counts) {
        final LongSegmentedSortedMultiset ssm = new LongSegmentedSortedMultiset(nodeSize);
        if (values.length > 0) {
            try (final WritableLongChunk<Values> valuesChunk = WritableLongChunk.makeWritableChunk(values.length);
                 final WritableIntChunk<ChunkLengths> countsChunk = WritableIntChunk.makeWritableChunk(values.length)) {
                for (int ii = 0; ii < values.length; ++ii) {
                    valuesChunk.set(ii, values[ii]);
                    countsChunk.set(ii, counts[ii]);
                }
                ssm.insert(valuesChunk, countsChunk);
            }
        }
        return ssm;
    }

    private void checkEqualsArray(int valueCount) {
        final int nodeSize = 4;
        final long[] values = new long[valueCount];
        for (int ii = 0; ii < valueCount; ++ii) {
            values[ii] = (long) ('a' + ii);
        }
        final LongSegmentedSortedMultiset ssm = makeSsm(nodeSize, values);

        final Long[] boxed = new Long[valueCount];
        for (int ii = 0; ii < valueCount; ++ii) {
            boxed[ii] = values[ii];
        }

        // a Vector with identical contents is equal (the primitive Vector becomes an ObjectVector after Object
        // replication, so this exercises both equalsArray overloads across the type variants)
        assertTrue(ssm.equals(ssm.getDirect()));
        assertTrue(ssm.equals(new LongVectorDirect(values)));
        assertTrue(ssm.equals(new ObjectVectorDirect<>(boxed)));

        // a Vector of a different length is not equal
        final long[] longer = new long[valueCount + 1];
        for (int ii = 0; ii < longer.length; ++ii) {
            longer[ii] = (long) ('a' + ii);
        }
        assertFalse(ssm.equals(new LongVectorDirect(longer)));
        final Long[] longerBoxed = new Long[valueCount + 1];
        for (int ii = 0; ii < longerBoxed.length; ++ii) {
            longerBoxed[ii] = (long) ('a' + ii);
        }
        assertFalse(ssm.equals(new ObjectVectorDirect<>(longerBoxed)));

        // a Vector that differs from the original in a single position is not equal; check the first, middle, and last
        if (valueCount > 0) {
            final long different = (long) ('a' + 20);
            for (final int position : new int[] {0, valueCount / 2, valueCount - 1}) {
                final long[] modifiedValues = values.clone();
                modifiedValues[position] = different;
                assertFalse(ssm.equals(new LongVectorDirect(modifiedValues)));

                final Long[] modifiedBoxed = boxed.clone();
                modifiedBoxed[position] = different;
                assertFalse(ssm.equals(new ObjectVectorDirect<>(modifiedBoxed)));
            }
        }
    }

    // region NullEquals
    public void testEqualsArrayNull() {
        // a singleton holding the null sentinel
        checkEqualsArrayNull(4, new long[] {NULL_LONG});
        // a single leaf containing the null sentinel
        checkEqualsArrayNull(4, new long[] {NULL_LONG, (long) ('a' + 1), (long) ('a' + 2)});
        // multiple leaves containing the null sentinel
        checkEqualsArrayNull(4, new long[] {NULL_LONG,
                (long) ('a' + 1), (long) ('a' + 2), (long) ('a' + 3), (long) ('a' + 4), (long) ('a' + 5)});
    }

    private void checkEqualsArrayNull(int nodeSize, long[] sortedValues) {
        // sortedValues must be sorted and distinct and contain the null sentinel (which sorts first)
        final LongSegmentedSortedMultiset ssm = makeSsm(nodeSize, sortedValues);
        final long[] stored = ssm.toArray();

        // boxing the null sentinel yields a non-null element holding the sentinel value, which compares equal
        final Long[] boxedSentinel = new Long[stored.length];
        for (int ii = 0; ii < stored.length; ++ii) {
            boxedSentinel[ii] = stored[ii];
        }
        assertTrue(ssm.equals(new ObjectVectorDirect<>(boxedSentinel)));

        // a literal null also compares equal to the stored null sentinel
        final Long[] boxedNull = boxedSentinel.clone();
        for (int ii = 0; ii < stored.length; ++ii) {
            if (stored[ii] == NULL_LONG) {
                boxedNull[ii] = null;
            }
        }
        assertTrue(ssm.equals(new ObjectVectorDirect<>(boxedNull)));

        // a null at a position holding a non-null value is not equal
        for (int ii = 0; ii < stored.length; ++ii) {
            if (stored[ii] != NULL_LONG) {
                final Long[] boxedWrongNull = boxedSentinel.clone();
                boxedWrongNull[ii] = null;
                assertFalse(ssm.equals(new ObjectVectorDirect<>(boxedWrongNull)));
                break;
            }
        }
    }
    // endregion NullEquals

    private void verifySsm(LongSegmentedSortedMultiset ssm, long[] expanded, SsaTestHelpers.TestDescriptor desc) {
        try (final WritableLongChunk<Values> valueChunk =
                WritableLongChunk.makeWritableChunk(Math.max(expanded.length, 1))) {
            valueChunk.setSize(expanded.length);
            for (int ii = 0; ii < expanded.length; ++ii) {
                valueChunk.set(ii, expanded[ii]);
            }
            checkSsm(ssm, valueChunk, true, desc);
        }
    }

    private void checkAppendMaximum(int nodeSize, int destCount, SsaTestHelpers.TestDescriptor desc) {
        final long[] destValues = new long[destCount];
        for (int ii = 0; ii < destCount; ++ii) {
            destValues[ii] = (long) ('a' + 1 + ii);
        }
        // strictly greater than everything in the destination, so it becomes the new maximum (exercises appendMaximum)
        final long value = (long) ('a' + 20);

        final LongSegmentedSortedMultiset source = makeSsm(nodeSize, new long[] {value});
        final LongSegmentedSortedMultiset dest = makeSsm(nodeSize, destValues);

        source.moveFrontToBack(dest, source.totalSize());

        verifySsm(source, new long[0], desc);
        final long[] expected = Arrays.copyOf(destValues, destCount + 1);
        expected[destCount] = value;
        verifySsm(dest, expected, desc);
    }

    private void checkPrependMinimum(int nodeSize, int destCount, SsaTestHelpers.TestDescriptor desc) {
        final long[] destValues = new long[destCount];
        for (int ii = 0; ii < destCount; ++ii) {
            destValues[ii] = (long) ('a' + 1 + ii);
        }
        // strictly less than everything in the destination, so it becomes the new minimum (exercises prependMinimum).
        // Use ('a' + 0) rather than a bare 'a' so that the Object replication boxes to the same type as the other
        // values (int arithmetic boxes to Integer; a bare long literal would box to Long and break comparisons).
        final long value = (long) ('a' + 0);

        final LongSegmentedSortedMultiset source = makeSsm(nodeSize, new long[] {value});
        final LongSegmentedSortedMultiset dest = makeSsm(nodeSize, destValues);

        source.moveBackToFront(dest, source.totalSize());

        verifySsm(source, new long[0], desc);
        final long[] expected = new long[destCount + 1];
        expected[0] = value;
        System.arraycopy(destValues, 0, expected, 1, destCount);
        verifySsm(dest, expected, desc);
    }
}

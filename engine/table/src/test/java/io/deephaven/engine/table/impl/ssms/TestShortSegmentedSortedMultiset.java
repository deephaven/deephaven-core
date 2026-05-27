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
import io.deephaven.util.compare.ShortComparisons;
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
import io.deephaven.vector.ShortVectorDirect;
import io.deephaven.vector.ObjectVectorDirect;
import io.deephaven.engine.table.impl.util.compact.ShortCompactKernel;
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
import static io.deephaven.util.QueryConstants.NULL_SHORT;
import static org.junit.Assert.assertArrayEquals;

@Category(ParallelTest.class)
public class TestShortSegmentedSortedMultiset extends RefreshingTableTestCase {

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
        final short v = (short) ('a' + 5);
        final short w = (short) ('a' + 6);

        // moveFrontToBack: a singleton whose value equals the destination's (singleton) maximum merges via addMaxCount
        {
            final ShortSegmentedSortedMultiset source = makeSsm(nodeSize, new short[] {v}, new int[] {2});
            final ShortSegmentedSortedMultiset dest = makeSsm(nodeSize, new short[] {v}, new int[] {3});
            source.moveFrontToBack(dest, source.totalSize());
            verifySsm(source, new short[0], desc);
            verifySsm(dest, new short[] {v, v, v, v, v}, desc);
        }

        // moveBackToFront: a singleton whose value equals the destination's (singleton) minimum merges via addMinCount
        {
            final ShortSegmentedSortedMultiset source = makeSsm(nodeSize, new short[] {v}, new int[] {2});
            final ShortSegmentedSortedMultiset dest = makeSsm(nodeSize, new short[] {v}, new int[] {3});
            source.moveBackToFront(dest, source.totalSize());
            verifySsm(source, new short[0], desc);
            verifySsm(dest, new short[] {v, v, v, v, v}, desc);
        }

        // moveFrontToBack: a non-singleton source whose minimum equals the destination's maximum, moving fewer than the
        // minimum's count, reduces the source minimum in place (addMinCount(-count)) rather than removing it
        {
            final ShortSegmentedSortedMultiset source = makeSsm(nodeSize, new short[] {v, w}, new int[] {3, 1});
            final ShortSegmentedSortedMultiset dest = makeSsm(nodeSize, new short[] {v}, new int[] {1});
            source.moveFrontToBack(dest, 1);
            verifySsm(source, new short[] {v, v, w}, desc);
            verifySsm(dest, new short[] {v, v}, desc);
        }

        // moveBackToFront: the symmetric case, reducing the source maximum in place (addMaxCount(-count))
        {
            final ShortSegmentedSortedMultiset source = makeSsm(nodeSize, new short[] {v, w}, new int[] {1, 3});
            final ShortSegmentedSortedMultiset dest = makeSsm(nodeSize, new short[] {w}, new int[] {1});
            source.moveBackToFront(dest, 1);
            verifySsm(source, new short[] {v, w, w}, desc);
            verifySsm(dest, new short[] {w, w}, desc);
        }
    }

    public void testPartialCopy() {
        final int nodeSize = 8;
        final ShortSegmentedSortedMultiset ssm = new ShortSegmentedSortedMultiset(nodeSize);

        final short[] data = new short[24];
        try (final WritableShortChunk<Values> valuesChunk = WritableShortChunk.makeWritableChunk(24);
             final WritableIntChunk<ChunkLengths> countsChunk = WritableIntChunk.makeWritableChunk(24)) {

            for (int ii = 0; ii < 24; ii++) {
                data[ii] = (short) ('a' + ii);
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
        QueryTable john = TstUtils.testRefreshingTable(TableTools.shortCol("John", NULL_SHORT, NULL_SHORT, (short)0x0, (short)0x1, Short.MAX_VALUE, Short.MAX_VALUE));
        final ColumnSource<Short> valueSource = john.getColumnSource("John");
        try (final ColumnSource.FillContext fillContext = valueSource.makeFillContext(1024);
             final WritableShortChunk<Values> chunk = WritableShortChunk.makeWritableChunk(1024);
             final WritableIntChunk<ChunkLengths> counts = WritableIntChunk.makeWritableChunk(1024)
        ) {
            valueSource.fillChunk(fillContext, chunk, john.getRowSet());
            ShortCompactKernel.compactAndCount(chunk, counts, true, true);
        }
    }
    //endregion SortFixupSanityCheck

    private void testUpdates(@NotNull final SsaTestHelpers.TestDescriptor desc, boolean allowAddition, boolean allowRemoval, boolean countNullNaN) {
        final Random random = new Random(desc.seed());
        final ColumnInfo[] columnInfo;
        final QueryTable table = getTable(desc.tableSize(), random, columnInfo = initColumnInfos(new String[]{"Value"},
                SsaTestHelpers.getGeneratorForShort()));

        final Table asShort = SsaTestHelpers.prepareTestTableForShort(table);

        final ShortSegmentedSortedMultiset ssm = new ShortSegmentedSortedMultiset(desc.nodeSize());

        //noinspection unchecked
        final ColumnSource<Short> valueSource = asShort.getColumnSource("Value");

        checkSsmInitial(asShort, ssm, valueSource, countNullNaN, desc);

        try (final SafeCloseable ignored = LivenessScopeStack.open(new LivenessScope(true), true)) {
            final ShiftObliviousListener asShortListener = new ShiftObliviousInstrumentedListenerAdapter(asShort, false) {
                @Override
                public void onUpdate(RowSet added, RowSet removed, RowSet modified) {
                    final int maxSize = Math.max(Math.max(added.intSize(), removed.intSize()), modified.intSize());
                    try (final ColumnSource.FillContext fillContext = valueSource.makeFillContext(maxSize);
                         final WritableShortChunk<Values> chunk = WritableShortChunk.makeWritableChunk(maxSize);
                         final WritableIntChunk<ChunkLengths> counts = WritableIntChunk.makeWritableChunk(maxSize)
                    ) {
                        final SegmentedSortedMultiSet.RemoveContext removeContext = SegmentedSortedMultiSet.makeRemoveContext(desc.nodeSize());

                        if (removed.isNonempty()) {
                            valueSource.fillPrevChunk(fillContext, chunk, removed);
                            ShortCompactKernel.compactAndCount(chunk, counts, countNullNaN, countNullNaN);
                            ssm.remove(removeContext, chunk, counts);
                        }


                        if (added.isNonempty()) {
                            valueSource.fillChunk(fillContext, chunk, added);
                            ShortCompactKernel.compactAndCount(chunk, counts, countNullNaN, countNullNaN);
                            ssm.insert(chunk, counts);
                        }
                    }
                }
            };
            asShort.addUpdateListener(asShortListener);

            final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
            while (desc.advance(50)) {
                updateGraph.runWithinUnitTestCycle(() -> {
                    final RowSet[] notify = GenerateTableUpdates.computeTableUpdates(desc.tableSize(), random, table, columnInfo, allowAddition, allowRemoval, false);
                    assertTrue(notify[2].isEmpty());
                    table.notifyListeners(notify[0], notify[1], notify[2]);
                });

                try (final ColumnSource.GetContext getContext = valueSource.makeGetContext(asShort.intSize())) {
                    checkSsm(ssm, valueSource.getChunk(getContext, asShort.getRowSet()).asShortChunk(), countNullNaN, desc);
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
                SsaTestHelpers.getGeneratorForShort()));

        final Table asShort = SsaTestHelpers.prepareTestTableForShort(table);

        final ShortSegmentedSortedMultiset ssmLo = new ShortSegmentedSortedMultiset(desc.nodeSize());
        final ShortSegmentedSortedMultiset ssmHi = new ShortSegmentedSortedMultiset(desc.nodeSize());

        //noinspection unchecked
        final ColumnSource<Short> valueSource = asShort.getColumnSource("Value");

        checkSsmInitial(asShort, ssmLo, valueSource, countNull, desc);
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

            try (final ColumnSource.FillContext fillContext = valueSource.makeFillContext(asShort.intSize());
                 final WritableShortChunk<Values> valueChunk = WritableShortChunk.makeWritableChunk(asShort.intSize())) {
                valueSource.fillChunk(fillContext, valueChunk, asShort.getRowSet());
                valueChunk.sort();
                final ShortChunk<? extends Values> loValues = valueChunk.slice(0, LongSizedDataStructure.intSize("ssmLo", ssmLo.totalSize()));
                final ShortChunk<? extends Values> hiValues = valueChunk.slice(LongSizedDataStructure.intSize("ssmLo", ssmLo.totalSize()), LongSizedDataStructure.intSize("ssmHi", ssmHi.totalSize()));
                checkSsm(ssmLo, loValues, countNull, desc);
                checkSsm(ssmHi, hiValues, countNull, desc);
            }
        }

        checkSsm(asShort, ssmHi, valueSource, countNull, desc);

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

        checkSsm(asShort, ssmLo, valueSource, countNull, desc);
    }

    private void checkSsmInitial(Table asShort, ShortSegmentedSortedMultiset ssm, ColumnSource<?> valueSource, boolean countNullNaN, @NotNull final SsaTestHelpers.TestDescriptor desc) {
        try (final ColumnSource.FillContext fillContext = valueSource.makeFillContext(asShort.intSize());
             final WritableShortChunk<Values> valueChunk = WritableShortChunk.makeWritableChunk(asShort.intSize());
             final WritableIntChunk<ChunkLengths> counts = WritableIntChunk.makeWritableChunk(asShort.intSize())) {
            valueSource.fillChunk(fillContext, valueChunk, asShort.getRowSet());
            valueChunk.sort();

            ShortCompactKernel.compactAndCount(valueChunk, counts, countNullNaN, countNullNaN);

            ssm.insert(valueChunk, counts);

            valueSource.fillChunk(fillContext, valueChunk, asShort.getRowSet());
            checkSsm(ssm, valueChunk, countNullNaN, desc);
        }
    }

    private void checkSsm(Table asShort, ShortSegmentedSortedMultiset ssm, ColumnSource<?> valueSource, boolean countNull, @NotNull final SsaTestHelpers.TestDescriptor desc) {
        try (final ColumnSource.FillContext fillContext = valueSource.makeFillContext(asShort.intSize());
             final WritableShortChunk<Values> valueChunk = WritableShortChunk.makeWritableChunk(asShort.intSize())) {
            valueSource.fillChunk(fillContext, valueChunk, asShort.getRowSet());
            checkSsm(ssm, valueChunk, countNull, desc);
        }
    }

    private void checkSsm(ShortSegmentedSortedMultiset ssm, ShortChunk<? extends Values> valueChunk, boolean countNull, @NotNull final SsaTestHelpers.TestDescriptor desc) {
        try {
            ssm.validate();
            try (final WritableShortChunk<?> keys = ssm.keyChunk();
                 final WritableLongChunk<?> counts = ssm.countChunk()) {

                int totalSize = 0;

                final Map<Short, Integer> checkMap = new TreeMap<>(ShortComparisons::compare);
                for (int ii = 0; ii < valueChunk.size(); ++ii) {
                    final short value = valueChunk.get(ii);
                    if (value == NULL_SHORT && !countNull) {
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
                    assertEquals((short) key, keys.get(offset.get()));
                    assertEquals((long) count, counts.get(offset.get()));
                    offset.increment();
                });
            }
        } catch (AssertionFailure e) {
            TestCase.fail("Check failed at " + desc + ": " + e.getMessage());
        }
    }

    private ShortSegmentedSortedMultiset makeSsm(int nodeSize, short[] values) {
        final int[] counts = new int[values.length];
        Arrays.fill(counts, 1);
        return makeSsm(nodeSize, values, counts);
    }

    private ShortSegmentedSortedMultiset makeSsm(int nodeSize, short[] values, int[] counts) {
        final ShortSegmentedSortedMultiset ssm = new ShortSegmentedSortedMultiset(nodeSize);
        if (values.length > 0) {
            try (final WritableShortChunk<Values> valuesChunk = WritableShortChunk.makeWritableChunk(values.length);
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
        final short[] values = new short[valueCount];
        for (int ii = 0; ii < valueCount; ++ii) {
            values[ii] = (short) ('a' + ii);
        }
        final ShortSegmentedSortedMultiset ssm = makeSsm(nodeSize, values);

        final Short[] boxed = new Short[valueCount];
        for (int ii = 0; ii < valueCount; ++ii) {
            boxed[ii] = values[ii];
        }

        // a Vector with identical contents is equal (the primitive Vector becomes an ObjectVector after Object
        // replication, so this exercises both equalsArray overloads across the type variants)
        assertTrue(ssm.equals(ssm.getDirect()));
        assertTrue(ssm.equals(new ShortVectorDirect(values)));
        assertTrue(ssm.equals(new ObjectVectorDirect<>(boxed)));

        // a Vector of a different length is not equal
        final short[] longer = new short[valueCount + 1];
        for (int ii = 0; ii < longer.length; ++ii) {
            longer[ii] = (short) ('a' + ii);
        }
        assertFalse(ssm.equals(new ShortVectorDirect(longer)));
        final Short[] longerBoxed = new Short[valueCount + 1];
        for (int ii = 0; ii < longerBoxed.length; ++ii) {
            longerBoxed[ii] = (short) ('a' + ii);
        }
        assertFalse(ssm.equals(new ObjectVectorDirect<>(longerBoxed)));

        // a Vector that differs from the original in a single position is not equal; check the first, middle, and last
        if (valueCount > 0) {
            final short different = (short) ('a' + 20);
            for (final int position : new int[] {0, valueCount / 2, valueCount - 1}) {
                final short[] modifiedValues = values.clone();
                modifiedValues[position] = different;
                assertFalse(ssm.equals(new ShortVectorDirect(modifiedValues)));

                final Short[] modifiedBoxed = boxed.clone();
                modifiedBoxed[position] = different;
                assertFalse(ssm.equals(new ObjectVectorDirect<>(modifiedBoxed)));
            }
        }
    }

    // region NullEquals
    public void testEqualsArrayNull() {
        // a singleton holding the null sentinel
        checkEqualsArrayNull(4, new short[] {NULL_SHORT});
        // a single leaf containing the null sentinel
        checkEqualsArrayNull(4, new short[] {NULL_SHORT, (short) ('a' + 1), (short) ('a' + 2)});
        // multiple leaves containing the null sentinel
        checkEqualsArrayNull(4, new short[] {NULL_SHORT,
                (short) ('a' + 1), (short) ('a' + 2), (short) ('a' + 3), (short) ('a' + 4), (short) ('a' + 5)});
    }

    private void checkEqualsArrayNull(int nodeSize, short[] sortedValues) {
        // sortedValues must be sorted and distinct and contain the null sentinel (which sorts first)
        final ShortSegmentedSortedMultiset ssm = makeSsm(nodeSize, sortedValues);
        final short[] stored = ssm.toArray();

        // boxing the null sentinel yields a non-null element holding the sentinel value, which compares equal
        final Short[] boxedSentinel = new Short[stored.length];
        for (int ii = 0; ii < stored.length; ++ii) {
            boxedSentinel[ii] = stored[ii];
        }
        assertTrue(ssm.equals(new ObjectVectorDirect<>(boxedSentinel)));

        // a literal null also compares equal to the stored null sentinel
        final Short[] boxedNull = boxedSentinel.clone();
        for (int ii = 0; ii < stored.length; ++ii) {
            if (stored[ii] == NULL_SHORT) {
                boxedNull[ii] = null;
            }
        }
        assertTrue(ssm.equals(new ObjectVectorDirect<>(boxedNull)));

        // a null at a position holding a non-null value is not equal
        for (int ii = 0; ii < stored.length; ++ii) {
            if (stored[ii] != NULL_SHORT) {
                final Short[] boxedWrongNull = boxedSentinel.clone();
                boxedWrongNull[ii] = null;
                assertFalse(ssm.equals(new ObjectVectorDirect<>(boxedWrongNull)));
                break;
            }
        }
    }
    // endregion NullEquals

    private void verifySsm(ShortSegmentedSortedMultiset ssm, short[] expanded, SsaTestHelpers.TestDescriptor desc) {
        try (final WritableShortChunk<Values> valueChunk =
                WritableShortChunk.makeWritableChunk(Math.max(expanded.length, 1))) {
            valueChunk.setSize(expanded.length);
            for (int ii = 0; ii < expanded.length; ++ii) {
                valueChunk.set(ii, expanded[ii]);
            }
            checkSsm(ssm, valueChunk, true, desc);
        }
    }

    private void checkAppendMaximum(int nodeSize, int destCount, SsaTestHelpers.TestDescriptor desc) {
        final short[] destValues = new short[destCount];
        for (int ii = 0; ii < destCount; ++ii) {
            destValues[ii] = (short) ('a' + 1 + ii);
        }
        // strictly greater than everything in the destination, so it becomes the new maximum (exercises appendMaximum)
        final short value = (short) ('a' + 20);

        final ShortSegmentedSortedMultiset source = makeSsm(nodeSize, new short[] {value});
        final ShortSegmentedSortedMultiset dest = makeSsm(nodeSize, destValues);

        source.moveFrontToBack(dest, source.totalSize());

        verifySsm(source, new short[0], desc);
        final short[] expected = Arrays.copyOf(destValues, destCount + 1);
        expected[destCount] = value;
        verifySsm(dest, expected, desc);
    }

    private void checkPrependMinimum(int nodeSize, int destCount, SsaTestHelpers.TestDescriptor desc) {
        final short[] destValues = new short[destCount];
        for (int ii = 0; ii < destCount; ++ii) {
            destValues[ii] = (short) ('a' + 1 + ii);
        }
        // strictly less than everything in the destination, so it becomes the new minimum (exercises prependMinimum).
        // Use ('a' + 0) rather than a bare 'a' so that the Object replication boxes to the same type as the other
        // values (int arithmetic boxes to Integer; a bare short literal would box to Short and break comparisons).
        final short value = (short) ('a' + 0);

        final ShortSegmentedSortedMultiset source = makeSsm(nodeSize, new short[] {value});
        final ShortSegmentedSortedMultiset dest = makeSsm(nodeSize, destValues);

        source.moveBackToFront(dest, source.totalSize());

        verifySsm(source, new short[0], desc);
        final short[] expected = new short[destCount + 1];
        expected[0] = value;
        System.arraycopy(destValues, 0, expected, 1, destCount);
        verifySsm(dest, expected, desc);
    }
}

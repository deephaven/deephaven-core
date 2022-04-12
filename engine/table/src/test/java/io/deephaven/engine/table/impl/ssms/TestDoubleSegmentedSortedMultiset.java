/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit TestCharSegmentedSortedMultiset and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.ssms;

import io.deephaven.base.verify.AssertionFailure;
import io.deephaven.engine.table.ShiftObliviousListener;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.engine.util.TableTools;
import io.deephaven.util.compare.DoubleComparisons;
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
import io.deephaven.engine.table.impl.util.compact.DoubleCompactKernel;
import io.deephaven.test.types.ParallelTest;
import io.deephaven.util.SafeCloseable;
import junit.framework.TestCase;
import org.apache.commons.lang3.mutable.MutableInt;
import org.jetbrains.annotations.NotNull;
import org.junit.experimental.categories.Category;

import java.util.Arrays;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import static io.deephaven.engine.table.impl.TstUtils.getTable;
import static io.deephaven.engine.table.impl.TstUtils.initColumnInfos;
import static io.deephaven.util.QueryConstants.NULL_DOUBLE;
import static org.junit.Assert.assertArrayEquals;

@Category(ParallelTest.class)
public class TestDoubleSegmentedSortedMultiset extends RefreshingTableTestCase {

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

    public void testPartialCopy() {
        final int nodeSize = 8;
        final DoubleSegmentedSortedMultiset ssm = new DoubleSegmentedSortedMultiset(nodeSize);

        final double[] data = new double[24];
        try (final WritableDoubleChunk<Values> valuesChunk = WritableDoubleChunk.makeWritableChunk(24);
             final WritableIntChunk<ChunkLengths> countsChunk = WritableIntChunk.makeWritableChunk(24)) {

            for (int ii = 0; ii < 24; ii++) {
                data[ii] = (double) ('a' + ii);
                countsChunk.set(ii, 1);
                valuesChunk.set(ii, data[ii]);
            }

            ssm.insert(valuesChunk, countsChunk);
        }

        assertArrayEquals(data, ssm.toArray(), .000001f);
        assertArrayEquals(data, ssm.subVector(0, 23).toArray(), .000001f);
        assertArrayEquals(Arrays.copyOfRange(data,0, 4), ssm.subVector(0, 3).toArray(), .000001f);
        assertArrayEquals(Arrays.copyOfRange(data, 0, 8), ssm.subVector(0, 7).toArray(), .000001f);
        assertArrayEquals(Arrays.copyOfRange(data, 0, 16), ssm.subVector(0, 15).toArray(), .000001f);

        assertArrayEquals(Arrays.copyOfRange(data, 2, 6), ssm.subVector(2, 5).toArray(), .000001f);
        assertArrayEquals(Arrays.copyOfRange(data, 2, 12), ssm.subVector(2, 11).toArray(), .000001f);
        assertArrayEquals(Arrays.copyOfRange(data, 7, 12), ssm.subVector(7, 11).toArray(), .000001f);
        assertArrayEquals(Arrays.copyOfRange(data, 7, 16), ssm.subVector(7, 15).toArray(), .000001f);
        assertArrayEquals(Arrays.copyOfRange(data, 11, 16), ssm.subVector(11, 15).toArray(), .000001f);
        assertArrayEquals(Arrays.copyOfRange(data, 2, 20), ssm.subVector(2, 19).toArray(), .000001f);
    }

    // region SortFixupSanityCheck
    public void testSanity() {
        QueryTable john = TstUtils.testRefreshingTable(TableTools.doubleCol("John", NULL_DOUBLE, NULL_DOUBLE, (double)0x0, (double)0x1, Double.MAX_VALUE, Double.MAX_VALUE));
        final ColumnSource<Double> valueSource = john.getColumnSource("John");
        try (final ColumnSource.FillContext fillContext = valueSource.makeFillContext(1024);
             final WritableDoubleChunk<Values> chunk = WritableDoubleChunk.makeWritableChunk(1024);
             final WritableIntChunk<ChunkLengths> counts = WritableIntChunk.makeWritableChunk(1024)
        ) {
            valueSource.fillChunk(fillContext, chunk, john.getRowSet());
            DoubleCompactKernel.compactAndCount(chunk, counts, true);
        }
    }
    //endregion SortFixupSanityCheck

    private void testUpdates(@NotNull final SsaTestHelpers.TestDescriptor desc, boolean allowAddition, boolean allowRemoval, boolean countNull) {
        final Random random = new Random(desc.seed());
        final TstUtils.ColumnInfo[] columnInfo;
        final QueryTable table = getTable(desc.tableSize(), random, columnInfo = initColumnInfos(new String[]{"Value"},
                SsaTestHelpers.getGeneratorForDouble()));

        final Table asDouble = SsaTestHelpers.prepareTestTableForDouble(table);

        final DoubleSegmentedSortedMultiset ssm = new DoubleSegmentedSortedMultiset(desc.nodeSize());

        //noinspection unchecked
        final ColumnSource<Double> valueSource = asDouble.getColumnSource("Value");

        checkSsmInitial(asDouble, ssm, valueSource, countNull, desc);

        try (final SafeCloseable ignored = LivenessScopeStack.open(new LivenessScope(true), true)) {
            final ShiftObliviousListener asDoubleListener = new ShiftObliviousInstrumentedListenerAdapter(asDouble, false) {
                @Override
                public void onUpdate(RowSet added, RowSet removed, RowSet modified) {
                    final int maxSize = Math.max(Math.max(added.intSize(), removed.intSize()), modified.intSize());
                    try (final ColumnSource.FillContext fillContext = valueSource.makeFillContext(maxSize);
                         final WritableDoubleChunk<Values> chunk = WritableDoubleChunk.makeWritableChunk(maxSize);
                         final WritableIntChunk<ChunkLengths> counts = WritableIntChunk.makeWritableChunk(maxSize)
                    ) {
                        final SegmentedSortedMultiSet.RemoveContext removeContext = SegmentedSortedMultiSet.makeRemoveContext(desc.nodeSize());

                        if (removed.isNonempty()) {
                            valueSource.fillPrevChunk(fillContext, chunk, removed);
                            DoubleCompactKernel.compactAndCount(chunk, counts, countNull);
                            ssm.remove(removeContext, chunk, counts);
                        }


                        if (added.isNonempty()) {
                            valueSource.fillChunk(fillContext, chunk, added);
                            DoubleCompactKernel.compactAndCount(chunk, counts, countNull);
                            ssm.insert(chunk, counts);
                        }
                    }
                }
            };
            asDouble.listenForUpdates(asDoubleListener);

            while (desc.advance(50)) {
                UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
                    final RowSet[] notify = GenerateTableUpdates.computeTableUpdates(desc.tableSize(), random, table, columnInfo, allowAddition, allowRemoval, false);
                    assertTrue(notify[2].isEmpty());
                    table.notifyListeners(notify[0], notify[1], notify[2]);
                });

                try (final ColumnSource.GetContext getContext = valueSource.makeGetContext(asDouble.intSize())) {
                    checkSsm(ssm, valueSource.getChunk(getContext, asDouble.getRowSet()).asDoubleChunk(), countNull, desc);
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
                SsaTestHelpers.getGeneratorForDouble()));

        final Table asDouble = SsaTestHelpers.prepareTestTableForDouble(table);

        final DoubleSegmentedSortedMultiset ssmLo = new DoubleSegmentedSortedMultiset(desc.nodeSize());
        final DoubleSegmentedSortedMultiset ssmHi = new DoubleSegmentedSortedMultiset(desc.nodeSize());

        //noinspection unchecked
        final ColumnSource<Double> valueSource = asDouble.getColumnSource("Value");

        checkSsmInitial(asDouble, ssmLo, valueSource, countNull, desc);
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

            try (final ColumnSource.FillContext fillContext = valueSource.makeFillContext(asDouble.intSize());
                 final WritableDoubleChunk<Values> valueChunk = WritableDoubleChunk.makeWritableChunk(asDouble.intSize())) {
                valueSource.fillChunk(fillContext, valueChunk, asDouble.getRowSet());
                valueChunk.sort();
                final DoubleChunk<? extends Values> loValues = valueChunk.slice(0, LongSizedDataStructure.intSize("ssmLo", ssmLo.totalSize()));
                final DoubleChunk<? extends Values> hiValues = valueChunk.slice(LongSizedDataStructure.intSize("ssmLo", ssmLo.totalSize()), LongSizedDataStructure.intSize("ssmHi", ssmHi.totalSize()));
                checkSsm(ssmLo, loValues, countNull, desc);
                checkSsm(ssmHi, hiValues, countNull, desc);
            }
        }

        checkSsm(asDouble, ssmHi, valueSource, countNull, desc);

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

        checkSsm(asDouble, ssmLo, valueSource, countNull, desc);
    }

    private void checkSsmInitial(Table asDouble, DoubleSegmentedSortedMultiset ssm, ColumnSource<?> valueSource, boolean countNull, @NotNull final SsaTestHelpers.TestDescriptor desc) {
        try (final ColumnSource.FillContext fillContext = valueSource.makeFillContext(asDouble.intSize());
             final WritableDoubleChunk<Values> valueChunk = WritableDoubleChunk.makeWritableChunk(asDouble.intSize());
             final WritableIntChunk<ChunkLengths> counts = WritableIntChunk.makeWritableChunk(asDouble.intSize())) {
            valueSource.fillChunk(fillContext, valueChunk, asDouble.getRowSet());
            valueChunk.sort();

            DoubleCompactKernel.compactAndCount(valueChunk, counts, countNull);

            ssm.insert(valueChunk, counts);

            valueSource.fillChunk(fillContext, valueChunk, asDouble.getRowSet());
            checkSsm(ssm, valueChunk, countNull, desc);
        }
    }

    private void checkSsm(Table asDouble, DoubleSegmentedSortedMultiset ssm, ColumnSource<?> valueSource, boolean countNull, @NotNull final SsaTestHelpers.TestDescriptor desc) {
        try (final ColumnSource.FillContext fillContext = valueSource.makeFillContext(asDouble.intSize());
             final WritableDoubleChunk<Values> valueChunk = WritableDoubleChunk.makeWritableChunk(asDouble.intSize())) {
            valueSource.fillChunk(fillContext, valueChunk, asDouble.getRowSet());
            checkSsm(ssm, valueChunk, countNull, desc);
        }
    }

    private void checkSsm(DoubleSegmentedSortedMultiset ssm, DoubleChunk<? extends Values> valueChunk, boolean countNull, @NotNull final SsaTestHelpers.TestDescriptor desc) {
        try {
            ssm.validate();
            try (final WritableDoubleChunk<?> keys = ssm.keyChunk();
                 final WritableLongChunk<?> counts = ssm.countChunk()) {

                int totalSize = 0;

                final Map<Double, Integer> checkMap = new TreeMap<>(DoubleComparisons::compare);
                for (int ii = 0; ii < valueChunk.size(); ++ii) {
                    final double value = valueChunk.get(ii);
                    if (value == NULL_DOUBLE && !countNull) {
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
                    assertEquals((double) key, keys.get(offset.intValue()));
                    assertEquals((long) count, counts.get(offset.intValue()));
                    offset.increment();
                });
            }
        } catch (AssertionFailure e) {
            TestCase.fail("Check failed at " + desc + ": " + e.getMessage());
        }
    }
}

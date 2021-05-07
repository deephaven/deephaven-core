/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit TestCharSegmentedSortedMultiset and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.db.v2.ssms;

import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.tables.utils.TableTools;
import io.deephaven.db.util.DhShortComparisons;
import io.deephaven.db.util.LongSizedDataStructure;
import io.deephaven.db.v2.*;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.sources.chunk.Attributes.ChunkLengths;
import io.deephaven.db.v2.sources.chunk.Attributes.Values;
import io.deephaven.db.v2.ssa.SsaTestHelpers;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.db.v2.utils.compact.ShortCompactKernel;
import io.deephaven.test.types.ParallelTest;
import junit.framework.TestCase;
import org.apache.commons.lang3.mutable.MutableInt;
import org.junit.experimental.categories.Category;

import java.util.Arrays;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import static io.deephaven.db.v2.TstUtils.getTable;
import static io.deephaven.db.v2.TstUtils.initColumnInfos;
import static io.deephaven.util.QueryConstants.NULL_SHORT;
import static org.junit.Assert.assertArrayEquals;

@Category(ParallelTest.class)
public class TestShortSegmentedSortedMultiset extends LiveTableTestCase {
    public void testInsertion() {
        for (int seed = 0; seed < 10; ++seed) {
            for (int tableSize = 10; tableSize <= 1000; tableSize *= 10) {
                for (int nodeSize = 8; nodeSize <= 2048; nodeSize *= 2) {
                    testUpdates(seed, tableSize, nodeSize, true, false, true);
                }
            }
        }
    }

    public void testRemove() {
        for (int seed = 0; seed < 10; ++seed) {
            for (int tableSize = 10; tableSize <= 1000; tableSize *= 10) {
                for (int nodeSize = 8; nodeSize <= 2048; nodeSize *= 2) {
                    testUpdates(seed, tableSize, nodeSize, false, true, true);
                }
            }
        }
    }

    public void testInsertAndRemove() {
        for (int tableSize = 10; tableSize <= 1000; tableSize *= 2) {
            for (int nodeSize = 8; nodeSize <= 2048; nodeSize *= 2) {
                for (int seed = 0; seed < 100; ++seed) {
                    testUpdates(seed, tableSize, nodeSize, true, true, true);
                }
            }
        }
    }

    public void testMove() {
        for (int tableSize = 10; tableSize <= 10000; tableSize *= 2) {
            for (int nodeSize = 8; nodeSize <= 2048; nodeSize *= 2) {
                for (int seed = 0; seed < 200; ++seed) {
                    testMove(seed, tableSize, nodeSize, true);
                }
            }
        }
    }

    public void testPartialCopy() {
        final int nodeSize = 8;
        final ShortSegmentedSortedMultiset ssm = new ShortSegmentedSortedMultiset(nodeSize);

        final short[] data = new short[24];
        final WritableShortChunk<Values> valuesChunk = WritableShortChunk.makeWritableChunk(24);
        final WritableIntChunk<ChunkLengths> countsChunk = WritableIntChunk.makeWritableChunk(24);
        for(int ii = 0; ii < 24; ii++) {
            data[ii] = (short)('a' + ii);
            countsChunk.set(ii, 1);
            valuesChunk.set(ii, data[ii]);
        }

        ssm.insert(valuesChunk, countsChunk);

        assertArrayEquals(data, ssm.toArray()/*EXTRA*/);
        assertArrayEquals(data, ssm.subArray(0, 23).toArray()/*EXTRA*/);
        assertArrayEquals(Arrays.copyOfRange(data,0, 4), ssm.subArray(0, 3).toArray()/*EXTRA*/);
        assertArrayEquals(Arrays.copyOfRange(data, 0, 8), ssm.subArray(0, 7).toArray()/*EXTRA*/);
        assertArrayEquals(Arrays.copyOfRange(data, 0, 16), ssm.subArray(0, 15).toArray()/*EXTRA*/);

        assertArrayEquals(Arrays.copyOfRange(data, 2, 6), ssm.subArray(2, 5).toArray()/*EXTRA*/);
        assertArrayEquals(Arrays.copyOfRange(data, 2, 12), ssm.subArray(2, 11).toArray()/*EXTRA*/);
        assertArrayEquals(Arrays.copyOfRange(data, 7, 12), ssm.subArray(7, 11).toArray()/*EXTRA*/);
        assertArrayEquals(Arrays.copyOfRange(data, 7, 16), ssm.subArray(7, 15).toArray()/*EXTRA*/);
        assertArrayEquals(Arrays.copyOfRange(data, 11, 16), ssm.subArray(11, 15).toArray()/*EXTRA*/);
        assertArrayEquals(Arrays.copyOfRange(data, 2, 20), ssm.subArray(2, 19).toArray()/*EXTRA*/);
    }

    // region SortFixupSanityCheck
    public void testSanity() {
        QueryTable john = TstUtils.testRefreshingTable(TableTools.shortCol("John", NULL_SHORT, NULL_SHORT, (short)0x0, (short)0x1, Short.MAX_VALUE, Short.MAX_VALUE));
        final ColumnSource<Short> valueSource = john.getColumnSource("John");
        try (final ColumnSource.FillContext fillContext = valueSource.makeFillContext(1024);
             final WritableShortChunk<Values> chunk = WritableShortChunk.makeWritableChunk(1024);
             final WritableIntChunk<ChunkLengths> counts = WritableIntChunk.makeWritableChunk(1024)
        ) {
            valueSource.fillChunk(fillContext, chunk, john.getIndex());
            ShortCompactKernel.compactAndCount(chunk, counts, true);
        }
    }
    //endregion SortFixupSanityCheck

    private void testUpdates(final int seed, final int tableSize, final int nodeSize, boolean allowAddition, boolean allowRemoval, boolean countNull) {
        final Random random = new Random(seed);
        final TstUtils.ColumnInfo[] columnInfo;
        final QueryTable table = getTable(tableSize, random, columnInfo = initColumnInfos(new String[]{"Value"},
                SsaTestHelpers.getGeneratorForShort()));

        final Table asShort = SsaTestHelpers.prepareTestTableForShort(table);

        final ShortSegmentedSortedMultiset ssm = new ShortSegmentedSortedMultiset(nodeSize);

        //noinspection unchecked
        final ColumnSource<Short> valueSource = asShort.getColumnSource("Value");

        System.out.println("Creation seed=" + seed + ", tableSize=" + tableSize + ", nodeSize=" + nodeSize);
        checkSsmInitial(asShort, ssm, valueSource, countNull);

        ((DynamicTable)asShort).listenForUpdates(new InstrumentedListenerAdapter((DynamicTable) asShort) {
            @Override
            public void onUpdate(Index added, Index removed, Index modified) {
                final int maxSize = Math.max(Math.max(added.intSize(), removed.intSize()), modified.intSize());
                try (final ColumnSource.FillContext fillContext = valueSource.makeFillContext(maxSize);
                     final WritableShortChunk<Values> chunk = WritableShortChunk.makeWritableChunk(maxSize);
                     final WritableIntChunk<ChunkLengths> counts = WritableIntChunk.makeWritableChunk(maxSize)
                ) {
                    final SegmentedSortedMultiSet.RemoveContext removeContext = SegmentedSortedMultiSet.makeRemoveContext(nodeSize);

                    if (removed.nonempty()) {
                        valueSource.fillPrevChunk(fillContext, chunk, removed);
                        ShortCompactKernel.compactAndCount(chunk, counts, countNull);
                        ssm.remove(removeContext, chunk, counts);
                    }


                    if (added.nonempty()) {
                        valueSource.fillChunk(fillContext, chunk, added);
                        ShortCompactKernel.compactAndCount(chunk, counts, countNull);
                        ssm.insert(chunk, counts);
                    }
                }
            }
        });

        for (int step = 0; step < 50; ++step) {
            System.out.println("Seed = " + seed + ", tableSize=" + tableSize + ", nodeSize=" + nodeSize + ", step = " + step);
            LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
                final Index [] notify = GenerateTableUpdates.computeTableUpdates(tableSize, random, table, columnInfo, allowAddition, allowRemoval, false);
                assertTrue(notify[2].empty());
                table.notifyListeners(notify[0], notify[1], notify[2]);
            });

            try (final ColumnSource.GetContext getContext = valueSource.makeGetContext(asShort.intSize())) {
                checkSsm(ssm, valueSource.getChunk(getContext, asShort.getIndex()).asShortChunk(), countNull);
            }

            if (!allowAddition && table.size() == 0) {
                System.out.println("All values removed.");
                break;
            }
        }

    }

    private void testMove(final int seed, final int tableSize, final int nodeSize, boolean countNull) {
        final Random random = new Random(seed);
        final QueryTable table = getTable(tableSize, random, initColumnInfos(new String[]{"Value"},
                SsaTestHelpers.getGeneratorForShort()));

        final Table asShort = SsaTestHelpers.prepareTestTableForShort(table);

        final ShortSegmentedSortedMultiset ssmLo = new ShortSegmentedSortedMultiset(nodeSize);
        final ShortSegmentedSortedMultiset ssmHi = new ShortSegmentedSortedMultiset(nodeSize);

        //noinspection unchecked
        final ColumnSource<Short> valueSource = asShort.getColumnSource("Value");

        System.out.println("Creation seed=" + seed + ", tableSize=" + tableSize + ", nodeSize=" + nodeSize +", actual size=" + asShort.size());
        checkSsmInitial(asShort, ssmLo, valueSource, countNull);
        final long totalExpectedSize = ssmLo.totalSize();

        while (ssmLo.size() > 0) {
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

            try (final ColumnSource.FillContext fillContext = valueSource.makeFillContext(asShort.intSize());
                 final WritableShortChunk<Attributes.Values> valueChunk = WritableShortChunk.makeWritableChunk(asShort.intSize())) {
                valueSource.fillChunk(fillContext, valueChunk, asShort.getIndex());
                valueChunk.sort();
                final ShortChunk<? extends Values> loValues = valueChunk.slice(0, LongSizedDataStructure.intSize("ssmLo", ssmLo.totalSize()));
                final ShortChunk<? extends Values> hiValues = valueChunk.slice(LongSizedDataStructure.intSize("ssmLo", ssmLo.totalSize()), LongSizedDataStructure.intSize("ssmHi", ssmHi.totalSize()));
                checkSsm(ssmLo, loValues, countNull);
                checkSsm(ssmHi, hiValues, countNull);
            }

        }

        System.out.println("All lo elements moved to hi.");
        checkSsm(asShort, ssmHi, valueSource, countNull);

        while (ssmHi.size() > 0) {
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
        }

        System.out.println("All hi elements moved to lo.");
        checkSsm(asShort, ssmLo, valueSource, countNull);
    }

    private void checkSsmInitial(Table asShort, ShortSegmentedSortedMultiset ssm, ColumnSource<?> valueSource, boolean countNull) {
        try (final ColumnSource.FillContext fillContext = valueSource.makeFillContext(asShort.intSize());
             final WritableShortChunk<Attributes.Values> valueChunk = WritableShortChunk.makeWritableChunk(asShort.intSize());
             final WritableIntChunk<ChunkLengths> counts = WritableIntChunk.makeWritableChunk(asShort.intSize())) {
            valueSource.fillChunk(fillContext, valueChunk, asShort.getIndex());
            valueChunk.sort();

            ShortCompactKernel.compactAndCount(valueChunk, counts, countNull);

            ssm.insert(valueChunk, counts);

            valueSource.fillChunk(fillContext, valueChunk, asShort.getIndex());
            checkSsm(ssm, valueChunk, countNull);
        }
    }

    private void checkSsm(Table asShort, ShortSegmentedSortedMultiset ssm, ColumnSource<?> valueSource, boolean countNull) {
        try (final ColumnSource.FillContext fillContext = valueSource.makeFillContext(asShort.intSize());
             final WritableShortChunk<Attributes.Values> valueChunk = WritableShortChunk.makeWritableChunk(asShort.intSize())) {
            valueSource.fillChunk(fillContext, valueChunk, asShort.getIndex());
            checkSsm(ssm, valueChunk, countNull);
        }
    }

    private void checkSsm(ShortSegmentedSortedMultiset ssm, ShortChunk<? extends Values> valueChunk, boolean countNull) {
        ssm.validate();
        final ShortChunk<?> keys = ssm.keyChunk();
        final LongChunk<?> counts = ssm.countChunk();
        int totalSize = 0;

        final Map<Short, Integer> checkMap = new TreeMap<>(DhShortComparisons::compare);
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
            TestCase.assertEquals((short)key, keys.get(offset.intValue()));
            TestCase.assertEquals((long)count, counts.get(offset.intValue()));
            offset.increment();
        });
    }
}

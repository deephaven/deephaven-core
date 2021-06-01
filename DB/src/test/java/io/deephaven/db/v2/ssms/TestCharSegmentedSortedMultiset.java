package io.deephaven.db.v2.ssms;

import io.deephaven.base.verify.AssertionFailure;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.tables.utils.TableTools;
import io.deephaven.db.util.DhCharComparisons;
import io.deephaven.db.util.LongSizedDataStructure;
import io.deephaven.db.util.liveness.LivenessScope;
import io.deephaven.db.util.liveness.LivenessScopeStack;
import io.deephaven.db.v2.*;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.sources.chunk.Attributes.ChunkLengths;
import io.deephaven.db.v2.sources.chunk.Attributes.Values;
import io.deephaven.db.v2.ssa.SsaTestHelpers;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.db.v2.utils.compact.CharCompactKernel;
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

import static io.deephaven.db.v2.TstUtils.getTable;
import static io.deephaven.db.v2.TstUtils.initColumnInfos;
import static io.deephaven.util.QueryConstants.NULL_CHAR;
import static org.junit.Assert.assertArrayEquals;

@Category(ParallelTest.class)
public class TestCharSegmentedSortedMultiset extends LiveTableTestCase {

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
        for (int tableSize = 10; tableSize <= 1000; tableSize *= 2) {
            for (int nodeSize = 8; nodeSize <= 2048; nodeSize *= 2) {
                for (int seed = 0; seed < 100; ++seed) {
                    testUpdates(desc.reset(seed, tableSize, nodeSize), true, true, true);
                }
            }
        }
    }

    public void testMove() {
        final SsaTestHelpers.TestDescriptor desc = new SsaTestHelpers.TestDescriptor();
        for (int tableSize = 10; tableSize <= 10000; tableSize *= 2) {
            for (int nodeSize = 8; nodeSize <= 2048; nodeSize *= 2) {
                for (int seed = 0; seed < 200; ++seed) {
                    testMove(desc.reset(seed, tableSize, nodeSize), true);
                }
            }
        }
    }

    public void testPartialCopy() {
        final int nodeSize = 8;
        final CharSegmentedSortedMultiset ssm = new CharSegmentedSortedMultiset(nodeSize);

        final char[] data = new char[24];
        final WritableCharChunk<Values> valuesChunk = WritableCharChunk.makeWritableChunk(24);
        final WritableIntChunk<ChunkLengths> countsChunk = WritableIntChunk.makeWritableChunk(24);
        for(int ii = 0; ii < 24; ii++) {
            data[ii] = (char)('a' + ii);
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
        QueryTable john = TstUtils.testRefreshingTable(TableTools.charCol("John", NULL_CHAR, NULL_CHAR, (char)0x0, (char)0x1, Character.MAX_VALUE, Character.MAX_VALUE));
        final ColumnSource<Character> valueSource = john.getColumnSource("John");
        try (final ColumnSource.FillContext fillContext = valueSource.makeFillContext(1024);
             final WritableCharChunk<Values> chunk = WritableCharChunk.makeWritableChunk(1024);
             final WritableIntChunk<ChunkLengths> counts = WritableIntChunk.makeWritableChunk(1024)
        ) {
            valueSource.fillChunk(fillContext, chunk, john.getIndex());
            CharCompactKernel.compactAndCount(chunk, counts, true);
        }
    }
    //endregion SortFixupSanityCheck

    private void testUpdates(@NotNull final SsaTestHelpers.TestDescriptor desc, boolean allowAddition, boolean allowRemoval, boolean countNull) {
        final Random random = new Random(desc.seed());
        final TstUtils.ColumnInfo[] columnInfo;
        final QueryTable table = getTable(desc.tableSize(), random, columnInfo = initColumnInfos(new String[]{"Value"},
                SsaTestHelpers.getGeneratorForChar()));

        final Table asCharacter = SsaTestHelpers.prepareTestTableForChar(table);

        final CharSegmentedSortedMultiset ssm = new CharSegmentedSortedMultiset(desc.nodeSize());

        //noinspection unchecked
        final ColumnSource<Character> valueSource = asCharacter.getColumnSource("Value");

        checkSsmInitial(asCharacter, ssm, valueSource, countNull, desc);

        try (final SafeCloseable ignored = LivenessScopeStack.open(new LivenessScope(true), true)) {
            final Listener asCharacterListener = new InstrumentedListenerAdapter((DynamicTable) asCharacter, false) {
                @Override
                public void onUpdate(Index added, Index removed, Index modified) {
                    final int maxSize = Math.max(Math.max(added.intSize(), removed.intSize()), modified.intSize());
                    try (final ColumnSource.FillContext fillContext = valueSource.makeFillContext(maxSize);
                         final WritableCharChunk<Values> chunk = WritableCharChunk.makeWritableChunk(maxSize);
                         final WritableIntChunk<ChunkLengths> counts = WritableIntChunk.makeWritableChunk(maxSize)
                    ) {
                        final SegmentedSortedMultiSet.RemoveContext removeContext = SegmentedSortedMultiSet.makeRemoveContext(desc.nodeSize());

                        if (removed.nonempty()) {
                            valueSource.fillPrevChunk(fillContext, chunk, removed);
                            CharCompactKernel.compactAndCount(chunk, counts, countNull);
                            ssm.remove(removeContext, chunk, counts);
                        }


                        if (added.nonempty()) {
                            valueSource.fillChunk(fillContext, chunk, added);
                            CharCompactKernel.compactAndCount(chunk, counts, countNull);
                            ssm.insert(chunk, counts);
                        }
                    }
                }
            };
            ((DynamicTable) asCharacter).listenForUpdates(asCharacterListener);

            while (desc.advance(50)) {
                LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
                    final Index[] notify = GenerateTableUpdates.computeTableUpdates(desc.tableSize(), random, table, columnInfo, allowAddition, allowRemoval, false);
                    assertTrue(notify[2].empty());
                    table.notifyListeners(notify[0], notify[1], notify[2]);
                });

                try (final ColumnSource.GetContext getContext = valueSource.makeGetContext(asCharacter.intSize())) {
                    checkSsm(ssm, valueSource.getChunk(getContext, asCharacter.getIndex()).asCharChunk(), countNull, desc);
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
                SsaTestHelpers.getGeneratorForChar()));

        final Table asCharacter = SsaTestHelpers.prepareTestTableForChar(table);

        final CharSegmentedSortedMultiset ssmLo = new CharSegmentedSortedMultiset(desc.nodeSize());
        final CharSegmentedSortedMultiset ssmHi = new CharSegmentedSortedMultiset(desc.nodeSize());

        //noinspection unchecked
        final ColumnSource<Character> valueSource = asCharacter.getColumnSource("Value");

        checkSsmInitial(asCharacter, ssmLo, valueSource, countNull, desc);
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

            try (final ColumnSource.FillContext fillContext = valueSource.makeFillContext(asCharacter.intSize());
                 final WritableCharChunk<Attributes.Values> valueChunk = WritableCharChunk.makeWritableChunk(asCharacter.intSize())) {
                valueSource.fillChunk(fillContext, valueChunk, asCharacter.getIndex());
                valueChunk.sort();
                final CharChunk<? extends Values> loValues = valueChunk.slice(0, LongSizedDataStructure.intSize("ssmLo", ssmLo.totalSize()));
                final CharChunk<? extends Values> hiValues = valueChunk.slice(LongSizedDataStructure.intSize("ssmLo", ssmLo.totalSize()), LongSizedDataStructure.intSize("ssmHi", ssmHi.totalSize()));
                checkSsm(ssmLo, loValues, countNull, desc);
                checkSsm(ssmHi, hiValues, countNull, desc);
            }
        }

        checkSsm(asCharacter, ssmHi, valueSource, countNull, desc);

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

        checkSsm(asCharacter, ssmLo, valueSource, countNull, desc);
    }

    private void checkSsmInitial(Table asCharacter, CharSegmentedSortedMultiset ssm, ColumnSource<?> valueSource, boolean countNull, @NotNull final SsaTestHelpers.TestDescriptor desc) {
        try (final ColumnSource.FillContext fillContext = valueSource.makeFillContext(asCharacter.intSize());
             final WritableCharChunk<Attributes.Values> valueChunk = WritableCharChunk.makeWritableChunk(asCharacter.intSize());
             final WritableIntChunk<ChunkLengths> counts = WritableIntChunk.makeWritableChunk(asCharacter.intSize())) {
            valueSource.fillChunk(fillContext, valueChunk, asCharacter.getIndex());
            valueChunk.sort();

            CharCompactKernel.compactAndCount(valueChunk, counts, countNull);

            ssm.insert(valueChunk, counts);

            valueSource.fillChunk(fillContext, valueChunk, asCharacter.getIndex());
            checkSsm(ssm, valueChunk, countNull, desc);
        }
    }

    private void checkSsm(Table asCharacter, CharSegmentedSortedMultiset ssm, ColumnSource<?> valueSource, boolean countNull, @NotNull final SsaTestHelpers.TestDescriptor desc) {
        try (final ColumnSource.FillContext fillContext = valueSource.makeFillContext(asCharacter.intSize());
             final WritableCharChunk<Attributes.Values> valueChunk = WritableCharChunk.makeWritableChunk(asCharacter.intSize())) {
            valueSource.fillChunk(fillContext, valueChunk, asCharacter.getIndex());
            checkSsm(ssm, valueChunk, countNull, desc);
        }
    }

    private void checkSsm(CharSegmentedSortedMultiset ssm, CharChunk<? extends Values> valueChunk, boolean countNull, @NotNull final SsaTestHelpers.TestDescriptor desc) {
        try {
            ssm.validate();
            final CharChunk<?> keys = ssm.keyChunk();
            final LongChunk<?> counts = ssm.countChunk();
            int totalSize = 0;

            final Map<Character, Integer> checkMap = new TreeMap<>(DhCharComparisons::compare);
            for (int ii = 0; ii < valueChunk.size(); ++ii) {
                final char value = valueChunk.get(ii);
                if (value == NULL_CHAR && !countNull) {
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
                assertEquals((char) key, keys.get(offset.intValue()));
                assertEquals((long) count, counts.get(offset.intValue()));
                offset.increment();
            });
        } catch (AssertionFailure e) {
            TestCase.fail("Check failed at " + desc + ": " + e.getMessage());
        }
    }
}

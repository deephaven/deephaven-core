package io.deephaven.db.v2.ssa;

import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.v2.*;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.Attributes.KeyIndices;
import io.deephaven.db.v2.sources.chunk.Attributes.Values;
import io.deephaven.db.v2.sources.chunk.CharChunk;
import io.deephaven.db.v2.sources.chunk.LongChunk;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.db.v2.utils.IndexShiftData;
import io.deephaven.test.types.ParallelTest;
import junit.framework.TestCase;
import org.junit.experimental.categories.Category;

import java.util.Random;

import static io.deephaven.db.v2.TstUtils.getTable;
import static io.deephaven.db.v2.TstUtils.initColumnInfos;

@Category(ParallelTest.class)
public class TestCharSegmentedSortedArray extends LiveTableTestCase {
    public void testInsertion() {
        for (int seed = 0; seed < 10; ++seed) {
            for (int tableSize = 100; tableSize <= 1000; tableSize *= 10) {
                for (int nodeSize = 8; nodeSize <= 2048; nodeSize *= 2) {
                    testUpdates(seed, tableSize, nodeSize, true, false);
                }
            }
        }
    }

    public void testRemove() {
        for (int seed = 0; seed < 20; ++seed) {
            for (int tableSize = 100; tableSize <= 10000; tableSize *= 10) {
                for (int nodeSize = 16; nodeSize <= 2048; nodeSize *= 2) {
                    testUpdates(seed, tableSize, nodeSize, false, true);
                }
            }
        }
    }

    public void testInsertAndRemove() {
        for (int seed = 0; seed < 10; ++seed) {
            for (int tableSize = 100; tableSize <= 10000; tableSize *= 10) {
                for (int nodeSize = 16; nodeSize <= 2048; nodeSize *= 2) {
                    testUpdates(seed, tableSize, nodeSize, true, true);
                }
            }
        }
    }

    public void testShifts() {
        for (int seed = 0; seed < 20; ++seed) {
            for (int tableSize = 10; tableSize <= 10000; tableSize *= 10) {
                for (int nodeSize = 16; nodeSize <= 2048; nodeSize *= 2) {
                    testShifts(seed, tableSize, nodeSize);
                }
            }
        }
    }

    private void testShifts(final int seed, final int tableSize, final int nodeSize) {
        final Random random = new Random(seed);
        final TstUtils.ColumnInfo[] columnInfo;
        final QueryTable table = getTable(tableSize, random, columnInfo = initColumnInfos(new String[]{"Value"},
                SsaTestHelpers.getGeneratorForChar()));

        final Table asCharacter = SsaTestHelpers.prepareTestTableForChar(table);

        final CharSegmentedSortedArray ssa = new CharSegmentedSortedArray(nodeSize);

        //noinspection unchecked
        final ColumnSource<Character> valueSource = asCharacter.getColumnSource("Value");

        System.out.println("Creation seed=" + seed + ", tableSize=" + tableSize + ", nodeSize=" + nodeSize);
        checkSsaInitial(asCharacter, ssa, valueSource);

        ((DynamicTable)asCharacter).listenForUpdates(new InstrumentedShiftAwareListenerAdapter((DynamicTable) asCharacter) {
            @Override
            public void onUpdate(Update upstream) {
                try (final ColumnSource.GetContext checkContext = valueSource.makeGetContext(asCharacter.getIndex().getPrevIndex().intSize())) {
                    final Index relevantIndices = asCharacter.getIndex().getPrevIndex();
                    checkSsa(ssa, valueSource.getPrevChunk(checkContext, relevantIndices).asCharChunk(), relevantIndices.asKeyIndicesChunk());
                }

                final int size = Math.max(upstream.modified.intSize() +  Math.max(upstream.added.intSize(), upstream.removed.intSize()), (int)upstream.shifted.getEffectiveSize());
                try (final ColumnSource.GetContext getContext = valueSource.makeGetContext(size)) {
                    ssa.validate();

                    final Index takeout = upstream.removed.union(upstream.getModifiedPreShift());
                    if (takeout.nonempty()) {
                        final CharChunk<? extends Values> valuesToRemove = valueSource.getPrevChunk(getContext, takeout).asCharChunk();
                        ssa.remove(valuesToRemove, takeout.asKeyIndicesChunk());
                    }

                    ssa.validate();

                    try (final ColumnSource.GetContext checkContext = valueSource.makeGetContext(asCharacter.getIndex().getPrevIndex().intSize())) {
                        final Index relevantIndices = asCharacter.getIndex().getPrevIndex().minus(takeout);
                        checkSsa(ssa, valueSource.getPrevChunk(checkContext, relevantIndices).asCharChunk(), relevantIndices.asKeyIndicesChunk());
                    }

                    if (upstream.shifted.nonempty()) {
                        final IndexShiftData.Iterator sit = upstream.shifted.applyIterator();
                        while (sit.hasNext()) {
                            sit.next();
                            final Index indexToShift = table.getIndex().getPrevIndex().subindexByKey(sit.beginRange(), sit.endRange()).minus(upstream.getModifiedPreShift()).minus(upstream.removed);
                            if (indexToShift.empty()) {
                                continue;
                            }

                            final CharChunk<? extends Values> shiftValues = valueSource.getPrevChunk(getContext, indexToShift).asCharChunk();

                            if (sit.polarityReversed()) {
                                ssa.applyShiftReverse(shiftValues, indexToShift.asKeyIndicesChunk(), sit.shiftDelta());
                            } else {
                                ssa.applyShift(shiftValues, indexToShift.asKeyIndicesChunk(), sit.shiftDelta());
                            }
                        }
                    }

                    ssa.validate();

                    final Index putin = upstream.added.union(upstream.modified);

                    try (final ColumnSource.GetContext checkContext = valueSource.makeGetContext(asCharacter.intSize())) {
                        final Index relevantIndices = asCharacter.getIndex().minus(putin);
                        checkSsa(ssa, valueSource.getChunk(checkContext, relevantIndices).asCharChunk(), relevantIndices.asKeyIndicesChunk());
                    }

                    if (putin.nonempty()) {
                        final CharChunk<? extends Values> valuesToInsert = valueSource.getChunk(getContext, putin).asCharChunk();
                        ssa.insert(valuesToInsert, putin.asKeyIndicesChunk());
                    }

                    ssa.validate();
                }
            }
        });

        for (int step = 0; step < 50; ++step) {
            System.out.println("Seed = " + seed + ", tableSize=" + tableSize + ", nodeSize=" + nodeSize + ", step = " + step);
            LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() ->
                    GenerateTableUpdates.generateShiftAwareTableUpdates(GenerateTableUpdates.DEFAULT_PROFILE, tableSize, random, table, columnInfo));

            try (final ColumnSource.GetContext getContext = valueSource.makeGetContext(asCharacter.intSize())) {
                checkSsa(ssa, valueSource.getChunk(getContext, asCharacter.getIndex()).asCharChunk(), asCharacter.getIndex().asKeyIndicesChunk());
            }
        }
    }

    private void testUpdates(final int seed, final int tableSize, final int nodeSize, boolean allowAddition, boolean allowRemoval) {
        final Random random = new Random(seed);
        final TstUtils.ColumnInfo[] columnInfo;
        final QueryTable table = getTable(tableSize, random, columnInfo = initColumnInfos(new String[]{"Value"},
                SsaTestHelpers.getGeneratorForChar()));

        final Table asCharacter = SsaTestHelpers.prepareTestTableForChar(table);

        final CharSegmentedSortedArray ssa = new CharSegmentedSortedArray(nodeSize);

        //noinspection unchecked
        final ColumnSource<Character> valueSource = asCharacter.getColumnSource("Value");

        System.out.println("Creation seed=" + seed + ", tableSize=" + tableSize + ", nodeSize=" + nodeSize);
        checkSsaInitial(asCharacter, ssa, valueSource);

        ((DynamicTable)asCharacter).listenForUpdates(new InstrumentedListenerAdapter((DynamicTable) asCharacter) {
            @Override
            public void onUpdate(Index added, Index removed, Index modified) {
                try (final ColumnSource.GetContext getContext = valueSource.makeGetContext(Math.max(added.intSize(), removed.intSize()))) {
                    if (removed.nonempty()) {
                        final CharChunk<? extends Values> valuesToRemove = valueSource.getPrevChunk(getContext, removed).asCharChunk();
                        ssa.remove(valuesToRemove, removed.asKeyIndicesChunk());
                    }
                    if (added.nonempty()) {
                        ssa.insert(valueSource.getChunk(getContext, added).asCharChunk(), added.asKeyIndicesChunk());
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

            try (final ColumnSource.GetContext getContext = valueSource.makeGetContext(asCharacter.intSize())) {
                checkSsa(ssa, valueSource.getChunk(getContext, asCharacter.getIndex()).asCharChunk(), asCharacter.getIndex().asKeyIndicesChunk());
            }

            if (!allowAddition && table.size() == 0) {
                System.out.println("All values removed.");
                break;
            }
        }

    }

    private void checkSsaInitial(Table asCharacter, CharSegmentedSortedArray ssa, ColumnSource<?> valueSource) {
        try (final ColumnSource.GetContext getContext = valueSource.makeGetContext(asCharacter.intSize())) {
            final CharChunk<? extends Values> valueChunk = valueSource.getChunk(getContext, asCharacter.getIndex()).asCharChunk();
            final LongChunk<Attributes.OrderedKeyIndices> tableIndexChunk = asCharacter.getIndex().asKeyIndicesChunk();

            ssa.insert(valueChunk, tableIndexChunk);

            checkSsa(ssa, valueChunk, tableIndexChunk);
        }
    }

    private void checkSsa(CharSegmentedSortedArray ssa, CharChunk<? extends Values> valueChunk, LongChunk<? extends KeyIndices> tableIndexChunk) {
        ssa.validate();

        try {
            CharSsaChecker.checkSsa(ssa, valueChunk, tableIndexChunk);
        } catch (SsaChecker.SsaCheckException e) {
            TestCase.fail(e.getMessage());
        }
    }
}

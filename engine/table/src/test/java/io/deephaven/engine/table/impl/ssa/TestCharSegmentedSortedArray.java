package io.deephaven.engine.table.impl.ssa;

import io.deephaven.base.verify.AssertionFailure;
import io.deephaven.engine.table.*;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.engine.liveness.LivenessScope;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.table.impl.*;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.CharChunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.test.types.ParallelTest;
import io.deephaven.util.SafeCloseable;
import junit.framework.TestCase;
import org.jetbrains.annotations.NotNull;
import org.junit.experimental.categories.Category;

import java.util.Random;

import static io.deephaven.engine.table.impl.TstUtils.*;

@Category(ParallelTest.class)
public class TestCharSegmentedSortedArray extends RefreshingTableTestCase {

    public void testInsertion() {
        final SsaTestHelpers.TestDescriptor desc = new SsaTestHelpers.TestDescriptor();
        for (int seed = 0; seed < 10; ++seed) {
            for (int tableSize = 100; tableSize <= 1000; tableSize *= 10) {
                for (int nodeSize = 8; nodeSize <= 2048; nodeSize *= 2) {
                    testUpdates(desc.reset(seed, tableSize, nodeSize), true, false);
                }
            }
        }
    }

    public void testRemove() {
        final SsaTestHelpers.TestDescriptor desc = new SsaTestHelpers.TestDescriptor();
        for (int seed = 0; seed < 20; ++seed) {
            for (int tableSize = 100; tableSize <= 10000; tableSize *= 10) {
                for (int nodeSize = 16; nodeSize <= 2048; nodeSize *= 2) {
                    testUpdates(desc.reset(seed, tableSize, nodeSize), false, true);
                }
            }
        }
    }

    public void testInsertAndRemove() {
        final SsaTestHelpers.TestDescriptor desc = new SsaTestHelpers.TestDescriptor();
        for (int seed = 0; seed < 10; ++seed) {
            for (int tableSize = 100; tableSize <= 10000; tableSize *= 10) {
                for (int nodeSize = 16; nodeSize <= 2048; nodeSize *= 2) {
                    testUpdates(desc.reset(seed, tableSize, nodeSize), true, true);
                }
            }
        }
    }

    public void testShifts() {
        final SsaTestHelpers.TestDescriptor desc = new SsaTestHelpers.TestDescriptor();
        final int nSeeds;
        final int[] tableSizes;
        if (SHORT_TESTS) {
            nSeeds = 5;
            tableSizes = new int[]{ 100, 1_000 };
        } else {
            nSeeds = 20;
            tableSizes = new int[]{ 10, 100, 1_000, 10_000 };
        }
        for (int seed = 0; seed < nSeeds; ++seed) {
            for (final int tableSize : tableSizes) {
                for (int nodeSize = 16; nodeSize <= 2048; nodeSize *= 2) {
                    testShifts(desc.reset(seed, tableSize, nodeSize));
                }
            }
        }
    }

    private void testShifts(@NotNull final SsaTestHelpers.TestDescriptor desc) {
        final Random random = new Random(desc.seed());
        final TstUtils.ColumnInfo[] columnInfo;
        final QueryTable table = getTable(desc.tableSize(), random, columnInfo = initColumnInfos(new String[]{"Value"},
                SsaTestHelpers.getGeneratorForChar()));

        final Table asCharacter = SsaTestHelpers.prepareTestTableForChar(table);

        final CharSegmentedSortedArray ssa = new CharSegmentedSortedArray(desc.nodeSize());

        final ColumnSource<Character> valueSource = asCharacter.getColumnSource("Value");

        checkSsaInitial(asCharacter, ssa, valueSource, desc);

        try (final SafeCloseable ignored = LivenessScopeStack.open(new LivenessScope(true), true)) {
            final TableUpdateListener asCharacterListener = new InstrumentedTableUpdateListenerAdapter(asCharacter, false) {
                @Override
                public void onUpdate(TableUpdate upstream) {
                    try (final ColumnSource.GetContext checkContext = valueSource.makeGetContext(asCharacter.getRowSet().intSizePrev());
                         final RowSet relevantIndices = asCharacter.getRowSet().copyPrev()) {
                        checkSsa(ssa, valueSource.getPrevChunk(checkContext, relevantIndices).asCharChunk(), relevantIndices.asRowKeyChunk(), desc);
                    }

                    final int size = Math.max(upstream.modified().intSize() + Math.max(upstream.added().intSize(), upstream.removed().intSize()), (int) upstream.shifted().getEffectiveSize());
                    try (final ColumnSource.GetContext getContext = valueSource.makeGetContext(size);
                         final RowSet takeout = upstream.removed().union(upstream.getModifiedPreShift())) {
                        ssa.validate();

                        if (takeout.isNonempty()) {
                            final CharChunk<? extends Values> valuesToRemove = valueSource.getPrevChunk(getContext, takeout).asCharChunk();
                            ssa.remove(valuesToRemove, takeout.asRowKeyChunk());
                        }

                        ssa.validate();

                        try (final RowSet prevRowSet = asCharacter.getRowSet().copyPrev();
                             final ColumnSource.GetContext checkContext = valueSource.makeGetContext(prevRowSet.intSize());
                             final RowSet relevantIndices = prevRowSet.minus(takeout)) {
                            checkSsa(ssa, valueSource.getPrevChunk(checkContext, relevantIndices).asCharChunk(), relevantIndices.asRowKeyChunk(), desc);
                        }

                        if (upstream.shifted().nonempty()) {
                            final RowSetShiftData.Iterator sit = upstream.shifted().applyIterator();
                            while (sit.hasNext()) {
                                sit.next();
                                try (final RowSet prevRowSet = table.getRowSet().copyPrev();
                                     final RowSet subRowSet = prevRowSet.subSetByKeyRange(sit.beginRange(), sit.endRange());
                                     final RowSet withoutMods = subRowSet.minus(upstream.getModifiedPreShift());
                                     final RowSet rowSetToShift = withoutMods.minus(upstream.removed())) {
                                    if (rowSetToShift.isEmpty()) {
                                        continue;
                                    }

                                    final CharChunk<? extends Values> shiftValues = valueSource.getPrevChunk(getContext, rowSetToShift).asCharChunk();

                                    if (sit.polarityReversed()) {
                                        ssa.applyShiftReverse(shiftValues, rowSetToShift.asRowKeyChunk(), sit.shiftDelta());
                                    } else {
                                        ssa.applyShift(shiftValues, rowSetToShift.asRowKeyChunk(), sit.shiftDelta());
                                    }
                                }
                            }
                        }

                        ssa.validate();

                        try (final RowSet putin = upstream.added().union(upstream.modified())) {

                            try (final ColumnSource.GetContext checkContext = valueSource.makeGetContext(asCharacter.intSize());
                                 final RowSet relevantIndices = asCharacter.getRowSet().minus(putin)) {
                                checkSsa(ssa, valueSource.getChunk(checkContext, relevantIndices).asCharChunk(), relevantIndices.asRowKeyChunk(), desc);
                            }

                            if (putin.isNonempty()) {
                                final CharChunk<? extends Values> valuesToInsert = valueSource.getChunk(getContext, putin).asCharChunk();
                                ssa.insert(valuesToInsert, putin.asRowKeyChunk());
                            }
                        }

                        ssa.validate();
                    }
                }
            };
            asCharacter.listenForUpdates(asCharacterListener);

            while (desc.advance(50)) {
                UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() ->
                        GenerateTableUpdates.generateShiftAwareTableUpdates(GenerateTableUpdates.DEFAULT_PROFILE, desc.tableSize(), random, table, columnInfo));

                try (final ColumnSource.GetContext getContext = valueSource.makeGetContext(asCharacter.intSize());
                        final RowSet asCharRowSetCopy = asCharacter.getRowSet().copy()) {
                    checkSsa(ssa, valueSource.getChunk(getContext, asCharRowSetCopy).asCharChunk(), asCharRowSetCopy.asRowKeyChunk(), desc);
                }
            }
        }
    }

    private void testUpdates(@NotNull final SsaTestHelpers.TestDescriptor desc, boolean allowAddition, boolean allowRemoval) {
        final Random random = new Random(desc.seed());
        final TstUtils.ColumnInfo[] columnInfo;
        final QueryTable table = getTable(desc.tableSize(), random, columnInfo = initColumnInfos(new String[]{"Value"},
                SsaTestHelpers.getGeneratorForChar()));

        final Table asCharacter = SsaTestHelpers.prepareTestTableForChar(table);

        final CharSegmentedSortedArray ssa = new CharSegmentedSortedArray(desc.nodeSize());

        final ColumnSource<Character> valueSource = asCharacter.getColumnSource("Value");

        checkSsaInitial(asCharacter, ssa, valueSource, desc);

        try (final SafeCloseable ignored = LivenessScopeStack.open(new LivenessScope(true), true)) {
            final ShiftObliviousListener asCharacterListener = new ShiftObliviousInstrumentedListenerAdapter(asCharacter, false) {
                @Override
                public void onUpdate(RowSet added, RowSet removed, RowSet modified) {
                    try (final ColumnSource.GetContext getContext = valueSource.makeGetContext(Math.max(added.intSize(), removed.intSize()))) {
                        if (removed.isNonempty()) {
                            final CharChunk<? extends Values> valuesToRemove = valueSource.getPrevChunk(getContext, removed).asCharChunk();
                            ssa.remove(valuesToRemove, removed.asRowKeyChunk());
                        }
                        if (added.isNonempty()) {
                            ssa.insert(valueSource.getChunk(getContext, added).asCharChunk(), added.asRowKeyChunk());
                        }
                    }
                }
            };
            asCharacter.listenForUpdates(asCharacterListener);

            while (desc.advance(50)) {
                UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
                    final RowSet[] notify = GenerateTableUpdates.computeTableUpdates(desc.tableSize(), random, table, columnInfo, allowAddition, allowRemoval, false);
                    assertTrue(notify[2].isEmpty());
                    table.notifyListeners(notify[0], notify[1], notify[2]);
                });

                try (final ColumnSource.GetContext getContext = valueSource.makeGetContext(asCharacter.intSize());
                     final RowSet asCharRowSetCopy = asCharacter.getRowSet().copy()) {
                    checkSsa(ssa, valueSource.getChunk(getContext, asCharRowSetCopy).asCharChunk(), asCharRowSetCopy.asRowKeyChunk(), desc);
                }

                if (!allowAddition && table.size() == 0) {
                    break;
                }
            }
        }
    }

    private void checkSsaInitial(Table asCharacter, CharSegmentedSortedArray ssa, ColumnSource<?> valueSource, @NotNull final SsaTestHelpers.TestDescriptor desc) {
        try (final ColumnSource.GetContext getContext = valueSource.makeGetContext(asCharacter.intSize());
             final RowSet asCharRowSetCopy = asCharacter.getRowSet().copy()) {
            final CharChunk<? extends Values> valueChunk = valueSource.getChunk(getContext, asCharRowSetCopy).asCharChunk();
            final LongChunk<OrderedRowKeys> tableIndexChunk = asCharRowSetCopy.asRowKeyChunk();

            ssa.insert(valueChunk, tableIndexChunk);

            checkSsa(ssa, valueChunk, tableIndexChunk, desc);
        }
    }

    private void checkSsa(CharSegmentedSortedArray ssa, CharChunk<? extends Values> valueChunk, LongChunk<? extends RowKeys> tableIndexChunk, @NotNull final SsaTestHelpers.TestDescriptor desc) {
        try {
            ssa.validate();
            CharSsaChecker.checkSsa(ssa, valueChunk, tableIndexChunk);
        } catch (AssertionFailure | SsaChecker.SsaCheckException e) {
            TestCase.fail("Check failed at " + desc + ": " + e.getMessage());
        }
    }
}

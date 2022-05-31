/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit TestCharSegmentedSortedArray and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
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
import io.deephaven.chunk.FloatChunk;
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
public class TestFloatSegmentedSortedArray extends RefreshingTableTestCase {

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
                SsaTestHelpers.getGeneratorForFloat()));

        final Table asFloat = SsaTestHelpers.prepareTestTableForFloat(table);

        final FloatSegmentedSortedArray ssa = new FloatSegmentedSortedArray(desc.nodeSize());

        final ColumnSource<Float> valueSource = asFloat.getColumnSource("Value");

        checkSsaInitial(asFloat, ssa, valueSource, desc);

        try (final SafeCloseable ignored = LivenessScopeStack.open(new LivenessScope(true), true)) {
            final TableUpdateListener asFloatListener = new InstrumentedTableUpdateListenerAdapter(asFloat, false) {
                @Override
                public void onUpdate(TableUpdate upstream) {
                    try (final ColumnSource.GetContext checkContext = valueSource.makeGetContext(asFloat.getRowSet().intSizePrev());
                         final RowSet relevantIndices = asFloat.getRowSet().copyPrev()) {
                        checkSsa(ssa, valueSource.getPrevChunk(checkContext, relevantIndices).asFloatChunk(), relevantIndices.asRowKeyChunk(), desc);
                    }

                    final int size = Math.max(upstream.modified().intSize() + Math.max(upstream.added().intSize(), upstream.removed().intSize()), (int) upstream.shifted().getEffectiveSize());
                    try (final ColumnSource.GetContext getContext = valueSource.makeGetContext(size);
                         final RowSet takeout = upstream.removed().union(upstream.getModifiedPreShift())) {
                        ssa.validate();

                        if (takeout.isNonempty()) {
                            final FloatChunk<? extends Values> valuesToRemove = valueSource.getPrevChunk(getContext, takeout).asFloatChunk();
                            ssa.remove(valuesToRemove, takeout.asRowKeyChunk());
                        }

                        ssa.validate();

                        try (final RowSet prevRowSet = asFloat.getRowSet().copyPrev();
                             final ColumnSource.GetContext checkContext = valueSource.makeGetContext(prevRowSet.intSize());
                             final RowSet relevantIndices = prevRowSet.minus(takeout)) {
                            checkSsa(ssa, valueSource.getPrevChunk(checkContext, relevantIndices).asFloatChunk(), relevantIndices.asRowKeyChunk(), desc);
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

                                    final FloatChunk<? extends Values> shiftValues = valueSource.getPrevChunk(getContext, rowSetToShift).asFloatChunk();

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

                            try (final ColumnSource.GetContext checkContext = valueSource.makeGetContext(asFloat.intSize());
                                 final RowSet relevantIndices = asFloat.getRowSet().minus(putin)) {
                                checkSsa(ssa, valueSource.getChunk(checkContext, relevantIndices).asFloatChunk(), relevantIndices.asRowKeyChunk(), desc);
                            }

                            if (putin.isNonempty()) {
                                final FloatChunk<? extends Values> valuesToInsert = valueSource.getChunk(getContext, putin).asFloatChunk();
                                ssa.insert(valuesToInsert, putin.asRowKeyChunk());
                            }
                        }

                        ssa.validate();
                    }
                }
            };
            asFloat.listenForUpdates(asFloatListener);

            while (desc.advance(50)) {
                UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() ->
                        GenerateTableUpdates.generateShiftAwareTableUpdates(GenerateTableUpdates.DEFAULT_PROFILE, desc.tableSize(), random, table, columnInfo));

                try (final ColumnSource.GetContext getContext = valueSource.makeGetContext(asFloat.intSize());
                        final RowSet asFloatRowSetCopy = asFloat.getRowSet().copy()) {
                    checkSsa(ssa, valueSource.getChunk(getContext, asFloatRowSetCopy).asFloatChunk(), asFloatRowSetCopy.asRowKeyChunk(), desc);
                }
            }
        }
    }

    private void testUpdates(@NotNull final SsaTestHelpers.TestDescriptor desc, boolean allowAddition, boolean allowRemoval) {
        final Random random = new Random(desc.seed());
        final TstUtils.ColumnInfo[] columnInfo;
        final QueryTable table = getTable(desc.tableSize(), random, columnInfo = initColumnInfos(new String[]{"Value"},
                SsaTestHelpers.getGeneratorForFloat()));

        final Table asFloat = SsaTestHelpers.prepareTestTableForFloat(table);

        final FloatSegmentedSortedArray ssa = new FloatSegmentedSortedArray(desc.nodeSize());

        final ColumnSource<Float> valueSource = asFloat.getColumnSource("Value");

        checkSsaInitial(asFloat, ssa, valueSource, desc);

        try (final SafeCloseable ignored = LivenessScopeStack.open(new LivenessScope(true), true)) {
            final ShiftObliviousListener asFloatListener = new ShiftObliviousInstrumentedListenerAdapter(asFloat, false) {
                @Override
                public void onUpdate(RowSet added, RowSet removed, RowSet modified) {
                    try (final ColumnSource.GetContext getContext = valueSource.makeGetContext(Math.max(added.intSize(), removed.intSize()))) {
                        if (removed.isNonempty()) {
                            final FloatChunk<? extends Values> valuesToRemove = valueSource.getPrevChunk(getContext, removed).asFloatChunk();
                            ssa.remove(valuesToRemove, removed.asRowKeyChunk());
                        }
                        if (added.isNonempty()) {
                            ssa.insert(valueSource.getChunk(getContext, added).asFloatChunk(), added.asRowKeyChunk());
                        }
                    }
                }
            };
            asFloat.listenForUpdates(asFloatListener);

            while (desc.advance(50)) {
                UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
                    final RowSet[] notify = GenerateTableUpdates.computeTableUpdates(desc.tableSize(), random, table, columnInfo, allowAddition, allowRemoval, false);
                    assertTrue(notify[2].isEmpty());
                    table.notifyListeners(notify[0], notify[1], notify[2]);
                });

                try (final ColumnSource.GetContext getContext = valueSource.makeGetContext(asFloat.intSize());
                     final RowSet asFloatRowSetCopy = asFloat.getRowSet().copy()) {
                    checkSsa(ssa, valueSource.getChunk(getContext, asFloatRowSetCopy).asFloatChunk(), asFloatRowSetCopy.asRowKeyChunk(), desc);
                }

                if (!allowAddition && table.size() == 0) {
                    break;
                }
            }
        }
    }

    private void checkSsaInitial(Table asFloat, FloatSegmentedSortedArray ssa, ColumnSource<?> valueSource, @NotNull final SsaTestHelpers.TestDescriptor desc) {
        try (final ColumnSource.GetContext getContext = valueSource.makeGetContext(asFloat.intSize());
             final RowSet asFloatRowSetCopy = asFloat.getRowSet().copy()) {
            final FloatChunk<? extends Values> valueChunk = valueSource.getChunk(getContext, asFloatRowSetCopy).asFloatChunk();
            final LongChunk<OrderedRowKeys> tableIndexChunk = asFloatRowSetCopy.asRowKeyChunk();

            ssa.insert(valueChunk, tableIndexChunk);

            checkSsa(ssa, valueChunk, tableIndexChunk, desc);
        }
    }

    private void checkSsa(FloatSegmentedSortedArray ssa, FloatChunk<? extends Values> valueChunk, LongChunk<? extends RowKeys> tableIndexChunk, @NotNull final SsaTestHelpers.TestDescriptor desc) {
        try {
            ssa.validate();
            FloatSsaChecker.checkSsa(ssa, valueChunk, tableIndexChunk);
        } catch (AssertionFailure | SsaChecker.SsaCheckException e) {
            TestCase.fail("Check failed at " + desc + ": " + e.getMessage());
        }
    }
}

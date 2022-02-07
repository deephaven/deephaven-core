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
import io.deephaven.chunk.ObjectChunk;
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
public class TestObjectSegmentedSortedArray extends RefreshingTableTestCase {

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
                SsaTestHelpers.getGeneratorForObject()));

        final Table asObject = SsaTestHelpers.prepareTestTableForObject(table);

        final ObjectSegmentedSortedArray ssa = new ObjectSegmentedSortedArray(desc.nodeSize());

        final ColumnSource<Object> valueSource = asObject.getColumnSource("Value");

        checkSsaInitial(asObject, ssa, valueSource, desc);

        try (final SafeCloseable ignored = LivenessScopeStack.open(new LivenessScope(true), true)) {
            final TableUpdateListener asObjectListener = new InstrumentedTableUpdateListenerAdapter(asObject, false) {
                @Override
                public void onUpdate(TableUpdate upstream) {
                    try (final ColumnSource.GetContext checkContext = valueSource.makeGetContext(asObject.getRowSet().intSizePrev());
                         final RowSet relevantIndices = asObject.getRowSet().copyPrev()) {
                        checkSsa(ssa, valueSource.getPrevChunk(checkContext, relevantIndices).asObjectChunk(), relevantIndices.asRowKeyChunk(), desc);
                    }

                    final int size = Math.max(upstream.modified().intSize() + Math.max(upstream.added().intSize(), upstream.removed().intSize()), (int) upstream.shifted().getEffectiveSize());
                    try (final ColumnSource.GetContext getContext = valueSource.makeGetContext(size);
                         final RowSet takeout = upstream.removed().union(upstream.getModifiedPreShift())) {
                        ssa.validate();

                        if (takeout.isNonempty()) {
                            final ObjectChunk<Object, ? extends Values> valuesToRemove = valueSource.getPrevChunk(getContext, takeout).asObjectChunk();
                            ssa.remove(valuesToRemove, takeout.asRowKeyChunk());
                        }

                        ssa.validate();

                        try (final RowSet prevRowSet = asObject.getRowSet().copyPrev();
                             final ColumnSource.GetContext checkContext = valueSource.makeGetContext(prevRowSet.intSize());
                             final RowSet relevantIndices = prevRowSet.minus(takeout)) {
                            checkSsa(ssa, valueSource.getPrevChunk(checkContext, relevantIndices).asObjectChunk(), relevantIndices.asRowKeyChunk(), desc);
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

                                    final ObjectChunk<Object, ? extends Values> shiftValues = valueSource.getPrevChunk(getContext, rowSetToShift).asObjectChunk();

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

                            try (final ColumnSource.GetContext checkContext = valueSource.makeGetContext(asObject.intSize());
                                 final RowSet relevantIndices = asObject.getRowSet().minus(putin)) {
                                checkSsa(ssa, valueSource.getChunk(checkContext, relevantIndices).asObjectChunk(), relevantIndices.asRowKeyChunk(), desc);
                            }

                            if (putin.isNonempty()) {
                                final ObjectChunk<Object, ? extends Values> valuesToInsert = valueSource.getChunk(getContext, putin).asObjectChunk();
                                ssa.insert(valuesToInsert, putin.asRowKeyChunk());
                            }
                        }

                        ssa.validate();
                    }
                }
            };
            asObject.listenForUpdates(asObjectListener);

            while (desc.advance(50)) {
                UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() ->
                        GenerateTableUpdates.generateShiftAwareTableUpdates(GenerateTableUpdates.DEFAULT_PROFILE, desc.tableSize(), random, table, columnInfo));

                try (final ColumnSource.GetContext getContext = valueSource.makeGetContext(asObject.intSize());
                        final RowSet asObjectRowSetCopy = asObject.getRowSet().copy()) {
                    checkSsa(ssa, valueSource.getChunk(getContext, asObjectRowSetCopy).asObjectChunk(), asObjectRowSetCopy.asRowKeyChunk(), desc);
                }
            }
        }
    }

    private void testUpdates(@NotNull final SsaTestHelpers.TestDescriptor desc, boolean allowAddition, boolean allowRemoval) {
        final Random random = new Random(desc.seed());
        final TstUtils.ColumnInfo[] columnInfo;
        final QueryTable table = getTable(desc.tableSize(), random, columnInfo = initColumnInfos(new String[]{"Value"},
                SsaTestHelpers.getGeneratorForObject()));

        final Table asObject = SsaTestHelpers.prepareTestTableForObject(table);

        final ObjectSegmentedSortedArray ssa = new ObjectSegmentedSortedArray(desc.nodeSize());

        final ColumnSource<Object> valueSource = asObject.getColumnSource("Value");

        checkSsaInitial(asObject, ssa, valueSource, desc);

        try (final SafeCloseable ignored = LivenessScopeStack.open(new LivenessScope(true), true)) {
            final ShiftObliviousListener asObjectListener = new ShiftObliviousInstrumentedListenerAdapter(asObject, false) {
                @Override
                public void onUpdate(RowSet added, RowSet removed, RowSet modified) {
                    try (final ColumnSource.GetContext getContext = valueSource.makeGetContext(Math.max(added.intSize(), removed.intSize()))) {
                        if (removed.isNonempty()) {
                            final ObjectChunk<Object, ? extends Values> valuesToRemove = valueSource.getPrevChunk(getContext, removed).asObjectChunk();
                            ssa.remove(valuesToRemove, removed.asRowKeyChunk());
                        }
                        if (added.isNonempty()) {
                            ssa.insert(valueSource.getChunk(getContext, added).asObjectChunk(), added.asRowKeyChunk());
                        }
                    }
                }
            };
            asObject.listenForUpdates(asObjectListener);

            while (desc.advance(50)) {
                UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
                    final RowSet[] notify = GenerateTableUpdates.computeTableUpdates(desc.tableSize(), random, table, columnInfo, allowAddition, allowRemoval, false);
                    assertTrue(notify[2].isEmpty());
                    table.notifyListeners(notify[0], notify[1], notify[2]);
                });

                try (final ColumnSource.GetContext getContext = valueSource.makeGetContext(asObject.intSize());
                     final RowSet asObjectRowSetCopy = asObject.getRowSet().copy()) {
                    checkSsa(ssa, valueSource.getChunk(getContext, asObjectRowSetCopy).asObjectChunk(), asObjectRowSetCopy.asRowKeyChunk(), desc);
                }

                if (!allowAddition && table.size() == 0) {
                    break;
                }
            }
        }
    }

    private void checkSsaInitial(Table asObject, ObjectSegmentedSortedArray ssa, ColumnSource<?> valueSource, @NotNull final SsaTestHelpers.TestDescriptor desc) {
        try (final ColumnSource.GetContext getContext = valueSource.makeGetContext(asObject.intSize());
             final RowSet asObjectRowSetCopy = asObject.getRowSet().copy()) {
            final ObjectChunk<Object, ? extends Values> valueChunk = valueSource.getChunk(getContext, asObjectRowSetCopy).asObjectChunk();
            final LongChunk<OrderedRowKeys> tableIndexChunk = asObjectRowSetCopy.asRowKeyChunk();

            ssa.insert(valueChunk, tableIndexChunk);

            checkSsa(ssa, valueChunk, tableIndexChunk, desc);
        }
    }

    private void checkSsa(ObjectSegmentedSortedArray ssa, ObjectChunk<Object, ? extends Values> valueChunk, LongChunk<? extends RowKeys> tableIndexChunk, @NotNull final SsaTestHelpers.TestDescriptor desc) {
        try {
            ssa.validate();
            ObjectSsaChecker.checkSsa(ssa, valueChunk, tableIndexChunk);
        } catch (AssertionFailure | SsaChecker.SsaCheckException e) {
            TestCase.fail("Check failed at " + desc + ": " + e.getMessage());
        }
    }
}

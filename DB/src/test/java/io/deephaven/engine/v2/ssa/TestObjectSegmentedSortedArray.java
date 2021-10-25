/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit TestCharSegmentedSortedArray and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.engine.v2.ssa;

import io.deephaven.base.verify.AssertionFailure;
import io.deephaven.engine.tables.Table;
import io.deephaven.engine.tables.live.LiveTableMonitor;
import io.deephaven.engine.util.liveness.LivenessScope;
import io.deephaven.engine.util.liveness.LivenessScopeStack;
import io.deephaven.engine.v2.*;
import io.deephaven.engine.v2.sources.ColumnSource;
import io.deephaven.engine.v2.sources.chunk.Attributes;
import io.deephaven.engine.v2.sources.chunk.Attributes.Values;
import io.deephaven.engine.v2.sources.chunk.ObjectChunk;
import io.deephaven.engine.v2.sources.chunk.LongChunk;
import io.deephaven.engine.v2.utils.TrackingMutableRowSet;
import io.deephaven.engine.v2.utils.IndexShiftData;
import io.deephaven.test.types.ParallelTest;
import io.deephaven.util.SafeCloseable;
import junit.framework.TestCase;
import org.jetbrains.annotations.NotNull;
import org.junit.experimental.categories.Category;

import java.util.Random;

import static io.deephaven.engine.v2.TstUtils.*;

@Category(ParallelTest.class)
public class TestObjectSegmentedSortedArray extends LiveTableTestCase {

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
        for (int seed = 0; seed < 20; ++seed) {
            for (int tableSize = 10; tableSize <= 10000; tableSize *= 10) {
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

        //noinspection unchecked
        final ColumnSource<Object> valueSource = asObject.getColumnSource("Value");

        checkSsaInitial(asObject, ssa, valueSource, desc);

        try (final SafeCloseable ignored = LivenessScopeStack.open(new LivenessScope(true), true)) {
            final ShiftAwareListener asObjectListener = new InstrumentedShiftAwareListenerAdapter((DynamicTable) asObject, false) {
                @Override
                public void onUpdate(Update upstream) {
                    try (final ColumnSource.GetContext checkContext = valueSource.makeGetContext(asObject.getIndex().getPrevRowSet().intSize())) {
                        final TrackingMutableRowSet relevantIndices = asObject.getIndex().getPrevRowSet();
                        checkSsa(ssa, valueSource.getPrevChunk(checkContext, relevantIndices).asObjectChunk(), relevantIndices.asRowKeyChunk(), desc);
                    }

                    final int size = Math.max(upstream.modified.intSize() + Math.max(upstream.added.intSize(), upstream.removed.intSize()), (int) upstream.shifted.getEffectiveSize());
                    try (final ColumnSource.GetContext getContext = valueSource.makeGetContext(size)) {
                        ssa.validate();

                        final TrackingMutableRowSet takeout = upstream.removed.union(upstream.getModifiedPreShift());
                        if (takeout.nonempty()) {
                            final ObjectChunk<Object, ? extends Values> valuesToRemove = valueSource.getPrevChunk(getContext, takeout).asObjectChunk();
                            ssa.remove(valuesToRemove, takeout.asRowKeyChunk());
                        }

                        ssa.validate();

                        try (final ColumnSource.GetContext checkContext = valueSource.makeGetContext(asObject.getIndex().getPrevRowSet().intSize())) {
                            final TrackingMutableRowSet relevantIndices = asObject.getIndex().getPrevRowSet().minus(takeout);
                            checkSsa(ssa, valueSource.getPrevChunk(checkContext, relevantIndices).asObjectChunk(), relevantIndices.asRowKeyChunk(), desc);
                        }

                        if (upstream.shifted.nonempty()) {
                            final IndexShiftData.Iterator sit = upstream.shifted.applyIterator();
                            while (sit.hasNext()) {
                                sit.next();
                                final TrackingMutableRowSet rowSetToShift = table.getIndex().getPrevRowSet().subSetByKeyRange(sit.beginRange(), sit.endRange()).minus(upstream.getModifiedPreShift()).minus(upstream.removed);
                                if (rowSetToShift.empty()) {
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

                        ssa.validate();

                        final TrackingMutableRowSet putin = upstream.added.union(upstream.modified);

                        try (final ColumnSource.GetContext checkContext = valueSource.makeGetContext(asObject.intSize())) {
                            final TrackingMutableRowSet relevantIndices = asObject.getIndex().minus(putin);
                            checkSsa(ssa, valueSource.getChunk(checkContext, relevantIndices).asObjectChunk(), relevantIndices.asRowKeyChunk(), desc);
                        }

                        if (putin.nonempty()) {
                            final ObjectChunk<Object, ? extends Values> valuesToInsert = valueSource.getChunk(getContext, putin).asObjectChunk();
                            ssa.insert(valuesToInsert, putin.asRowKeyChunk());
                        }

                        ssa.validate();
                    }
                }
            };
            ((DynamicTable) asObject).listenForUpdates(asObjectListener);

            while (desc.advance(50)) {
                System.out.println();
                LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() ->
                        GenerateTableUpdates.generateShiftAwareTableUpdates(GenerateTableUpdates.DEFAULT_PROFILE, desc.tableSize(), random, table, columnInfo));

                try (final ColumnSource.GetContext getContext = valueSource.makeGetContext(asObject.intSize())) {
                    checkSsa(ssa, valueSource.getChunk(getContext, asObject.getIndex()).asObjectChunk(), asObject.getIndex().asRowKeyChunk(), desc);
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

        //noinspection unchecked
        final ColumnSource<Object> valueSource = asObject.getColumnSource("Value");

        checkSsaInitial(asObject, ssa, valueSource, desc);

        try (final SafeCloseable ignored = LivenessScopeStack.open(new LivenessScope(true), true)) {
            final Listener asObjectListener = new InstrumentedListenerAdapter((DynamicTable) asObject, false) {
                @Override
                public void onUpdate(TrackingMutableRowSet added, TrackingMutableRowSet removed, TrackingMutableRowSet modified) {
                    try (final ColumnSource.GetContext getContext = valueSource.makeGetContext(Math.max(added.intSize(), removed.intSize()))) {
                        if (removed.nonempty()) {
                            final ObjectChunk<Object, ? extends Values> valuesToRemove = valueSource.getPrevChunk(getContext, removed).asObjectChunk();
                            ssa.remove(valuesToRemove, removed.asRowKeyChunk());
                        }
                        if (added.nonempty()) {
                            ssa.insert(valueSource.getChunk(getContext, added).asObjectChunk(), added.asRowKeyChunk());
                        }
                    }
                }
            };
            ((DynamicTable) asObject).listenForUpdates(asObjectListener);

            while (desc.advance(50)) {
                LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
                    final TrackingMutableRowSet[] notify = GenerateTableUpdates.computeTableUpdates(desc.tableSize(), random, table, columnInfo, allowAddition, allowRemoval, false);
                    assertTrue(notify[2].empty());
                    table.notifyListeners(notify[0], notify[1], notify[2]);
                });

                try (final ColumnSource.GetContext getContext = valueSource.makeGetContext(asObject.intSize())) {
                    checkSsa(ssa, valueSource.getChunk(getContext, asObject.getIndex()).asObjectChunk(), asObject.getIndex().asRowKeyChunk(), desc);
                }

                if (!allowAddition && table.size() == 0) {
                    break;
                }
            }
        }
    }

    private void checkSsaInitial(Table asObject, ObjectSegmentedSortedArray ssa, ColumnSource<?> valueSource, @NotNull final SsaTestHelpers.TestDescriptor desc) {
        try (final ColumnSource.GetContext getContext = valueSource.makeGetContext(asObject.intSize())) {
            final ObjectChunk<Object, ? extends Values> valueChunk = valueSource.getChunk(getContext, asObject.getIndex()).asObjectChunk();
            final LongChunk<Attributes.OrderedRowKeys> tableIndexChunk = asObject.getIndex().asRowKeyChunk();

            ssa.insert(valueChunk, tableIndexChunk);

            checkSsa(ssa, valueChunk, tableIndexChunk, desc);
        }
    }

    private void checkSsa(ObjectSegmentedSortedArray ssa, ObjectChunk<Object, ? extends Values> valueChunk, LongChunk<? extends Attributes.RowKeys> tableIndexChunk, @NotNull final SsaTestHelpers.TestDescriptor desc) {
        try {
            ssa.validate();
            ObjectSsaChecker.checkSsa(ssa, valueChunk, tableIndexChunk);
        } catch (AssertionFailure | SsaChecker.SsaCheckException e) {
            TestCase.fail("Check failed at " + desc + ": " + e.getMessage());
        }
    }
}

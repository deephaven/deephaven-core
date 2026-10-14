//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.barrage;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.impl.TableUpdateImpl;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.GenerateTableUpdates;
import io.deephaven.test.types.OutOfBandTest;
import io.deephaven.util.mutable.MutableInt;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.function.Consumer;

import static io.deephaven.engine.testutil.TstUtils.i;

/**
 * Runs {@link BarrageMessageIncrementalRoundTripTestBase} with the producer writing to subscribers in turn, and adds
 * the incremental tests that give each producer a single subscriber. A producer writes a single subscriber the same way
 * whatever its propagation threads, so {@link BarrageMessageIncrementalRoundTripParallelTest} does not repeat them.
 */
@Category(OutOfBandTest.class)
public class BarrageMessageIncrementalRoundTripTest extends BarrageMessageIncrementalRoundTripTestBase {

    @Test
    public void testAppendIncremental() {
        final int MAX_STEPS = 100;
        final Consumer<TestHelper> runOne = helper -> {
            final int maxSteps = MAX_STEPS * helper.numConsumerCoalesce * helper.numProducerCoalesce;
            helper.runTest(() -> {
                final long lastKey = (Math.abs(helper.random.nextLong()) % 16)
                        + (helper.sourceTable.getRowSet().isNonempty() ? helper.sourceTable.getRowSet().lastRowKey()
                                : -1);
                final TableUpdateImpl update = new TableUpdateImpl();
                update.added = RowSetFactory.fromRange(lastKey + 1,
                        lastKey + Math.max(1, helper.size / maxSteps));
                update.removed = i();
                update.modified = i();
                update.shifted = RowSetShiftData.EMPTY;
                update.modifiedColumnSet = ModifiedColumnSet.EMPTY;

                final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
                updateGraph.runWithinUnitTestCycle(() -> GenerateTableUpdates.generateTableUpdates(update,
                        helper.random, helper.sourceTable, helper.columnInfo));
            });
        };

        for (final int size : new int[] {10, 100, 1000}) {
            for (final int numProducerCoalesce : new int[] {1, 10}) {
                for (final int numConsumerCoalesce : new int[] {1, 10}) {
                    runOne.accept(new OneProducerPerClient(numProducerCoalesce, numConsumerCoalesce, size, 0,
                            new MutableInt(MAX_STEPS)));
                }
            }
        }
    }

    @Test
    public void testPrependIncremental() {
        final int MAX_STEPS = 100;
        final Consumer<TestHelper> runOne = helper -> {
            final int maxSteps = MAX_STEPS * helper.numConsumerCoalesce * helper.numProducerCoalesce;
            helper.runTest(() -> {
                final long lastKey =
                        helper.sourceTable.getRowSet().isNonempty() ? helper.sourceTable.getRowSet().lastRowKey() : -1;
                final TableUpdateImpl update = new TableUpdateImpl();
                final int stepSize = Math.max(1, helper.size / maxSteps);
                update.added = RowSetFactory.fromRange(0, stepSize - 1);
                update.removed = i();
                update.modified = i();
                update.modifiedColumnSet = ModifiedColumnSet.EMPTY;

                final RowSetShiftData.Builder shifted = new RowSetShiftData.Builder();
                if (lastKey >= 0) {
                    shifted.shiftRange(0, lastKey, stepSize + (Math.abs(helper.random.nextLong()) % 16));
                }
                update.shifted = shifted.build();

                final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
                updateGraph.runWithinUnitTestCycle(() -> GenerateTableUpdates.generateTableUpdates(update,
                        helper.random, helper.sourceTable, helper.columnInfo));
            });
        };

        for (final int size : new int[] {10, 100, 1000}) {
            for (final int numProducerCoalesce : new int[] {1, 10}) {
                for (final int numConsumerCoalesce : new int[] {1, 10}) {
                    runOne.accept(new OneProducerPerClient(numProducerCoalesce, numConsumerCoalesce, size, 0,
                            new MutableInt(MAX_STEPS)));
                }
            }
        }
    }

    @Test
    public void testRoundTripIncremental() {
        final Consumer<TestHelper> runOne = helper -> {
            helper.runTest(() -> {
                final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
                updateGraph.runWithinUnitTestCycle(() -> GenerateTableUpdates.generateShiftAwareTableUpdates(
                        GenerateTableUpdates.DEFAULT_PROFILE,
                        helper.size, helper.random, helper.sourceTable, helper.columnInfo));
            });
        };

        for (final int size : new int[] {10, 100, 1000}) {
            for (final int numProducerCoalesce : new int[] {1, 10}) {
                for (final int numConsumerCoalesce : new int[] {1, 10}) {
                    runOne.accept(new OneProducerPerClient(numProducerCoalesce, numConsumerCoalesce, size, 0,
                            new MutableInt(100)));
                }
            }
        }
    }
}

//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.barrage;

import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.Table;
import io.deephaven.test.types.OutOfBandTest;
import io.deephaven.util.mutable.MutableInt;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.BitSet;
import java.util.function.Supplier;

/**
 * Runs {@link BarrageMessageSubscriptionRoundTripTestBase} with the producer writing to subscribers in turn, and adds
 * the viewport change tests that give each producer a single subscriber. A producer writes a single subscriber the same
 * way whatever its propagation threads, so {@link BarrageMessageSubscriptionRoundTripParallelTest} does not repeat
 * them.
 */
@Category(OutOfBandTest.class)
public class BarrageMessageSubscriptionRoundTripTest extends BarrageMessageSubscriptionRoundTripTestBase {

    @Test
    public void testViewportChange() {
        for (final int size : new int[] {10, 100}) {
            for (final int numProducerCoalesce : new int[] {1, 4}) {
                for (final int numConsumerCoalesce : new int[] {1, 4}) {
                    new SubscriptionChangingHelper(numProducerCoalesce, numConsumerCoalesce, size, 0,
                            new MutableInt(25)) {
                        @Override
                        void createNuggetsForTableMaker(final Supplier<Table> makeTable) {
                            final RemoteNugget nugget = new RemoteNugget(makeTable);
                            nuggets.add(nugget);

                            final BitSet columns = new BitSet();
                            columns.set(0, 4);
                            nugget.clients.add(
                                    new RemoteClient(RowSetFactory.fromRange(0, size / 5),
                                            columns, nugget.barrageMessageProducer, nugget.originalTable,
                                            "sub-changer"));
                        }

                        void maybeChangeSub(final int step, final int rt, final int pt) {
                            // every other step, just before its last coalesced update, which every coalescing reaches
                            if (step % 2 != 0 || rt != numConsumerCoalesce - 1 || pt != numProducerCoalesce - 1) {
                                return;
                            }

                            for (final RemoteNugget nugget : nuggets) {
                                final RemoteClient client = nugget.clients.get(nugget.clients.size() - 1);
                                final WritableRowSet viewport = client.viewport.copy();
                                viewport.shiftInPlace(Math.max(size / 25, 1));

                                // maintain viewport direction in this test
                                client.setViewport(viewport, client.reverseViewport);
                            }
                        }
                    }.runTest();
                }
            }
        }
    }

    @Test
    public void testViewportDirectionChange() {
        for (final int size : new int[] {10, 100}) {
            for (final int numProducerCoalesce : new int[] {1, 4}) {
                for (final int numConsumerCoalesce : new int[] {1, 4}) {
                    new SubscriptionChangingHelper(numProducerCoalesce, numConsumerCoalesce, size, 0,
                            new MutableInt(25)) {
                        @Override
                        void createNuggetsForTableMaker(final Supplier<Table> makeTable) {
                            final RemoteNugget nugget = new RemoteNugget(makeTable);
                            nuggets.add(nugget);

                            final BitSet columns = new BitSet();
                            columns.set(0, 4);
                            nugget.clients.add(
                                    new RemoteClient(RowSetFactory.fromRange(0, size / 5),
                                            columns, nugget.barrageMessageProducer, nugget.originalTable,
                                            "sub-changer"));
                        }

                        void maybeChangeSub(final int step, final int rt, final int pt) {
                            // every other step, just before its last coalesced update, which every coalescing reaches
                            if (step % 2 != 0 || rt != numConsumerCoalesce - 1 || pt != numProducerCoalesce - 1) {
                                return;
                            }

                            for (final RemoteNugget nugget : nuggets) {
                                final RemoteClient client = nugget.clients.get(nugget.clients.size() - 1);
                                final WritableRowSet viewport = client.viewport.copy();
                                viewport.shiftInPlace(Math.max(size / 25, 1));

                                // alternate viewport direction with every call to this function
                                client.setViewport(viewport, !client.reverseViewport);
                            }
                        }
                    }.runTest();
                }
            }
        }
    }
}

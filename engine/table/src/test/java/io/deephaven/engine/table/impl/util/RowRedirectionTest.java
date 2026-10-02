//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.util;

import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.io.logger.Logger;
import io.deephaven.internal.log.LoggerFactory;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.LongStream;

import static org.junit.Assert.*;

public class RowRedirectionTest extends RefreshingTableTestCase {
    private final Logger log = LoggerFactory.getLogger(RowRedirectionTest.class);

    @Test
    public void testBasic() {
        final WritableRowRedirection rowRedirection = WritableRowRedirection.FACTORY.createRowRedirection(8);
        for (int i = 0; i < 3; i++) {
            rowRedirection.put(i, i * 2);
        }
        final WritableRowRedirection rowRedirection1 = WritableRowRedirection.FACTORY.createRowRedirection(8);
        for (int i = 0; i < 3; i++) {
            rowRedirection1.put(i * 2, i * 4);
        }
        for (int i = 0; i < 3; i++) {
            assertEquals(rowRedirection.get(i), i * 2);
            assertEquals(rowRedirection1.get(i * 2), i * 4);
        }
        rowRedirection.startTrackingPrevValues();
        rowRedirection1.startTrackingPrevValues();
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            for (int i1 = 0; i1 < 3; i1++) {
                rowRedirection1.put(i1 * 2, i1 * 3);
            }
            for (int i1 = 0; i1 < 3; i1++) {
                assertEquals(i1 * 2, rowRedirection.get(i1));
                assertEquals(i1 * 2, rowRedirection.getPrev(i1));

                assertEquals(i1 * 3, rowRedirection1.get(i1 * 2));
                assertEquals(rowRedirection1.getPrev(i1 * 2), i1 * 4);
            }
        });

        updateGraph.runWithinUnitTestCycle(() -> {
            for (int i = 0; i < 3; i++) {
                rowRedirection.put((i + 1) % 3, i * 2);
            }
        });
    }

    @Test
    public void testContiguous() {
        final WritableRowRedirection rowRedirection = new ContiguousWritableRowRedirection(10);

        // Fill row redirection with values 100 + ii * 2
        for (int ii = 0; ii < 100; ++ii) {
            rowRedirection.put(ii, 100 + ii * 2);
        }

        // Check that 100 + ii * 2 comes back from get()
        for (int ii = 0; ii < 100; ++ii) {
            assertEquals(100 + ii * 2, rowRedirection.get(ii));
        }

        assertEquals(-1, rowRedirection.get(100));
        assertEquals(-1, rowRedirection.get(-1));

        // As of startTrackingPrevValues, get() and getPrev() should both be returning 100 + ii * 2
        rowRedirection.startTrackingPrevValues();

        // Now set current values to 200 + ii * 3
        // Confirm that get() returns 200 + ii * 3; meanwhile getPrev() still returns 100 + ii * 2
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            for (int ii1 = 0; ii1 < 100; ++ii1) {
                assertEquals(100 + ii1 * 2, rowRedirection.get(ii1));
            }
            for (int ii1 = 0; ii1 < 100; ++ii1) {
                assertEquals(100 + ii1 * 2, rowRedirection.getPrev(ii1));
            }

            // Now set current values to 200 + ii * 3
            for (int ii1 = 0; ii1 < 100; ++ii1) {
                rowRedirection.put(ii1, 200 + ii1 * 3);
            }

            // Confirm that get() returns 200 + ii * 3; meanwhile getPrev() still returns 100 + ii * 2
            for (int ii1 = 0; ii1 < 100; ++ii1) {
                assertEquals(200 + ii1 * 3, rowRedirection.get(ii1));
            }
            for (int ii1 = 0; ii1 < 100; ++ii1) {
                assertEquals(100 + ii1 * 2, rowRedirection.getPrev(ii1));
            }
        });

        // After end of cycle, both should return 200 + ii * 3
        for (int ii = 0; ii < 100; ++ii) {
            assertEquals(200 + ii * 3, rowRedirection.get(ii));
        }
        for (int ii = 0; ii < 100; ++ii) {
            assertEquals(200 + ii * 3, rowRedirection.getPrev(ii));
        }
    }

    /**
     * The chunked fill methods of WritableRowRedirectionLockFree must agree with the scalar get()/getPrev() overlay for
     * every kind of key: served by 'baseline', updated this cycle (served by 'updates'), removed this cycle (tombstoned
     * in 'updates'), never present, and the NULL_ROW_KEY sentinel — both mid-cycle and after the terminal commit folds
     * 'updates' into 'baseline', and before prev tracking starts, when the two names are one map.
     */
    @Test
    public void testChunkedFillsMatchScalarGets() {
        final WritableRowRedirection redirection = WritableRowRedirection.FACTORY.createRowRedirection(8);
        assertTrue(redirection instanceof WritableRowRedirectionLockFree);
        for (int ii = 0; ii < 100; ++ii) {
            redirection.put(ii * 2, 1000 + ii);
        }
        final long[] probes = {-1, 0, 1, 2, 4, 6, 9, 48, 50, 52, 100, 150, 198, 200, 5000};
        // Before prev tracking: a static table's redirection, one map under both names.
        checkFillsMatchScalars(redirection, probes);
        redirection.startTrackingPrevValues();


        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            for (int ii = 0; ii < 25; ++ii) {
                redirection.put(ii * 4, 2000 + ii);
            }
            for (int ii = 0; ii < 10; ++ii) {
                redirection.remove(50 + ii * 2);
            }
            checkFillsMatchScalars(redirection, probes);
        });
        checkFillsMatchScalars(redirection, probes);
    }

    private static void checkFillsMatchScalars(final RowRedirection redirection, final long[] probes) {
        final int size = probes.length;
        try (final ChunkSource.FillContext fillContext = redirection.makeFillContext(size, null);
                final WritableLongChunk<RowKeys> keys = WritableLongChunk.makeWritableChunk(size);
                final WritableLongChunk<RowKeys> actual = WritableLongChunk.makeWritableChunk(size)) {
            for (int ii = 0; ii < size; ++ii) {
                keys.set(ii, probes[ii]);
            }

            redirection.fillChunkUnordered(fillContext, actual, keys);
            assertEquals(size, actual.size());
            for (int ii = 0; ii < size; ++ii) {
                assertEquals(redirection.get(probes[ii]), actual.get(ii));
            }

            redirection.fillPrevChunkUnordered(fillContext, actual, keys);
            assertEquals(size, actual.size());
            for (int ii = 0; ii < size; ++ii) {
                assertEquals(redirection.getPrev(probes[ii]), actual.get(ii));
            }

            // The ordered variants, over the non-negative probes (a RowSequence cannot hold NULL_ROW_KEY).
            final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
            for (final long probe : probes) {
                if (probe >= 0) {
                    builder.appendKey(probe);
                }
            }
            try (final RowSet orderedProbes = builder.build()) {
                redirection.fillChunk(fillContext, actual, orderedProbes);
                assertEquals(orderedProbes.intSize(), actual.size());
                int oi = 0;
                for (final RowSet.Iterator it = orderedProbes.iterator(); it.hasNext(); ++oi) {
                    assertEquals(redirection.get(it.nextLong()), actual.get(oi));
                }

                redirection.fillPrevChunk(fillContext, actual, orderedProbes);
                assertEquals(orderedProbes.intSize(), actual.size());
                oi = 0;
                for (final RowSet.Iterator it = orderedProbes.iterator(); it.hasNext(); ++oi) {
                    assertEquals(redirection.getPrev(it.nextLong()), actual.get(oi));
                }
            }
        }
    }

    /**
     * Two readers fill from two lock-free redirections through ONE shared RowSet, concurrently, while a third thread
     * mutates it, as two sorts of one live table do in one batch. Each reader owns its destination and its fill
     * context; only the RowSequence is shared, and the redirection must not borrow the chunk that sequence caches.
     */
    @Test
    public void testLockFreeFillChunkDoesNotBorrowTheRowSequencesChunk() throws InterruptedException {
        final int n = 1000;
        final int iterations = 3000;
        final WritableRowSet shared = RowSetFactory.fromKeys(LongStream.range(0, n).map(k -> 3 * k).toArray());
        final WritableRowRedirection r1 = WritableRowRedirection.FACTORY.createRowRedirection(2048);
        final WritableRowRedirection r2 = WritableRowRedirection.FACTORY.createRowRedirection(2048);
        shared.forAllRowKeys(k -> {
            r1.put(k, 1_000_000 + k);
            r2.put(k, 2_000_000 + k);
        });
        final AtomicReference<String> failure = new AtomicReference<>();
        final CountDownLatch start = new CountDownLatch(1);
        // The key the mutator toggles lies beyond the readers' keys and in neither redirection.
        final long toggled = 3L * n + 100;
        final Thread mutator = new Thread(() -> {
            try {
                for (int it = 0; failure.get() == null && !Thread.currentThread().isInterrupted(); ++it) {
                    if ((it & 1) == 0) {
                        shared.insert(toggled);
                    } else {
                        shared.remove(toggled);
                    }
                    // and read it the way another reader of the table would, keeping its cached chunk in play
                    shared.asRowKeyChunk();
                }
            } catch (final Throwable e) {
                failure.compareAndSet(null, "mutator threw " + e);
            }
        }, "mutator");
        final Thread[] readers = new Thread[2];
        for (int t = 0; t < 2; ++t) {
            final WritableRowRedirection r = t == 0 ? r1 : r2;
            final long base = t == 0 ? 1_000_000 : 2_000_000;
            final String name = "reader" + t;
            readers[t] = new Thread(() -> {
                try (final ChunkSource.FillContext fc = r.makeFillContext(n + 1, null);
                        final WritableLongChunk<RowKeys> dest = WritableLongChunk.makeWritableChunk(n + 1)) {
                    start.await();
                    for (int it = 0; it < iterations && failure.get() == null; ++it) {
                        r.fillChunk(fc, dest, shared);
                        if (dest.size() != n && dest.size() != n + 1) {
                            failure.compareAndSet(null, name + " iteration " + it + ": size " + dest.size());
                            return;
                        }
                        for (int ii = 0; ii < n; ++ii) {
                            if (dest.get(ii) != base + 3L * ii) {
                                failure.compareAndSet(null, name + " iteration " + it + ": position " + ii + " got "
                                        + dest.get(ii) + " expected " + (base + 3L * ii));
                                return;
                            }
                        }
                    }
                } catch (final Throwable e) {
                    failure.compareAndSet(null, name + " threw " + e);
                }
            }, name);
            readers[t].start();
        }
        mutator.start();
        start.countDown();
        for (final Thread reader : readers) {
            reader.join();
        }
        mutator.interrupt();
        mutator.join();
        assertNull(failure.get(), failure.get());
    }
}

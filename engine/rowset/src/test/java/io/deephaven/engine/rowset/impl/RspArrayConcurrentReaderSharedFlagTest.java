//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.impl.rsp.RspArray;
import io.deephaven.test.types.OutOfBandTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * {@link RefCountedCow} lets readers run concurrently with the single writer of an unshared set, discarding results the
 * clock invalidates. The derivation paths that share containers (deepCopy, the RspArray sub-range constructors, the
 * shared-container appends) have to record that a packed ArrayContainer is now shared where the <em>source</em> will
 * see it, and the source is the set being read, under mutation by its writer. The only thing a reader may write is the
 * flag slot of the {@code short[]} it shares: never a word of the source, which may by then describe a different span,
 * and which the writer may be updating at the same moment. The writer's set is live, and no retry would repair it.
 *
 * <p>
 * The race is timing dependent, so the test keeps mutating for a fixed budget rather than a fixed number of passes; it
 * reproduced within a few seconds on every run while the flag was a bit in the source's word.
 */
@Category(OutOfBandTest.class)
public class RspArrayConcurrentReaderSharedFlagTest {
    private static final long BS = RspArray.BLOCK_SIZE;
    private static final int BLOCKS = 6000;

    @Test
    public void testReaderDerivingSubsetsDoesNotCorruptTheWriterSet() throws InterruptedException {
        run(true, 6_000_000_000L);
    }

    /** Control: a reader that only takes and drops references never writes into the source. */
    @Test
    public void testReaderTakingReferencesOnlyDoesNotCorruptTheWriterSet() throws InterruptedException {
        run(false, 1_000_000_000L);
    }

    private static void run(final boolean readerDerivesSubsets, final long budgetNanos) throws InterruptedException {
        // One packed ArrayContainer (scattered values) per block, so every span's spanInfo carries cardinality bits.
        final RowSetBuilderSequential b = RowSetFactory.builderSequential();
        for (int i = 0; i < BLOCKS; ++i) {
            final long base = (long) i * BS;
            for (int j = 0; j < 8; ++j) {
                b.appendKey(base + 3L * j + 1);
            }
        }
        final WritableRowSet live = b.build();
        final AtomicBoolean done = new AtomicBoolean(false);
        final AtomicReference<Throwable> readerFailure = new AtomicReference<>();
        final AtomicLong readerWork = new AtomicLong(); // keeps the reads from being optimized away
        final AtomicLong readerCompleted = new AtomicLong(); // derivations that ran to completion
        final Runnable readerLoop = () -> {
            try {
                while (!done.get()) {
                    // A gap between snapshot attempts, as a real reader has between snapshots: it is what lets the
                    // writer find its set unshared (and so mutate it in place) just before the next attempt begins.
                    final long t0 = System.nanoTime();
                    while (System.nanoTime() - t0 < 100_000) {
                        Thread.onSpinWait();
                    }
                    // A snapshot-style read: take a reference to the current set, then derive a sub range from it.
                    // Nothing the reader observes is checked: racing the writer, it may see torn sizes and keys, and
                    // the contract only promises that it can discard such a result and retry. The test's assertions
                    // are on the writer's set, plus a count of derivations that ran to completion, so that a reader
                    // failing on every attempt cannot make the test pass by never sharing anything.
                    try (final RowSet c = live.copy()) {
                        if (readerDerivesSubsets) {
                            try (final RowSet sub = c.subSetByKeyRange(1 + 1, Long.MAX_VALUE - 1)) {
                                readerWork.addAndGet(sub.size());
                            }
                        } else {
                            readerWork.addAndGet(c.size() + c.firstRowKey());
                        }
                        readerCompleted.incrementAndGet();
                    } catch (RuntimeException e) {
                        // A torn read is allowed by the contract; the reader would retry.
                    }
                }
            } catch (Throwable t) {
                readerFailure.set(t);
            }
        };
        final Thread reader = new Thread(readerLoop, "reader");
        reader.setDaemon(true);
        reader.start();
        Throwable writerFailure = null;
        long expectedSize = live.size();
        try {
            // Each pass adds one new value to every block, in place when the reader does not hold a reference.
            final long deadline = System.nanoTime() + budgetNanos;
            for (int pass = 0; System.nanoTime() < deadline && writerFailure == null; ++pass) {
                final RowSetBuilderSequential add = RowSetFactory.builderSequential();
                for (int i = 0; i < BLOCKS; ++i) {
                    add.appendKey((long) i * BS + 100 + pass);
                }
                try (final RowSet toAdd = add.build()) {
                    live.insert(toAdd);
                    expectedSize += toAdd.size();
                }
                if (live.size() != expectedSize) {
                    writerFailure = new AssertionError("pass " + pass + ": writer's set lost or gained keys: size="
                            + live.size() + " expected=" + expectedSize);
                    break;
                }
                if ((pass & 15) == 15) {
                    try {
                        live.validate("pass " + pass);
                    } catch (RuntimeException | AssertionError e) {
                        writerFailure = e;
                    }
                }
            }
        } finally {
            done.set(true);
            reader.join(10_000);
        }
        assertFalse("the reader did not stop within 10 seconds of being told to", reader.isAlive());
        assertNull("writer's live set was corrupted by a concurrent reader: " + writerFailure, writerFailure);
        assertNull("reader failed unexpectedly: " + readerFailure.get(), readerFailure.get());
        assertTrue("the reader never completed a derivation, so nothing was shared with the writer's set",
                readerCompleted.get() > 0);
        live.close();
    }
}

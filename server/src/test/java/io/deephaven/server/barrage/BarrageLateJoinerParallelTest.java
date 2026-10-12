//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.barrage;

/**
 * Runs {@link BarrageLateJoinerTest} with every producer writing each propagation phase to its subscribers on four
 * threads, so that late joiners' snapshots and compaction meet parallel writes.
 */
public class BarrageLateJoinerParallelTest extends BarrageLateJoinerTest {
    @Override
    protected int propagationThreads() {
        return 4;
    }
}

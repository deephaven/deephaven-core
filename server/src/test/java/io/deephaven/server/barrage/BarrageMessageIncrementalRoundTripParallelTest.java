//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.barrage;

/**
 * Runs {@link BarrageMessageIncrementalRoundTripTestBase} with every producer writing each propagation phase to its
 * subscribers on four threads.
 */
public class BarrageMessageIncrementalRoundTripParallelTest extends BarrageMessageIncrementalRoundTripTestBase {
    @Override
    protected int propagationThreads() {
        return 4;
    }
}

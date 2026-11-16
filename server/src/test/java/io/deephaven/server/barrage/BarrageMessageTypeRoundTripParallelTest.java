//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.barrage;

/**
 * Runs {@link BarrageMessageTypeRoundTripTest} with every producer writing each propagation phase to its subscribers on
 * four threads.
 */
public class BarrageMessageTypeRoundTripParallelTest extends BarrageMessageTypeRoundTripTest {
    @Override
    protected int propagationThreads() {
        return 4;
    }
}

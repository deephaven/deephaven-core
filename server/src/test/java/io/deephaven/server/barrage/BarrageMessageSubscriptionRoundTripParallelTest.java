//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.barrage;

/**
 * Runs {@link BarrageMessageSubscriptionRoundTripTestBase} with every producer writing each propagation phase to its
 * subscribers on four threads.
 */
public class BarrageMessageSubscriptionRoundTripParallelTest extends BarrageMessageSubscriptionRoundTripTestBase {
    @Override
    protected int propagationThreads() {
        return 4;
    }

    /**
     * Only the last: where a change lands among the coalesced updates decides what the producer sends, which the serial
     * run covers at every step, not how it writes to its subscribers.
     */
    @Override
    protected int[] subscriptionChangeSteps(final int numCoalesce) {
        return new int[] {numCoalesce - 1};
    }
}

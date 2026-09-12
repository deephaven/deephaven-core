//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.barrage;

import io.deephaven.test.types.OutOfBandTest;
import org.junit.experimental.categories.Category;

/**
 * The whole subscription round-trip suite and the late-joiner suite, re-run with the producer forced to compact its
 * pending queue after every one, two or three recorded deltas, on the update graph thread so that compaction
 * interleaves deterministically with the cycles, subscription changes and snapshots those suites perform. Every
 * assertion those suites make about what subscribers see must hold unchanged.
 */
public class BarrageCompactingRoundTripTest {

    static void forceCompactionEvery(final BarrageMessageProducer producer, final int deltas) {
        producer.setCompactionPolicy(true, Long.MAX_VALUE, 1.0, deltas, true);
    }

    @Category(OutOfBandTest.class)
    public static class SubscriptionEvery1 extends BarrageMessageSubscriptionRoundTripTest {
        @Override
        protected void configureProducer(final BarrageMessageProducer producer) {
            forceCompactionEvery(producer, 1);
        }
    }

    @Category(OutOfBandTest.class)
    public static class SubscriptionEvery2 extends BarrageMessageSubscriptionRoundTripTest {
        @Override
        protected void configureProducer(final BarrageMessageProducer producer) {
            forceCompactionEvery(producer, 2);
        }
    }

    @Category(OutOfBandTest.class)
    public static class SubscriptionEvery3 extends BarrageMessageSubscriptionRoundTripTest {
        @Override
        protected void configureProducer(final BarrageMessageProducer producer) {
            forceCompactionEvery(producer, 3);
        }
    }

    @Category(OutOfBandTest.class)
    public static class LateJoinerEvery1 extends BarrageLateJoinerTest {
        @Override
        protected void configureProducer(final BarrageMessageProducer producer) {
            forceCompactionEvery(producer, 1);
        }
    }

    @Category(OutOfBandTest.class)
    public static class LateJoinerEvery2 extends BarrageLateJoinerTest {
        @Override
        protected void configureProducer(final BarrageMessageProducer producer) {
            forceCompactionEvery(producer, 2);
        }
    }

    @Category(OutOfBandTest.class)
    public static class LateJoinerEvery3 extends BarrageLateJoinerTest {
        @Override
        protected void configureProducer(final BarrageMessageProducer producer) {
            forceCompactionEvery(producer, 3);
        }
    }
}

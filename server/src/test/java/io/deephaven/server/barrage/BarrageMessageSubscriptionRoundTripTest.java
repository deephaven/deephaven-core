//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.barrage;

import io.deephaven.test.types.OutOfBandTest;
import org.junit.experimental.categories.Category;

/**
 * Runs {@link BarrageMessageSubscriptionRoundTripTestBase} with the producer writing to subscribers in turn, as a
 * server with one propagation thread does.
 */
@Category(OutOfBandTest.class)
public class BarrageMessageSubscriptionRoundTripTest extends BarrageMessageSubscriptionRoundTripTestBase {
}

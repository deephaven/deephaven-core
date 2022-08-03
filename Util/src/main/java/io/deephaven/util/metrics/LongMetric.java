/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.util.metrics;

import java.util.function.LongConsumer;

public interface LongMetric extends LongConsumer {
    void sample(long v);

    default void accept(final long v) {
        sample(v);
    }
}

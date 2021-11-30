package io.deephaven.util.metrics;

import java.util.function.LongConsumer;

public interface LongMetric extends LongConsumer {
    void sample(long v);

    default void accept(final long v) {
        sample(v);
    }
}

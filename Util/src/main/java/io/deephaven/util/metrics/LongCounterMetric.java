/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.util.metrics;

public class LongCounterMetric implements LongMetric {
    private final int id;

    public LongCounterMetric(final String name) {
        id = MetricsManager.instance.registerLongCounterMetric(name);
    }

    @Override
    public void sample(final long n) {
        MetricsManager.instance.sampleLongCounter(id, n);
    }
}

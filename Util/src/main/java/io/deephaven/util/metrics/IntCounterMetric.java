/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.util.metrics;

public class IntCounterMetric implements IntMetric {
    private final int id;

    public IntCounterMetric(final String name) {
        id = MetricsManager.instance.registerIntCounterMetric(name);
    }

    @Override
    public void sample(final int n) {
        MetricsManager.instance.sampleIntCounter(id, n);
    }
}

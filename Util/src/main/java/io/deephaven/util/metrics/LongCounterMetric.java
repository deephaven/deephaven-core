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

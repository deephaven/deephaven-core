package io.deephaven.util.metrics;

public class LongCounterLog2HistogramMetric implements LongMetric {
    private final int id;

    public LongCounterLog2HistogramMetric(final String name) {
        id = MetricsManager.instance.registerLongCounterLog2HistogramMetric(name);
    }

    @Override
    public void sample(final long v) {
        MetricsManager.instance.sampleLongCounterLog2HistogramCount(id, v);
    }
}

/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.util.metrics;

public interface IntMetric {
    void sample(int v);
}

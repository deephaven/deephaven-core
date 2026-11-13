//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.by.staticpercentile;

import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.sources.LongAsInstantColumnSource;

import java.util.Collections;
import java.util.Map;

/**
 * {@link StaticPercentileOperator} for Instant values, which arrive reinterpreted as epoch nanoseconds. Results are
 * never averaged.
 */
final class InstantStaticPercentileOperator extends LongStaticPercentileOperator {
    private final ColumnSource<?> instantResult;

    InstantStaticPercentileOperator(final double percentile, final String name) {
        super(percentile, false, name);
        // noinspection unchecked
        instantResult = new LongAsInstantColumnSource((ColumnSource<Long>) resultColumn());
    }

    @Override
    public Map<String, ? extends ColumnSource<?>> getResultColumns() {
        return Collections.<String, ColumnSource<?>>singletonMap(name, instantResult);
    }
}

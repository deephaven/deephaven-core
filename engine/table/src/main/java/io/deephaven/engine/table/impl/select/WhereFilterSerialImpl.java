//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

/**
 * A serial wrapper for a {@link WhereFilter} that enforces serial (non-parallel) execution.
 */
public class WhereFilterSerialImpl extends WhereFilterDelegatingBase {
    /**
     * Wraps the provided {@link WhereFilter} with serial behavior.
     *
     * @param filter the filter to wrap
     * @return a new {@code WhereFilterSerialImpl} instance that enforces serial execution
     */
    public static WhereFilter of(WhereFilter filter) {
        return new WhereFilterSerialImpl(filter);
    }

    private WhereFilterSerialImpl(WhereFilter filter) {
        super(filter);
    }

    /**
     * Always returns {@code false} to indicate that parallelization is not permitted.
     *
     * @return false
     */
    @Override
    public boolean permitParallelization() {
        return false;
    }

    @Override
    public boolean isSerial() {
        return true;
    }

    public WhereFilter copy() {
        return new WhereFilterSerialImpl(filter.copy());
    }

    @Override
    public String toString() {
        return "serial{" + filter + "}";
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }
}

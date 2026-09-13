//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.pushdown.fuzz;

import io.deephaven.engine.context.ExecutionContext;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A {@link FuzzType.LiteralSink} backed by the query scope.
 *
 * <p>
 * Many of the values this bench cares about cannot be written as formula literals at all -- {@code NaN}, the
 * infinities, {@code BigDecimal} scale extremes, lone-surrogate {@code char}s, {@code Instant}s outside the
 * {@code DateTimeUtils} literal syntax. Rather than escape them into text (a bug farm that would test the fuzzer's
 * escaping rather than pushdown), every such value is bound to a query-scope parameter and referenced by name, which is
 * exact.
 */
public final class FuzzScope implements FuzzType.LiteralSink {

    private final Map<String, Object> bound = new LinkedHashMap<>();
    private int next;

    @Override
    public String bind(final Object value) {
        final String name = "fuzzP" + (next++);
        ExecutionContext.getContext().getQueryScope().putParam(name, value);
        bound.put(name, value);
        return name;
    }

    /** The parameters bound so far, for the case description. */
    public Map<String, Object> bound() {
        return bound;
    }

    /**
     * Re-publish every bound parameter into the current query scope. Needed because the harness runs each case inside
     * its own {@link io.deephaven.engine.context.ExecutionContext}, and a filter built earlier may be re-evaluated
     * after that context is re-opened.
     */
    public void republish() {
        bound.forEach((name, value) -> ExecutionContext.getContext().getQueryScope().putParam(name, value));
    }

    @Override
    public String toString() {
        if (bound.isEmpty()) {
            return "{}";
        }
        final StringBuilder sb = new StringBuilder("{");
        bound.forEach((k, v) -> sb.append(k).append('=').append(describe(v)).append(", "));
        sb.setLength(sb.length() - 2);
        return sb.append('}').toString();
    }

    /** Render a bound value so the case description can be pasted back into a repro. */
    private static String describe(final Object value) {
        if (value == null) {
            return "null";
        }
        if (value instanceof Character) {
            return "(char) 0x" + Integer.toHexString((Character) value);
        }
        if (value instanceof Float) {
            final float f = (Float) value;
            if (Float.isNaN(f)) {
                return "Float.NaN";
            }
            if (f == Float.POSITIVE_INFINITY) {
                return "Float.POSITIVE_INFINITY";
            }
            if (f == Float.NEGATIVE_INFINITY) {
                return "Float.NEGATIVE_INFINITY";
            }
            return f + "f";
        }
        if (value instanceof Double) {
            final double d = (Double) value;
            if (Double.isNaN(d)) {
                return "Double.NaN";
            }
            if (d == Double.POSITIVE_INFINITY) {
                return "Double.POSITIVE_INFINITY";
            }
            if (d == Double.NEGATIVE_INFINITY) {
                return "Double.NEGATIVE_INFINITY";
            }
            return String.valueOf(d);
        }
        if (value instanceof String) {
            final String s = (String) value;
            final StringBuilder sb = new StringBuilder(s.length() + 2).append('"');
            for (int ii = 0; ii < s.length(); ++ii) {
                final char c = s.charAt(ii);
                if (c < ' ' || c > '~') {
                    sb.append(String.format("\\u%04X", (int) c));
                } else {
                    sb.append(c);
                }
            }
            return sb.append('"').toString();
        }
        return value.getClass().getSimpleName() + "(" + value + ")";
    }
}

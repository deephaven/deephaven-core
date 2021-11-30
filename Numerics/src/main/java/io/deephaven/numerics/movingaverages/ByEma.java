/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.numerics.movingaverages;

import io.deephaven.base.verify.Require;
import io.deephaven.time.DateTime;
import io.deephaven.function.DoublePrimitives;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * An EMA which can compute aggregated EMAs without grouping and then ungrouping.
 */
public abstract class ByEma implements Serializable {

    private static final long serialVersionUID = -6541641502689292655L;

    public static class Key implements Serializable {
        private static final long serialVersionUID = 8481816497504935854L;
        final Object[] values;

        public Key(Object... values) {
            this.values = values;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            Key key = (Key) o;
            return Arrays.equals(values, key.values);
        }

        @Override
        public int hashCode() {
            return values != null ? Arrays.hashCode(values) : 0;
        }

        @Override
        public String toString() {
            return "Key{" +
                    "values=" + (values == null ? null : Arrays.asList(values)) +
                    '}';
        }
    }

    public enum BadDataBehavior {
        BD_RESET(true, false, true), BD_SKIP(false, false, false), BD_PROCESS(false, true, false);

        private final boolean reset;
        private final boolean process;
        private final boolean returnNan;

        BadDataBehavior(boolean reset, boolean process, boolean returnNan) {
            this.reset = reset;
            this.process = process;
            this.returnNan = returnNan;
        }
    }

    private final BadDataBehavior nullBehavior;
    private final BadDataBehavior nanBehavior;
    private final Map<Key, AbstractMa> emas = new HashMap<>();

    protected ByEma(BadDataBehavior nullBehavior, BadDataBehavior nanBehavior) {
        this.nullBehavior = nullBehavior;
        this.nanBehavior = nanBehavior;

        Require.neqNull(nullBehavior, "nullBehavior");
        Require.neqNull(nanBehavior, "nanBehavior");
    }

    // Engine automatic type conversion takes care of converting all non-double nulls into double nulls so we don't have
    // to duplicate the null checking for each type.

    public synchronized double update(double value) {
        return update(value, (Object) null);
    }

    public synchronized double update(double value, Object... by) {
        return update(Long.MIN_VALUE, value, by);
    }

    public synchronized double update(DateTime timestamp, double value) {
        return update(timestamp, value, (Object) null);
    }

    public synchronized double update(DateTime timestamp, double value, Object... by) {
        return update(timestamp.getNanos(), value, by);
    }

    public synchronized double update(long timestampNanos, double value, Object... by) {
        return updateInternal(timestampNanos, value, DoublePrimitives.isNull(value), Double.isNaN(value), by);
    }

    private static boolean resetEma(boolean isNull, BadDataBehavior nullBehavior, boolean isNaN,
            BadDataBehavior nanBehavior) {
        return (isNull && nullBehavior.reset) || (isNaN && nanBehavior.reset);
    }

    private static boolean returnNan(boolean isNull, BadDataBehavior nullBehavior, boolean isNaN,
            BadDataBehavior nanBehavior) {
        return (isNull && nullBehavior.returnNan) || (isNaN && nanBehavior.returnNan);
    }

    private static boolean processSample(boolean isNull, BadDataBehavior nullBehavior, boolean isNaN,
            BadDataBehavior nanBehavior) {
        return (!isNull && !isNaN) || (isNull && nullBehavior.process) || (isNaN && nanBehavior.process);
    }

    private synchronized double updateInternal(long timestampNanos, double value, boolean isNull, boolean isNaN,
            Object... by) {
        Key key = new Key(by);

        AbstractMa ema = emas.get(key);

        if (ema == null || resetEma(isNull, nullBehavior, isNaN, nanBehavior)) {
            ema = createEma(key);
            emas.put(key, ema);
        }

        if (processSample(isNull, nullBehavior, isNaN, nanBehavior)) {
            ema.processDouble(timestampNanos, value);
        }

        return returnNan(isNull, nullBehavior, isNaN, nanBehavior) ? Double.NaN : ema.getCurrent();
    }

    protected abstract AbstractMa createEma(Key key);
}

package io.deephaven.engine.rowset.impl.rsp.container;

public class MutableInteger {
    public int value;

    public MutableInteger() {
        value = 0;
    }

    public MutableInteger(final int v) {
        value = v;
    }

    public static int getIfNotNullAndNonNegative(final MutableInteger mi, final int defaultValue) {
        if (mi == null || mi.value < 0) {
            return defaultValue;
        }
        return mi.value;
    }

    public static void setIfNotNull(final MutableInteger mi, final int v) {
        if (mi == null) {
            return;
        }
        mi.value = v;
    }
}

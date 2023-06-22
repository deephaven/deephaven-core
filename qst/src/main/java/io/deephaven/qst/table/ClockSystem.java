/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.qst.table;

/**
 * The system time provider.
 */
public enum ClockSystem implements Clock {
    INSTANCE;

    @Override
    public final <R> R walk(Visitor<R> visitor) {
        return visitor.visit(this);
    }
}

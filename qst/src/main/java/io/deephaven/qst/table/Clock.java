/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.qst.table;

/**
 * The time provider for a {@link TimeTable}.
 *
 * @see ClockSystem
 */
public interface Clock {

    <V extends Visitor> V walk(V visitor);

    interface Visitor {
        void visit(ClockSystem system);
    }
}

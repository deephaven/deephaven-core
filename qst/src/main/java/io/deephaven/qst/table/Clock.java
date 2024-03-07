//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.qst.table;

/**
 * The time provider for a {@link TimeTable}.
 *
 * @see ClockSystem
 */
public interface Clock {

    <R> R walk(Visitor<R> visitor);

    interface Visitor<R> {
        R visit(ClockSystem system);
    }
}

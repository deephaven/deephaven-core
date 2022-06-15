/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.naturaljoin;

/**
 * This signals that our helper should look for a duplicate right hand side row, and then throw an error.
 */
public class DuplicateRightRowDecorationException extends RuntimeException {
    final public long tablePosition;

    public DuplicateRightRowDecorationException(long tablePosition) {
        this.tablePosition = tablePosition;
    }
}

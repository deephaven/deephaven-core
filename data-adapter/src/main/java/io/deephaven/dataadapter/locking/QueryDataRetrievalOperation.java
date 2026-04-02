//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.dataadapter.locking;

/**
 * An operation that uses data from Deephaven {@link io.deephaven.engine.table.Table Tables}, using either
 * {@link io.deephaven.engine.table.ColumnSource#getPrev} or {@link io.deephaven.engine.table.ColumnSource#get})
 * depending on the value of the argument to {@link #retrieveData}.
 */
@FunctionalInterface
public interface QueryDataRetrievalOperation {

    /**
     * Performs an operation using data from a query.
     *
     * @param usePrev Whether to use the previous data at a given index when retrieving data (i.e. if {@code true}, use
     *        {@link io.deephaven.engine.table.ColumnSource#getPrev} instead of
     *        {@link io.deephaven.engine.table.ColumnSource#get}).
     */
    boolean retrieveData(boolean usePrev);

}

//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.dataadapter.rec.updaters;

/**
 * Updates one field of a record of type {@code} R with a value of type {@code C}.
 *
 * @param <R> the record type
 */
public interface RecordUpdater<R, C> {

    /**
     * Returns the source type {@code C} for this record updater (i.e. the data type that is taken as an input to update
     * a field of the record of type {@code R}).
     *
     * @return The supported source type of this record updater.
     */
    Class<C> getSourceType();

}

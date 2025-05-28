//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.dataadapter.rec.updaters;

/**
 * Updates one field of a record of type {@code} R with a value of type {@code short}.
 *
 * @param <R> the record type
 */
public interface ShortRecordUpdater<R> extends RecordUpdater<R, Short> {
    default Class<Short> getSourceType() {
        return short.class;
    }

    void accept(R record, short colValue);

}

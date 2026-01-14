//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.dataadapter.rec.updaters;

import io.deephaven.dataadapter.consumers.ObjIntConsumer;

/**
 * Updates one field of a record of type {@code} R with a value of type {@code int}.
 *
 * @param <R> the record type
 */
public interface IntRecordUpdater<R> extends RecordUpdater<R, Integer>, ObjIntConsumer<R> {
    default Class<Integer> getSourceType() {
        return int.class;
    }
}

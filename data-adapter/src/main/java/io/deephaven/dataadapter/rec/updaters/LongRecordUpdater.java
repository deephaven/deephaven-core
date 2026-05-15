//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.dataadapter.rec.updaters;

import io.deephaven.dataadapter.consumers.ObjLongConsumer;

/**
 * Updates one field of a record of type {@code} R with a value of type {@code long}.
 *
 * @param <R> the record type
 */
public interface LongRecordUpdater<R> extends RecordUpdater<R, Long>, ObjLongConsumer<R> {
    default Class<Long> getSourceType() {
        return long.class;
    }

}

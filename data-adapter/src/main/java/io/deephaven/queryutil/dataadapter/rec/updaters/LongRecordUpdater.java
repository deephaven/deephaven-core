package io.deephaven.queryutil.dataadapter.rec.updaters;

import io.deephaven.queryutil.dataadapter.consumers.ObjLongConsumer;

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

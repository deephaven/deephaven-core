package io.deephaven.queryutil.dataadapter.rec.updaters;

import io.deephaven.queryutil.dataadapter.consumers.ObjFloatConsumer;

/**
 * Updates one field of a record of type {@code} R with a value of type {@code float}.
 *
 * @param <R> the record type
 */
public interface FloatRecordUpdater<R> extends RecordUpdater<R, Float>, ObjFloatConsumer<R> {
    default Class<Float> getSourceType() {
        return float.class;
    }
}

package io.deephaven.dataadapter.rec.updaters;

import io.deephaven.dataadapter.consumers.ObjDoubleConsumer;

/**
 * Updates one field of a record of type {@code} R with a value of type {@code double}.
 *
 * @param <R> the record type
 */
public interface DoubleRecordUpdater<R> extends RecordUpdater<R, Double>, ObjDoubleConsumer<R> {
    default Class<Double> getSourceType() {
        return double.class;
    }
}

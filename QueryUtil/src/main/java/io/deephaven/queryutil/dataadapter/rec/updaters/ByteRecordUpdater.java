package io.deephaven.queryutil.dataadapter.rec.updaters;

import io.deephaven.queryutil.dataadapter.consumers.ObjByteConsumer;

/**
 * Updates one field of a record of type {@code} R with a value of type {@code byte}.
 *
 * @param <R> the record type
 */
public interface ByteRecordUpdater<R> extends RecordUpdater<R, Byte>, ObjByteConsumer<R> {
    default Class<Byte> getSourceType() {
        return byte.class;
    }
}

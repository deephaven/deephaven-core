package io.deephaven.queryutil.dataadapter.rec.updaters;

import io.deephaven.queryutil.dataadapter.consumers.ObjCharConsumer;

/**
 * Updates one field of a record of type {@code} R with a value of type {@code char}.
 *
 * @param <R> the record type
 */
public interface CharRecordUpdater<R> extends RecordUpdater<R, Character>, ObjCharConsumer<R> {
    default Class<Character> getSourceType() {
        return char.class;
    }
}

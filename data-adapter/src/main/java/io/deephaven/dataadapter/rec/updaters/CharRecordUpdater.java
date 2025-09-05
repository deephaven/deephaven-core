//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.dataadapter.rec.updaters;

import io.deephaven.dataadapter.consumers.ObjCharConsumer;

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

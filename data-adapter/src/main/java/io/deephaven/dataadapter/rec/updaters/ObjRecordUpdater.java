//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.dataadapter.rec.updaters;

import io.deephaven.util.type.TypeUtils;

import java.util.function.BiConsumer;

/**
 * Updates one field of a record of type {@code} R with a value of type {@code T}.
 *
 * @param <R> the record type
 * @param <T> the type of the source object to add to the record
 */
public interface ObjRecordUpdater<R, T> extends RecordUpdater<R, T>, BiConsumer<R, T> {

    /**
     * Creates a {@code RecordUpdater} that updates a record with an object retrieved from a column. If {@code colType}
     * is a primitive type, then the returned RecordUpdater will box the primitive value (with proper conversion of DB
     * nulls) before calling the provided {@code recordUpdater}.
     * <p>
     * Use the type-specific record updaters such as {@link IntRecordUpdater} to avoid boxing.
     *
     * @param colType The column's data type.
     * @param recordUpdater An updater to populate an instance of {@code R} with an object from a column.
     * @param <R> The record type.
     * @return An updater that passes objects (with DB null-safe boxing, if necessary) to the given
     *         {@code recordUpdater}.
     */
    static <R> RecordUpdater<R, ?> getBoxingUpdater(final Class<?> colType,
            final ObjRecordUpdater<R, Object> recordUpdater) {
        final RecordUpdater<R, ?> updater;
        if (!colType.isPrimitive()) {
            updater = recordUpdater;
        } else if (char.class.equals(colType)) {
            updater = (CharRecordUpdater<R>) (record, val) -> recordUpdater.accept(record, TypeUtils.box(val));
        } else if (byte.class.equals(colType)) {
            updater = (ByteRecordUpdater<R>) (record, val) -> recordUpdater.accept(record, TypeUtils.box(val));
        } else if (short.class.equals(colType)) {
            updater = (ShortRecordUpdater<R>) (record, val) -> recordUpdater.accept(record, TypeUtils.box(val));
        } else if (int.class.equals(colType)) {
            updater = (IntRecordUpdater<R>) (record, val) -> recordUpdater.accept(record, TypeUtils.box(val));
        } else if (float.class.equals(colType)) {
            updater = (FloatRecordUpdater<R>) (record, val) -> recordUpdater.accept(record, TypeUtils.box(val));
        } else if (long.class.equals(colType)) {
            updater = (LongRecordUpdater<R>) (record, val) -> recordUpdater.accept(record, TypeUtils.box(val));
        } else if (double.class.equals(colType)) {
            updater = (DoubleRecordUpdater<R>) (record, val) -> recordUpdater.accept(record, TypeUtils.box(val));
        } else {
            throw new IllegalStateException("Unexpected type: " + colType.getName());
        }

        return updater;
    }

    static <R, T> ObjRecordUpdater<R, T> getObjectUpdater(final Class<T> colType,
            final BiConsumer<R, T> recordUpdater) {
        return new ObjRecordUpdater<>() {
            @Override
            public Class<T> getSourceType() {
                return colType;
            }

            @Override
            public void accept(R r, T t) {
                recordUpdater.accept(r, t);
            }
        };
    }

    static <R> ObjRecordUpdater<R, Object> getObjectUpdater(final BiConsumer<R, Object> recordUpdater) {
        return getObjectUpdater(Object.class, recordUpdater);
    }


}

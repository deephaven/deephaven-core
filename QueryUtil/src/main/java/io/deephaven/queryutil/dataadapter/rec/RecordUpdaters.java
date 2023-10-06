package io.deephaven.queryutil.dataadapter.rec;

import io.deephaven.base.verify.Assert;
import io.deephaven.queryutil.dataadapter.consumers.*;
import io.deephaven.util.type.TypeUtils;

import java.util.function.BiConsumer;

/**
 * Utility for producing type-specific
 */
public class RecordUpdaters {

    /**
     * Creates a {@code RecordUpdater} that updates a record with an object retrieved from a column. If {@code colType}
     * is a primitive type, then the returned RecordUpdater will box the primitive value (with proper conversion of DB
     * nulls) before calling the provided {@code recordUpdater}.
     * <p>
     * Use the type-specific methods such as {@link #getIntUpdater} to avoid boxing. Use
     * {@link #getReferenceTypeUpdater} to capture the column type as the type of the second argument to the
     * {@code recordUpdater}.
     *
     * @param colType The column's data type.
     * @param recordUpdater An updater to populate an instance of {@code R} with an object from a column.
     * @param <R> The record type.
     * @return An updater that passes objects (with DB null-safe boxing, if necessary) to the given
     *         {@code recordUpdater}.
     */
    public static <R> RecordUpdater<R, ?> getObjectUpdater(final Class<?> colType,
            final BiConsumer<R, Object> recordUpdater) {
        final RecordUpdater<R, ?> updater;
        if (!colType.isPrimitive()) {
            updater = getReferenceTypeUpdater(colType, recordUpdater::accept);
        } else if (boolean.class.equals(colType)) {
            updater = getBooleanUpdater((record, val) -> recordUpdater.accept(record, TypeUtils.box(val)));
        } else if (char.class.equals(colType)) {
            updater = getCharUpdater((record, val) -> recordUpdater.accept(record, TypeUtils.box(val)));
        } else if (byte.class.equals(colType)) {
            updater = getByteUpdater((record, val) -> recordUpdater.accept(record, TypeUtils.box(val)));
        } else if (short.class.equals(colType)) {
            updater = getShortUpdater((record, val) -> recordUpdater.accept(record, TypeUtils.box(val)));
        } else if (int.class.equals(colType)) {
            updater = getIntUpdater((record, val) -> recordUpdater.accept(record, TypeUtils.box(val)));
        } else if (float.class.equals(colType)) {
            updater = getFloatUpdater((record, val) -> recordUpdater.accept(record, TypeUtils.box(val)));
        } else if (long.class.equals(colType)) {
            updater = getLongUpdater((record, val) -> recordUpdater.accept(record, TypeUtils.box(val)));
        } else if (double.class.equals(colType)) {
            updater = getDoubleUpdater((record, val) -> recordUpdater.accept(record, TypeUtils.box(val)));
        } else {
            throw Assert.statementNeverExecuted();
        }
        return updater;
    }

    public static <R, C> RecordUpdater<R, C> getReferenceTypeUpdater(Class<C> type, BiConsumer<R, C> recordUpdater) {
        return new RecordUpdater<>() {
            @Override
            public Class<C> getSourceType() {
                return type;
            }

            @Override
            public void updateRecord(R record, C colValue) {
                recordUpdater.accept(record, colValue);
            }
        };
    }

    public static <R> RecordUpdater<R, String> getStringUpdater(BiConsumer<R, String> recordUpdater) {
        return getReferenceTypeUpdater(String.class, recordUpdater);
    }

    public static <R> RecordUpdater<R, Boolean> getBooleanUpdater(BiConsumer<R, Boolean> recordUpdater) {
        return getReferenceTypeUpdater(Boolean.class, recordUpdater);
    }

    public static <R> RecordUpdater<R, Character> getCharUpdater(ObjCharConsumer<R> recordUpdater) {
        return new RecordUpdater<>() {
            @Override
            public Class<Character> getSourceType() {
                return char.class;
            }

            @Override
            public void updateRecordWithChar(R record, char colValue) {
                recordUpdater.accept(record, colValue);
            }
        };
    }

    public static <R> RecordUpdater<R, Byte> getByteUpdater(ObjByteConsumer<R> recordUpdater) {
        return new RecordUpdater<>() {
            @Override
            public Class<Byte> getSourceType() {
                return byte.class;
            }

            @Override
            public void updateRecordWithByte(R record, byte colValue) {
                recordUpdater.accept(record, colValue);
            }
        };
    }

    public static <R> RecordUpdater<R, Short> getShortUpdater(ObjShortConsumer<R> recordUpdater) {
        return new RecordUpdater<>() {
            @Override
            public Class<Short> getSourceType() {
                return short.class;
            }

            @Override
            public void updateRecordWithShort(R record, short colValue) {
                recordUpdater.accept(record, colValue);
            }
        };
    }

    public static <R> RecordUpdater<R, Integer> getIntUpdater(ObjIntConsumer<R> recordUpdater) {
        return new RecordUpdater<>() {
            @Override
            public Class<Integer> getSourceType() {
                return int.class;
            }

            @Override
            public void updateRecordWithInt(R record, int colValue) {
                recordUpdater.accept(record, colValue);
            }
        };
    }

    public static <R> RecordUpdater<R, Float> getFloatUpdater(ObjFloatConsumer<R> recordUpdater) {
        return new RecordUpdater<>() {
            @Override
            public Class<Float> getSourceType() {
                return float.class;
            }

            @Override
            public void updateRecordWithFloat(R record, float colValue) {
                recordUpdater.accept(record, colValue);
            }
        };
    }

    public static <R> RecordUpdater<R, Long> getLongUpdater(ObjLongConsumer<R> recordUpdater) {
        return new RecordUpdater<>() {
            @Override
            public Class<Long> getSourceType() {
                return long.class;
            }

            @Override
            public void updateRecordWithLong(R record, long colValue) {
                recordUpdater.accept(record, colValue);
            }
        };
    }

    public static <R> RecordUpdater<R, Double> getDoubleUpdater(ObjDoubleConsumer<R> recordUpdater) {
        return new RecordUpdater<>() {
            @Override
            public Class<Double> getSourceType() {
                return double.class;
            }

            @Override
            public void updateRecordWithDouble(R record, double colValue) {
                recordUpdater.accept(record, colValue);
            }
        };
    }

    private RecordUpdaters() {}
}

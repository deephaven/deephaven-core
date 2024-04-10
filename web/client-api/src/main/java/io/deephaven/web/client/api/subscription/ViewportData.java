//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.subscription;

import com.vertispan.tsdefs.annotations.TsInterface;
import com.vertispan.tsdefs.annotations.TsName;
import elemental2.core.JsArray;
import elemental2.core.JsObject;
import io.deephaven.web.client.api.*;
import io.deephaven.web.shared.data.*;
import jsinterop.annotations.JsFunction;
import jsinterop.annotations.JsProperty;
import jsinterop.base.Any;
import jsinterop.base.Js;
import jsinterop.base.JsArrayLike;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.HashSet;
import java.util.Iterator;
import java.util.PrimitiveIterator.OfLong;
import java.util.Set;

/**
 * Extends {@link TableData}, but only contains data in the current viewport. The only API change from TableData is that
 * ViewportData also contains the offset to this data, so that the actual row number may be
 * determined.
 * <p>Do not assume that the first row in `rows` is the first visible row, because extra rows may be provided
 * for easier scrolling without going to the server.
 */
@TsInterface
@TsName(namespace = "dh")
public interface ViewportData extends TableData {

    /**
     * The position of the first returned row, null if this data is not for a viewport.
     */
    @JsProperty
    Double getOffset();

    /**
     * Clean the data at the provided index
     */
    @JsFunction
    interface DataCleaner {
        void clean(JsArray<Any> data, int index);
    }

    @Deprecated
    public static final int NO_ROW_FORMAT_COLUMN = -1;

    public class MergeResults {
        public Set<Integer> added = new HashSet<>();
        public Set<Integer> modified = new HashSet<>();
        public Set<Integer> removed = new HashSet<>();
    }

    private static DataCleaner getDataCleanerForColumnType(String columnType) {
        switch (columnType) {
            case "int":
                return (data, i) -> {
                    int value = data.getAtAsAny(i).asInt();
                    if (value == QueryConstants.NULL_INT) {
                        data.setAt(i, null);
                    }
                };
            case "byte":
                return (data, i) -> {
                    byte value = data.getAtAsAny(i).asByte();
                    if (value == QueryConstants.NULL_BYTE) {
                        data.setAt(i, null);
                    }
                };
            case "short":
                return (data, i) -> {
                    short value = data.getAtAsAny(i).asShort();
                    if (value == QueryConstants.NULL_SHORT) {
                        data.setAt(i, null);
                    }
                };
            case "double":
                return (data, i) -> {
                    double value = data.getAtAsAny(i).asDouble();
                    if (value == QueryConstants.NULL_DOUBLE) {
                        data.setAt(i, null);
                    }
                };
            case "float":
                return (data, i) -> {
                    float value = data.getAtAsAny(i).asFloat();
                    if (value == QueryConstants.NULL_FLOAT) {
                        data.setAt(i, null);
                    }
                };
            case "char":
                return (data, i) -> {
                    char value = data.getAtAsAny(i).asChar();
                    if (value == QueryConstants.NULL_CHAR) {
                        data.setAt(i, null);
                    }
                };
            default:
                return null;
        }
    }

    public static Object cleanData(Object dataColumn, Column column) {
        if (dataColumn == null) {
            return null;
        }
        if (column == null) {
            return dataColumn;
        }

        switch (column.getType()) {
            case "long": {
                JsArray<Any> values = Js.uncheckedCast(dataColumn);
                LongWrapper[] cleanData = new LongWrapper[values.length];
                for (int i = 0; i < values.length; i++) {
                    long value = values.getAtAsAny(i).asLong();
                    if (value == QueryConstants.NULL_LONG) {
                        cleanData[i] = null;
                    } else {
                        cleanData[i] = LongWrapper.of(value);
                    }
                }
                return cleanData;
            }
            case "java.time.Instant":
            case "java.time.ZonedDateTime": {
                JsArray<Any> values = Js.uncheckedCast(dataColumn);
                DateWrapper[] cleanData = new DateWrapper[values.length];
                for (int i = 0; i < values.length; i++) {
                    long value = values.getAtAsAny(i).asLong();
                    if (value == QueryConstants.NULL_LONG) {
                        cleanData[i] = null;
                    } else {
                        cleanData[i] = new DateWrapper(value);
                    }
                }
                return cleanData;
            }
            case "java.math.BigDecimal": {
                final JsArray<Any> values = Js.uncheckedCast(dataColumn);
                final BigDecimalWrapper[] cleanData = new BigDecimalWrapper[values.length];
                for (int i = 0; i < values.length; i++) {
                    final BigDecimal value = Js.cast(values.getAt(i));
                    if (value == null) {
                        cleanData[i] = null;
                    } else {
                        cleanData[i] = new BigDecimalWrapper(value);
                    }
                }
                return cleanData;
            }
            case "java.math.BigInteger": {
                final JsArray<Any> values = Js.uncheckedCast(dataColumn);
                final BigIntegerWrapper[] cleanData = new BigIntegerWrapper[values.length];
                for (int i = 0; i < values.length; i++) {
                    final BigInteger value = Js.cast(values.getAt(i));
                    if (value == null) {
                        cleanData[i] = null;
                    } else {
                        cleanData[i] = new BigIntegerWrapper(value);
                    }
                }
                return cleanData;
            }
            case "java.time.LocalDate": {
                final JsArray<Any> values = Js.uncheckedCast(dataColumn);
                final LocalDateWrapper[] cleanData = new LocalDateWrapper[values.length];
                for (int i = 0; i < values.length; i++) {
                    final LocalDate value = Js.cast(values.getAt(i));
                    if (value == null) {
                        cleanData[i] = null;
                    } else {
                        cleanData[i] = new LocalDateWrapper(value);
                    }
                }
                return cleanData;
            }
            case "java.time.LocalTime": {
                final JsArray<Any> values = Js.uncheckedCast(dataColumn);
                final LocalTimeWrapper[] cleanData = new LocalTimeWrapper[values.length];
                for (int i = 0; i < values.length; i++) {
                    final LocalTime value = Js.cast(values.getAt(i));
                    if (value == null) {
                        cleanData[i] = null;
                    } else {
                        cleanData[i] = new LocalTimeWrapper(value);
                    }
                }
                return cleanData;
            }
            default:
                DataCleaner dataCleaner = getDataCleanerForColumnType(column.getType());
                if (dataCleaner != null) {
                    JsArray<Any> values = Js.uncheckedCast(dataColumn);
                    JsArray<Any> cleanData = Js.uncheckedCast(JsArray.from((JsArrayLike<Any>) values));

                    for (int i = 0; i < values.length; i++) {
                        dataCleaner.clean(cleanData, i);
                    }

                    return cleanData;
                } else {
                    return dataColumn;
                }
        }
    }
}

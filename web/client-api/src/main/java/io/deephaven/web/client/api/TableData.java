//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import com.vertispan.tsdefs.annotations.TsName;
import com.vertispan.tsdefs.annotations.TsTypeRef;
import com.vertispan.tsdefs.annotations.TsUnion;
import com.vertispan.tsdefs.annotations.TsUnionMember;
import elemental2.core.JsArray;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Any;
import jsinterop.base.Js;

/**
 * Common interface for various ways of accessing table data and formatting for viewport or non-viewport subscriptions on tables,
 * data in trees, and snapshots.
 * <p>
 * Generally speaking, it is more efficient to access data in column-major order, rather than iterating through
 * each Row and accessing all columns that it holds. The {@link #getRows()} accessor can be useful to read row data,
 * but may incur other costs - it is likely faster to access data by columns using {@link #getData(RowPositionUnion, Column)}.
 */
/*
 * Java note: this interface contains some extra overloads that aren't available in JS. Implementations are expected to
 * implement only abstract methods, and default methods present in this interface will dispatch accordingly.
 */
@TsName(namespace = "dh")
public interface TableData {
    public static final int NO_ROW_FORMAT_COLUMN = -1;

    /**
     * TS type union to allow either "int" or "LongWrapper" to be passed as an argument for various methods.
     */
    @TsUnion
    @JsType(name = "?", namespace = JsPackage.GLOBAL, isNative = true)
    interface RowPositionUnion {
        @JsOverlay
        default boolean isLongWrapper() {
            return this instanceof LongWrapper;
        }

        @JsOverlay
        default boolean isInt() {
            return (Object) this instanceof Double;
        }

        @JsOverlay
        @TsUnionMember
        default LongWrapper asLongWrapper() {
            return Js.cast(this);
        }

        @JsOverlay
        @TsUnionMember
        default int asInt() {
            return Js.asInt(this);
        }
    }

    @JsProperty
    JsRangeSet getFullIndex();

    @JsProperty
    JsRangeSet getAdded();

    @JsProperty
    JsRangeSet getRemoved();

    @JsProperty
    JsRangeSet getModified();

//    @JsProperty
//    JsShiftData getShifts();

    @JsProperty
    JsArray<Column> getColumns();

    @JsProperty
    JsArray<@TsTypeRef(Row.class) ? extends Row> getRows();

    @JsMethod
    default Row get(RowPositionUnion index) {
        if (index.isLongWrapper()) {
            return get((index.asLongWrapper()).getWrapped());
        }
        return get(Js.coerceToInt(index));
    }

    Row get(long index);

    Row get(int index);

    @JsMethod
    default Any getData(RowPositionUnion index, Column column) {
        if (index.isLongWrapper()) {
            return getData(index.asLongWrapper().getWrapped(), column);
        }
        return getData(index.asInt(), column);
    }

    Any getData(int index, Column column);

    Any getData(long index, Column column);

    @JsMethod
    default Format getFormat(RowPositionUnion index, Column column) {
        if (index.isLongWrapper()) {
            return getFormat(index.asLongWrapper().getWrapped(), column);
        }
        return getFormat(index.asInt(), column);
    }

    Format getFormat(int index, Column column);

    Format getFormat(long index, Column column);

    /**
     * The position of the first returned row, null if this data is not for a viewport.
     */
    @JsProperty
    Double getOffset();

    @TsName(namespace = "dh")
    interface Row {
        @JsProperty
        LongWrapper getIndex();

        @JsMethod
        Any get(Column column);

        @JsMethod
        Format getFormat(Column column);
    }
}

//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import com.vertispan.tsdefs.annotations.TsTypeRef;
import com.vertispan.tsdefs.annotations.TsUnion;
import com.vertispan.tsdefs.annotations.TsUnionMember;
import elemental2.core.JsArray;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Any;
import jsinterop.base.Js;

/**
 * Common interface for various ways of accessing table data and formatting for viewport or non-viewport subscriptions
 * on tables, data in trees, and snapshots.
 * <p>
 * Generally speaking, it is more efficient to access data in column-major order, rather than iterating through each Row
 * and accessing all columns that it holds. The {@link #getRows()} accessor can be useful to read row data, but may
 * incur other costs - it is likely faster to access data by columns using {@link #getData(RowPositionUnion, Column)}.
 */
/*
 * Java note: this interface contains some extra overloads that aren't available in JS. Implementations are expected to
 * implement only abstract methods, and default methods present in this interface will dispatch accordingly.
 */
@JsType(namespace = "dh")
public interface TableData {
    @JsIgnore
    int NO_ROW_FORMAT_COLUMN = -1;

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
    JsArray<Column> getColumns();

    /**
     * A lazily computed array of all rows available on the client.
     */
    @JsProperty
    JsArray<@TsTypeRef(Row.class) ? extends Row> getRows();

    /**
     * Reads a row object from the table, from which any subscribed column can be read.
     *
     * @param index the position or key to access
     * @return the row at the given location
     */
    @JsMethod
    default Row get(RowPositionUnion index) {
        if (index.isLongWrapper()) {
            return get((index.asLongWrapper()).getWrapped());
        }
        return get(Js.coerceToInt(index));
    }

    @JsIgnore
    Row get(long index);

    @JsIgnore
    Row get(int index);

    /**
     * Reads a specific cell from the table, by row key and column.
     *
     * @param index the row in the table to get data from
     * @param column the column to read
     * @return the value in the table
     */
    // TODO (deephaven-core#5927) Consider a get/fillChunk API as an efficient alternative
    @JsMethod
    default Any getData(RowPositionUnion index, Column column) {
        if (index.isLongWrapper()) {
            return getData(index.asLongWrapper().getWrapped(), column);
        }
        return getData(index.asInt(), column);
    }

    @JsIgnore
    Any getData(int index, Column column);

    @JsIgnore
    Any getData(long index, Column column);

    /**
     * The server-specified Format to use for the cell at the given position.
     * 
     * @param index the row to read
     * @param column the column to read
     * @return a Format instance with any server-specified details
     */
    @JsMethod
    default Format getFormat(RowPositionUnion index, Column column) {
        if (index.isLongWrapper()) {
            return getFormat(index.asLongWrapper().getWrapped(), column);
        }
        return getFormat(index.asInt(), column);
    }

    @JsIgnore
    Format getFormat(int index, Column column);

    @JsIgnore
    Format getFormat(long index, Column column);

    /**
     * Represents a row available in a subscription/snapshot on the client. Do not retain references to rows - they will
     * not function properly when the event isn't actively going off (or promise resolving). Instead, wait for the next
     * event, or re-request the viewport data.
     */
    @JsType(namespace = "dh")
    interface Row {
        @JsProperty
        LongWrapper getIndex();

        @JsMethod
        Any get(Column column);

        @JsMethod
        Format getFormat(Column column);
    }
}

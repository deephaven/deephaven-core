//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.subscription;

import com.vertispan.tsdefs.annotations.TsTypeRef;
import elemental2.core.JsArray;
import io.deephaven.web.client.api.TableData;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;

/**
 * Extends {@link TableData}, but only contains data in the current viewport. The only API change from TableData is that
 * ViewportData also contains the offset to this data, so that the actual row number may be determined.
 * <p>
 * For viewport subscriptions, it is not necessary to read with the key, only with the position.
 * <p>
 * Do not assume that the first row in `rows` is the first visible row, because extra rows may be provided for easier
 * scrolling without going to the server.
 */
@JsType(namespace = "dh")
public interface ViewportData extends TableData {

    /**
     * The position of the first returned row, null if this data is not for a viewport.
     */
    @JsProperty
    Double getOffset();

    @JsProperty
    @Override
    JsArray<TableData.@TsTypeRef(ViewportRow.class) Row> getRows();

    /**
     * Reads a row object from the viewport, based on its position in the table.
     */
    @Override
    @TsTypeRef(ViewportRow.class)
    default TableData.Row get(RowPositionUnion index) {
        return TableData.super.get(index);
    }

    /**
     * This object may be pooled internally or discarded and not updated. Do not retain references to it. Instead,
     * request the viewport again.
     */
    @JsType(namespace = "dh")
    interface ViewportRow extends TableData.Row {
    }
}

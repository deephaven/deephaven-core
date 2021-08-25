package io.deephaven.web.client.api.subscription;

import elemental2.core.JsArray;
import io.deephaven.web.client.api.Column;
import io.deephaven.web.client.api.Format;
import io.deephaven.web.client.api.LongWrapper;
import io.deephaven.web.client.api.TableData;
import jsinterop.annotations.JsMethod;
import jsinterop.base.Any;
import jsinterop.base.Js;

public class ViewportRow implements TableData.Row {
    protected final int offsetInSnapshot;
    private final Object[] dataColumns;
    private final JsArray<Any> rowStyleColumn;

    public ViewportRow(int offsetInSnapshot, Object[] dataColumns, Object rowStyleColumn) {
        this.offsetInSnapshot = offsetInSnapshot;
        this.dataColumns = dataColumns;
        this.rowStyleColumn = Js.uncheckedCast(rowStyleColumn);
    }

    @Override
    public LongWrapper getIndex() {
        throw new UnsupportedOperationException(
            "Viewports don't currently represent their position with an index");
    }

    @Override
    @JsMethod
    public Any get(Column column) {
        JsArray<Any> uncheckedData = Js.uncheckedCast(dataColumns[column.getIndex()]);
        if (uncheckedData == null) {
            throw new java.util.NoSuchElementException("Column " + column.getName()
                + " not found in row, was it specified in the viewport?");
        }
        return uncheckedData.getAnyAt(offsetInSnapshot);
    }

    @Override
    @JsMethod
    public Format getFormat(Column column) {
        long cellColors = 0;
        long rowColors = 0;
        String numberFormat = null;
        String formatString = null;
        if (column.getStyleColumnIndex() != null) {
            JsArray<Any> colors = Js.uncheckedCast(dataColumns[column.getStyleColumnIndex()]);
            cellColors = colors.getAnyAt(offsetInSnapshot).asLong();
        }
        if (rowStyleColumn != null) {
            rowColors = rowStyleColumn.getAnyAt(offsetInSnapshot).asLong();
        }
        if (column.getFormatColumnIndex() != null) {
            JsArray<Any> formatStrings =
                Js.uncheckedCast(dataColumns[column.getFormatColumnIndex()]);
            numberFormat = formatStrings.getAnyAt(offsetInSnapshot).asString();
        }
        if (column.getFormatStringColumnIndex() != null) {
            JsArray<Any> formatStrings =
                Js.uncheckedCast(dataColumns[column.getFormatStringColumnIndex()]);
            formatString = formatStrings.getAnyAt(offsetInSnapshot).asString();
        }
        return new Format(cellColors, rowColors, numberFormat, formatString);
    }
}

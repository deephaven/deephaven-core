/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.web.client.api.subscription;

import com.vertispan.tsdefs.annotations.TsInterface;
import com.vertispan.tsdefs.annotations.TsName;
import elemental2.core.JsArray;
import io.deephaven.web.client.api.Column;
import io.deephaven.web.client.api.Format;
import io.deephaven.web.client.api.LongWrapper;
import io.deephaven.web.client.api.TableData;
import jsinterop.annotations.JsMethod;
import jsinterop.base.Any;
import jsinterop.base.Js;

import java.util.HashMap;
import java.util.Map;

@TsInterface
@TsName(namespace = "dh")
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
        throw new UnsupportedOperationException("Viewports don't currently represent their position with an index");
    }

    @Override
    @JsMethod
    public Any get(Column column) {
        JsArray<Any> uncheckedData = Js.uncheckedCast(dataColumns[column.getIndex()]);
        if (uncheckedData == null) {
            throw new java.util.NoSuchElementException(
                    "Column " + column.getName() + " not found in row, was it specified in the viewport?");
        }
        return uncheckedData.getAtAsAny(offsetInSnapshot);
    }

    @Override
    @JsMethod
    public Format getFormat(Column column) {
        long cellColors = 0;
        long rowColors = 0;
        String numberFormat = null;
        String formatString = null;
        String formatDatabarString = null;
        Map<String, String> formatDatabar = new HashMap<>();
        if (column.getStyleColumnIndex() != null) {
            JsArray<Any> colors = Js.uncheckedCast(dataColumns[column.getStyleColumnIndex()]);
            cellColors = colors.getAtAsAny(offsetInSnapshot).asLong();
        }
        if (rowStyleColumn != null) {
            rowColors = rowStyleColumn.getAtAsAny(offsetInSnapshot).asLong();
        }
        if (column.getFormatColumnIndex() != null) {
            JsArray<Any> formatStrings = Js.uncheckedCast(dataColumns[column.getFormatColumnIndex()]);
            numberFormat = formatStrings.getAtAsAny(offsetInSnapshot).asString();
        }
        if (column.getFormatStringColumnIndex() != null) {
            JsArray<Any> formatStrings = Js.uncheckedCast(dataColumns[column.getFormatStringColumnIndex()]);
            formatString = formatStrings.getAtAsAny(offsetInSnapshot).asString();
        }
        if(column.getFormatDatabarColumnIndexRange() != null) {
            Integer[] range = column.getFormatDatabarColumnIndexRange();
            Integer start = range[0];
            Integer end = range[1];

            JsArray<Any> formatDatabarStrings = Js.uncheckedCast(dataColumns[start]);
            formatDatabarString = formatDatabarStrings.getAtAsAny(offsetInSnapshot).asString();
            formatDatabar.put("test", formatDatabarString);
//            for (int idx = start; idx <= end; idx++) {
//                JsArray<Any> formatDatabarStrings = Js.uncheckedCast(dataColumns[idx]);
//                formatDatabar.put(column.getName(), formatDatabarStrings.getAtAsAny(offsetInSnapshot).asString());
//            }
        }
        return new Format(cellColors, rowColors, numberFormat, formatString, formatDatabar);
    }

//    @JsMethod
//    public java.util.Map<String, String> getDatabar(Column column) {
//        java.util.Map<String, String> databarProps = new HashMap<>();
//
//        if (column.getFormatDatabarColumnIndexRange() != null) {
//            Integer[] range = column.getFormatDatabarColumnIndexRange();
//            int start = range[0];
//            int end = range[1];
//            int colIdx = column.getIndex();
//
//            if (colIdx >= start && colIdx <= end) {
////                databarProps.put(String.valueOf(colIdx), dataColumns[colIdx]);
//            }
//        }
//
//        return databarProps;
//    }
}

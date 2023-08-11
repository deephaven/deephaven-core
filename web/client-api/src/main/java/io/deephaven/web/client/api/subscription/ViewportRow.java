/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.web.client.api.subscription;

import com.vertispan.tsdefs.annotations.TsInterface;
import com.vertispan.tsdefs.annotations.TsName;
import elemental2.core.JsArray;
import io.deephaven.web.client.api.*;
import io.deephaven.web.client.api.barrage.DatabarFormatColumnType;
import jsinterop.annotations.JsMethod;
import jsinterop.base.Any;
import jsinterop.base.Js;
import java.util.Map;

/**
 * This object may be pooled internally or discarded and not updated. Do not retain references to it. Instead, request
 * the viewport again.
 */
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

    /**
     * the data for the given column's cell
     * 
     * @param column
     * @return Any
     */
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

    /**
     * the format object for the given columns' cell
     * 
     * @param column
     * @return {@link Format}.
     */
    @Override
    @JsMethod
    public Format getFormat(Column column) {
        long cellColors = 0;
        long rowColors = 0;
        String numberFormat = null;
        String formatString = null;
        DataBarFormat formatDataBar = null;
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
        if (column.getFormatDataBarColumnIndices() != null) {
            formatDataBar = getDataBarFormat(column);
        }
        return new Format(cellColors, rowColors, numberFormat, formatString, formatDataBar);
    }

    @Override
    @JsMethod
    public DataBarFormat getDataBarFormat(Column column) {
        Map<String, Integer> formatDatabarColumnIndices = column.getFormatDataBarColumnIndices();
        DatabarFormatBuilder formatBuilder = new DatabarFormatBuilder();

        if (!formatDatabarColumnIndices.isEmpty()) {
            formatDatabarColumnIndices.entrySet().forEach(entry -> {
                String name = entry.getKey().split("__")[1];
                int index = entry.getValue().intValue();
                JsArray<Any> val = Js.uncheckedCast(dataColumns[index]);
                DatabarFormatColumnType type = DatabarFormatColumnType.valueOf(name);
                switch (type) {
                    case MIN:
                        formatBuilder.setMin(val.getAtAsAny(offsetInSnapshot).asDouble());
                        break;
                    case MAX:
                        formatBuilder.setMax(val.getAtAsAny(offsetInSnapshot).asDouble());
                        break;
                    case VALUE:
                        formatBuilder.setValue(val.getAtAsAny(offsetInSnapshot).asDouble());
                        break;
                    case AXIS:
                        formatBuilder.setAxis(val.getAtAsAny(offsetInSnapshot).asString());
                        break;
                    case POSITIVE_COLOR:
                        if (val.getAtAsAny(offsetInSnapshot) != null) {
                            formatBuilder.setPositiveColor(val.getAtAsAny(offsetInSnapshot).asString());
                        }
                        break;
                    case NEGATIVE_COLOR:
                        if (val.getAtAsAny(offsetInSnapshot) != null) {
                            formatBuilder.setNegativeColor(val.getAtAsAny(offsetInSnapshot).asString());
                        }
                        break;
                    case VALUE_PLACEMENT:
                        formatBuilder.setValuePlacement(val.getAtAsAny(offsetInSnapshot).asString());
                        break;
                    case DIRECTION:
                        formatBuilder.setDirection(val.getAtAsAny(offsetInSnapshot).asString());
                        break;
                    case OPACITY:
                        formatBuilder.setOpacity(val.getAtAsAny(offsetInSnapshot).asDouble());
                        break;
                    case MARKER:
                        if (val.getAtAsAny(offsetInSnapshot) != null) {
                            formatBuilder.setMarker(val.getAtAsAny(offsetInSnapshot).asDouble());
                        }
                        break;
                    case MARKER_COLOR:
                        if (val.getAtAsAny(offsetInSnapshot) != null) {
                            formatBuilder.setMarkerColor(val.getAtAsAny(offsetInSnapshot).asString());
                        }
                        break;
                    default:
                        throw new RuntimeException("Invalid data bar format column type: " + type);
                }
            });
        }
        return formatBuilder.build();
    }
}

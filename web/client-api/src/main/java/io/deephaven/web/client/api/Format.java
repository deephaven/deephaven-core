//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import com.vertispan.tsdefs.annotations.TsInterface;
import com.vertispan.tsdefs.annotations.TsName;
import elemental2.core.JsNumber;
import elemental2.core.JsString;
import jsinterop.annotations.JsNullable;
import jsinterop.annotations.JsProperty;

import java.util.Objects;

/**
 * This object may be pooled internally or discarded and not updated. Do not retain references to it.
 */
@TsInterface
@TsName(namespace = "dh")
public final class Format {
    public static final Format EMPTY = new Format(0, 0, null, null);

    private final long cellColors;
    private final long rowColors;

    private final String numberFormat;
    private final String formatString;

    public Format(long cellColors, long rowColors, String numberFormat, String formatString) {
        this.cellColors = cellColors == Long.MIN_VALUE ? 0 : cellColors;
        this.rowColors = rowColors == Long.MIN_VALUE ? 0 : rowColors;
        this.numberFormat = numberFormat;
        this.formatString = formatString;
    }

    private static int getFg(long color) {
        return (int) color;
    }

    private static int getBg(long color) {
        return (int) (color >>> 32);
    }

    private static boolean isSet(int color) {
        return (color & 0x01000000) != 0;
    }

    /**
     * Color to apply to the text, in <b>#rrggbb</b> format.
     * 
     * @return String
     */
    @JsNullable
    @JsProperty
    public String getColor() {
        int color = getFg(cellColors);
        if (!isSet(color)) {
            color = getFg(rowColors);
            if (!isSet(color)) {
                return null;
            }
        }
        return color(color);
    }

    /**
     * Color to apply to the cell's background, in <b>#rrggbb</b> format.
     * 
     * @return String
     */
    @JsNullable
    @JsProperty
    public String getBackgroundColor() {
        int color = getBg(cellColors);
        if (!isSet(color)) {
            color = getBg(rowColors);
            if (!isSet(color)) {
                return null;
            }
        }
        return color(color);
    }

    private String color(int color) {
        return "#" + new JsString("00000" + new JsNumber(color & 0xffffff).toString(16)).substr(-6);
    }


    /**
     * @deprecated Prefer formatString. Number format string to apply to the value in this cell.
     */
    @Deprecated
    @JsNullable
    @JsProperty
    public String getNumberFormat() {
        return numberFormat;
    }

    /**
     * The format string to apply to the value of this cell.
     * 
     * @return String
     */
    @JsNullable
    @JsProperty
    public String getFormatString() {
        return formatString;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        Format format = (Format) o;
        return cellColors == format.cellColors && rowColors == format.rowColors
                && Objects.equals(numberFormat, format.numberFormat)
                && Objects.equals(formatString, format.formatString);
    }

    @Override
    public int hashCode() {
        return Objects.hash(cellColors, rowColors, numberFormat, formatString);
    }

    @Override
    public String toString() {
        return "Format{" +
                "cellColors=" + cellColors +
                ", rowColors=" + rowColors +
                ", numberFormat='" + numberFormat + '\'' +
                ", formatString='" + formatString + '\'' +
                '}';
    }
}

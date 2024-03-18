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

/**
 * This object may be pooled internally or discarded and not updated. Do not retain references to it.
 */
@TsInterface
@TsName(namespace = "dh")
public class Format {
    private final long cellColors;
    private final long rowColors;

    private final String numberFormat;
    private final String formatString;

    public Format(long cellColors, long rowColors, String numberFormat, String formatString) {
        this.cellColors = cellColors;
        this.rowColors = rowColors;
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

}

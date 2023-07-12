/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.web.client.api;

import com.vertispan.tsdefs.annotations.TsInterface;
import com.vertispan.tsdefs.annotations.TsName;
import elemental2.core.JsNumber;
import elemental2.core.JsString;
import jsinterop.annotations.JsNullable;
import jsinterop.annotations.JsProperty;

import java.util.Map;

@TsInterface
@TsName(namespace = "dh")
public class Format {
    private final long cellColors;
    private final long rowColors;

    private final String numberFormat;
    private final String formatString;
    private final Map<String, String> formatDatabar;

    public Format(long cellColors, long rowColors, String numberFormat, String formatString, Map<String, String> formatDatabar) {
        this.cellColors = cellColors;
        this.rowColors = rowColors;
        this.numberFormat = numberFormat;
        this.formatString = formatString;
        this.formatDatabar = formatDatabar;
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
     * @deprecated Prefer formatString.
     */
    @Deprecated
    @JsNullable
    @JsProperty
    public String getNumberFormat() {
        return numberFormat;
    }

    @JsNullable
    @JsProperty
    public String getFormatString() {
        return formatString;
    }

    @JsNullable
    @JsProperty
    public Map<String, String> getFormatDatabar() {
        return formatDatabar;
    }

}

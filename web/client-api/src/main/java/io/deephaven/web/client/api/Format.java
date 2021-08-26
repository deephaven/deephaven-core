package io.deephaven.web.client.api;

import elemental2.core.JsNumber;
import elemental2.core.JsString;
import jsinterop.annotations.JsProperty;

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
     * @deprecated Prefer {@link #getFormatString()}.
     */
    @Deprecated
    @JsProperty
    public String getNumberFormat() {
        return numberFormat;
    }

    @JsProperty
    public String getFormatString() {
        return formatString;
    }

}

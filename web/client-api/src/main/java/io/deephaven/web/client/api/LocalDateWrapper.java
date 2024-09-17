//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import com.google.gwt.i18n.client.NumberFormat;
import com.vertispan.tsdefs.annotations.TsInterface;
import com.vertispan.tsdefs.annotations.TsName;
import jsinterop.annotations.JsMethod;

/**
 * Wrap LocalDate values for use in JS. Provides text formatting for display and access to the underlying value.
 */
@TsInterface
@TsName(namespace = "dh")
public class LocalDateWrapper {
    private static final NumberFormat YEAR_FORMAT = NumberFormat.getFormat("0000");
    private static final NumberFormat MONTH_DAY_FORMAT = NumberFormat.getFormat("00");

    private final int year;
    private final int monthValue, dayOfMonth;

    public LocalDateWrapper(int year, int monthValue, int dayOfMonth) {
        this.year = year;
        this.monthValue = monthValue;
        this.dayOfMonth = dayOfMonth;
    }

    @JsMethod
    public String valueOf() {
        return toString();
    }

    @JsMethod
    public int getYear() {
        return year;
    }

    @JsMethod
    public int getMonthValue() {
        return monthValue;
    }

    @JsMethod
    public int getDayOfMonth() {
        return dayOfMonth;
    }

    @JsMethod
    @Override
    public String toString() {
        return YEAR_FORMAT.format(getYear())
                + "-" + MONTH_DAY_FORMAT.format(getMonthValue())
                + "-" + MONTH_DAY_FORMAT.format(getDayOfMonth());
    }
}

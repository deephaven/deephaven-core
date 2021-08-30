package io.deephaven.web.client.api;

import com.google.gwt.i18n.client.NumberFormat;
import io.deephaven.web.shared.data.LocalDate;
import jsinterop.annotations.JsMethod;

import javax.annotation.Nonnull;

/**
 * Wrap LocalDate values for use in JS. Provides text formatting for display and access to the underlying value.
 */
public class LocalDateWrapper {
    private final static NumberFormat YEAR_FORMAT = NumberFormat.getFormat("0000");
    private final static NumberFormat MONTH_DAY_FORMAT = NumberFormat.getFormat("00");

    private final int year;
    private final int monthValue, dayOfMonth;

    public LocalDateWrapper(@Nonnull LocalDate localDate) {
        year = localDate.getYear();
        monthValue = localDate.getMonthValue();
        dayOfMonth = localDate.getDayOfMonth();
    }

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

    @Deprecated
    public LocalDate getWrapped() {
        LocalDate localDate = new LocalDate();
        localDate.setYear(year);
        localDate.setMonthValue((byte) monthValue);
        localDate.setDayOfMonth((byte) dayOfMonth);
        return localDate;
    }

    @JsMethod
    @Override
    public String toString() {
        return YEAR_FORMAT.format(getYear())
                + "-" + MONTH_DAY_FORMAT.format(getMonthValue())
                + "-" + MONTH_DAY_FORMAT.format(getDayOfMonth());
    }
}

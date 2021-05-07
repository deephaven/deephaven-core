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

    private final LocalDate localDate;

    public LocalDateWrapper(@Nonnull LocalDate localDate) {
        this.localDate = localDate;
    }

    @JsMethod
    public String valueOf() {
        return toString();
    }

    @JsMethod
    public int getYear() {
        return localDate.getYear();
    }

    @JsMethod
    public int getMonthValue() {
        return localDate.getMonthValue();
    }

    @JsMethod
    public int getDayOfMonth() {
        return localDate.getDayOfMonth();
    }

    public LocalDate getWrapped() {
        return localDate;
    }

    @JsMethod
    @Override
    public String toString() {
        return YEAR_FORMAT.format(localDate.getYear())
                + "-" + MONTH_DAY_FORMAT.format(localDate.getMonthValue())
                + "-" + MONTH_DAY_FORMAT.format(localDate.getDayOfMonth());
    }
}

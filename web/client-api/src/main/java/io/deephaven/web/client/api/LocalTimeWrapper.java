package io.deephaven.web.client.api;

import com.google.gwt.i18n.client.NumberFormat;
import io.deephaven.web.shared.data.LocalTime;
import jsinterop.annotations.JsMethod;

import javax.annotation.Nonnull;

/**
 * Wrap LocalTime values for use in JS. Provides text formatting for display and access to the underlying value.
 */
public class LocalTimeWrapper {
    private final static NumberFormat TWO_DIGIT_FORMAT = NumberFormat.getFormat("00");
    private final static NumberFormat NANOS_FORMAT = NumberFormat.getFormat("000000000");

    private final LocalTime localTime;

    public LocalTimeWrapper(@Nonnull LocalTime localTime) {
        this.localTime = localTime;
    }

    @JsMethod
    public String valueOf() {
        return toString();
    }

    @JsMethod
    public int getHour() {
        return localTime.getHour();
    }

    @JsMethod
    public int getMinute() {
        return localTime.getMinute();
    }

    @JsMethod
    public int getSecond() {
        return localTime.getSecond();
    }

    @JsMethod
    public int getNano() {
        return localTime.getNano();
    }

    public LocalTime getWrapped() {
        return localTime;
    }

    @JsMethod
    @Override
    public String toString() {
        return TWO_DIGIT_FORMAT.format(localTime.getHour())
                + ":" + TWO_DIGIT_FORMAT.format(localTime.getMinute())
                + ":" + TWO_DIGIT_FORMAT.format(localTime.getSecond())
                + "." + NANOS_FORMAT.format(localTime.getNano());
    }
}

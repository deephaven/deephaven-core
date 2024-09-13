//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import com.google.gwt.i18n.client.NumberFormat;
import com.vertispan.tsdefs.annotations.TsInterface;
import com.vertispan.tsdefs.annotations.TsName;
import io.deephaven.util.QueryConstants;
import jsinterop.annotations.JsMethod;

import java.util.function.IntFunction;
import java.util.function.LongFunction;

/**
 * Wrap LocalTime values for use in JS. Provides text formatting for display and access to the underlying value.
 */
@TsInterface
@TsName(namespace = "dh")
public class LocalTimeWrapper {
    private static final NumberFormat TWO_DIGIT_FORMAT = NumberFormat.getFormat("00");
    private static final NumberFormat NANOS_FORMAT = NumberFormat.getFormat("000000000");

    private final int hour;
    private final int minute;
    private final int second;
    private final int nano;

    public static IntFunction<LocalTimeWrapper> intCreator(int unitPerMicro) {
        int nanoPerUnit = 1_000_000_000 / unitPerMicro;
        return val -> {
            if (val == QueryConstants.NULL_INT) {
                return null;
            }
            int nano = (val % unitPerMicro) * nanoPerUnit;
            int secVal = val / unitPerMicro;
            int second = (secVal % 60);
            secVal /= 60;
            int minute = (secVal % 60);
            int hour = (secVal / 60);
            return new LocalTimeWrapper(hour, minute, second, nano);
        };
    }

    public static LongFunction<LocalTimeWrapper> longCreator(int unitPerMicro) {
        int nanoPerUnit = 1_000_000_000 / unitPerMicro;
        return val -> {
            if (val == QueryConstants.NULL_LONG) {
                return null;
            }
            int nano = (int) (val % unitPerMicro) * nanoPerUnit;
            int secVal = (int) (val / unitPerMicro);
            byte second = (byte) (secVal % 60);
            secVal /= 60;
            byte minute = (byte) (secVal % 60);
            byte hour = (byte) (secVal / 60);
            return new LocalTimeWrapper(hour, minute, second, nano);
        };
    }

    public LocalTimeWrapper(int hour, int minute, int second, int nano) {
        this.hour = hour;
        this.minute = minute;
        this.second = second;
        this.nano = nano;
    }

    @JsMethod
    public String valueOf() {
        return toString();
    }

    @JsMethod
    public int getHour() {
        return hour;
    }

    @JsMethod
    public int getMinute() {
        return minute;
    }

    @JsMethod
    public int getSecond() {
        return second;
    }

    @JsMethod
    public int getNano() {
        return nano;
    }

    @JsMethod
    @Override
    public String toString() {
        return TWO_DIGIT_FORMAT.format(hour)
                + ":" + TWO_DIGIT_FORMAT.format(minute)
                + ":" + TWO_DIGIT_FORMAT.format(second)
                + "." + NANOS_FORMAT.format(nano);
    }
}

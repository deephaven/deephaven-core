//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import elemental2.core.JsDate;
import io.deephaven.util.QueryConstants;
import io.deephaven.web.client.api.i18n.JsDateTimeFormat;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsType;

/**
 * Wraps a timestamp value stored as nanoseconds since the epoch for use in JS.
 *
 * <p>
 * This type extends {@link LongWrapper} and inherits the following conversions:
 * <ul>
 * <li>{@link LongWrapper#asNumber()} for a numeric representation</li>
 * <li>{@link LongWrapper#toString()} and {@link LongWrapper#valueOf()} for string representations</li>
 * <li>{@link LongWrapper#ofString(String)} for parsing</li>
 * </ul>
 */
@JsType(namespace = "dh")
public class DateWrapper extends LongWrapper {
    @JsIgnore
    public DateWrapper(long valueInNanos) {
        super(valueInNanos);
    }

    @JsIgnore
    public static DateWrapper of(long dateInNanos) {
        if (dateInNanos == QueryConstants.NULL_LONG) {
            return null;
        }
        return new DateWrapper(dateInNanos);
    }

    /**
     * Creates a wrapper from a JavaScript {@link JsDate}.
     *
     * @param date The JavaScript date value.
     * @return A wrapper containing the corresponding timestamp in nanoseconds.
     */
    public static DateWrapper ofJsDate(JsDate date) {
        long valueInNanos = JsDateTimeFormat.NANOS_PER_MILLI * (long) date.getTime();
        return new DateWrapper(valueInNanos);
    }

    /**
     * Returns the wrapped timestamp as a JavaScript {@link JsDate}.
     *
     * @return A {@link JsDate} representing the wrapped value.
     */
    public JsDate asDate() {
        JsDate date = new JsDate();
        date.setTime(getWrapped() / JsDateTimeFormat.NANOS_PER_MILLI);
        return date;
    }
}

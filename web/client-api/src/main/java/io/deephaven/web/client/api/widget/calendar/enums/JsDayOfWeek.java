//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.widget.calendar.enums;

import com.vertispan.tsdefs.annotations.TsTypeDef;
import elemental2.core.JsObject;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsType;

/**
 * String enum values for {@code dh.calendar.DayOfWeek}.
 *
 * Instances are represented as strings like {@link #MONDAY}. Use {@link #values()} to get the list of all supported
 * values.
 */
@JsType(name = "DayOfWeek", namespace = "dh.calendar")
@SuppressWarnings("unusable-by-js")
@TsTypeDef(tsType = "string", name = "DayOfWeekType")
public class JsDayOfWeek {
    public static final String SUNDAY = "SUNDAY";
    public static final String MONDAY = "MONDAY";
    public static final String TUESDAY = "TUESDAY";
    public static final String WEDNESDAY = "WEDNESDAY";
    public static final String THURSDAY = "THURSDAY";
    public static final String FRIDAY = "FRIDAY";
    public static final String SATURDAY = "SATURDAY";

    private static final String[] VALUES = JsObject.freeze(new String[] {
            SUNDAY,
            MONDAY,
            TUESDAY,
            WEDNESDAY,
            THURSDAY,
            FRIDAY,
            SATURDAY
    });

    /**
     * Gets the full list of day-of-week enum values.
     *
     * @return A frozen array of all supported {@code dh.calendar.DayOfWeek} values.
     */
    @JsMethod
    public static String[] values() {
        return VALUES;
    }
}

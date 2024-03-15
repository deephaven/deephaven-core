//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.widget.calendar.enums;

import com.vertispan.tsdefs.annotations.TsTypeDef;
import elemental2.core.JsObject;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsType;

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

    @JsMethod
    public static String[] values() {
        return VALUES;
    }
}

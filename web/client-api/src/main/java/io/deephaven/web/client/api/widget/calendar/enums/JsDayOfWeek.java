package io.deephaven.web.client.api.widget.calendar.enums;

import io.deephaven.web.shared.data.BusinessCalendarDescriptor;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsType;

@JsType(name = "DayOfWeek", namespace="dh.calendar")
@SuppressWarnings("unusable-by-js")
public class JsDayOfWeek {
    public static final BusinessCalendarDescriptor.DayOfWeek SUNDAY = BusinessCalendarDescriptor.DayOfWeek.SUNDAY;
    public static final BusinessCalendarDescriptor.DayOfWeek MONDAY = BusinessCalendarDescriptor.DayOfWeek.MONDAY;
    public static final BusinessCalendarDescriptor.DayOfWeek TUESDAY = BusinessCalendarDescriptor.DayOfWeek.TUESDAY;
    public static final BusinessCalendarDescriptor.DayOfWeek WEDNESDAY = BusinessCalendarDescriptor.DayOfWeek.WEDNESDAY;
    public static final BusinessCalendarDescriptor.DayOfWeek THURSDAY = BusinessCalendarDescriptor.DayOfWeek.THURSDAY;
    public static final BusinessCalendarDescriptor.DayOfWeek FRIDAY = BusinessCalendarDescriptor.DayOfWeek.FRIDAY;
    public static final BusinessCalendarDescriptor.DayOfWeek SATURDAY = BusinessCalendarDescriptor.DayOfWeek.SATURDAY;

    @JsMethod
    public static BusinessCalendarDescriptor.DayOfWeek[] values() {
        return BusinessCalendarDescriptor.DayOfWeek.values();
    }
}

package io.deephaven.web.client.api.widget.calendar;

import elemental2.core.JsObject;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.figuredescriptor.BusinessCalendarDescriptor;
import io.deephaven.web.client.api.i18n.JsTimeZone;
import io.deephaven.web.client.api.widget.calendar.enums.JsDayOfWeek;
import jsinterop.annotations.JsProperty;

public class JsBusinessCalendar {
    private final BusinessCalendarDescriptor businessCalendarDescriptor;
    private final JsTimeZone timeZone;
    private final JsBusinessPeriod[] businessPeriods;
    private final JsHoliday[] holidays;

    public JsBusinessCalendar(BusinessCalendarDescriptor businessCalendarDescriptor) {
        this.businessCalendarDescriptor = businessCalendarDescriptor;
        JsObject.freeze(this.businessCalendarDescriptor);
        timeZone = JsTimeZone.getTimeZone(businessCalendarDescriptor.getTimeZone());
        businessPeriods = businessCalendarDescriptor.getBusinessPeriodsList().map((p0, p1, p2) -> new JsBusinessPeriod(p0));
        JsObject.freeze(businessPeriods);
        holidays = businessCalendarDescriptor.getHolidaysList().map((p0, p1, p2) -> new JsHoliday(p0));
        JsObject.freeze(holidays);
    }

    @JsProperty
    public String getName() {
        return businessCalendarDescriptor.getName();
    }

    @JsProperty
    public JsTimeZone getTimeZone() {
        return timeZone;
    }

    @JsProperty
    public String[] getBusinessDays() {
        return businessCalendarDescriptor.getBusinessDaysList().map((p0, p1, p2) -> JsDayOfWeek.values()[(int)(double)p0]);
    }

    @JsProperty
    public JsBusinessPeriod[] getBusinessPeriods() {
        return businessPeriods;
    }

    @JsProperty
    public JsHoliday[] getHolidays() {
        return holidays;
    }
}

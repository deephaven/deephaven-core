//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.widget.calendar;

import com.vertispan.tsdefs.annotations.TsInterface;
import com.vertispan.tsdefs.annotations.TsName;
import elemental2.core.JsArray;
import elemental2.core.JsObject;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.console_pb.figuredescriptor.BusinessCalendarDescriptor;
import io.deephaven.web.client.api.i18n.JsTimeZone;
import io.deephaven.web.client.api.widget.calendar.enums.JsDayOfWeek;
import jsinterop.annotations.JsProperty;

/**
 * Defines a calendar with business hours and holidays.
 */
@TsInterface
@TsName(namespace = "dh.calendar", name = "BusinessCalendar")
public class JsBusinessCalendar {
    private final BusinessCalendarDescriptor businessCalendarDescriptor;
    private final JsTimeZone timeZone;
    private final JsArray<JsBusinessPeriod> businessPeriods;
    private final JsArray<JsHoliday> holidays;

    public JsBusinessCalendar(BusinessCalendarDescriptor businessCalendarDescriptor) {
        this.businessCalendarDescriptor = businessCalendarDescriptor;
        JsObject.freeze(this.businessCalendarDescriptor);
        timeZone = JsTimeZone.getTimeZone(businessCalendarDescriptor.getTimeZone());
        businessPeriods =
                businessCalendarDescriptor.getBusinessPeriodsList().map((p0, p1) -> new JsBusinessPeriod(p0));
        JsObject.freeze(businessPeriods);
        holidays = businessCalendarDescriptor.getHolidaysList().map((p0, p1) -> new JsHoliday(p0));
        JsObject.freeze(holidays);
    }

    /**
     * The name of the calendar.
     * 
     * @return String
     */
    @JsProperty
    public String getName() {
        return businessCalendarDescriptor.getName();
    }

    /**
     * The time zone of this calendar.
     * 
     * @return dh.i18n.TimeZone
     */
    @JsProperty
    public JsTimeZone getTimeZone() {
        return timeZone;
    }

    /**
     * The days of the week that are business days.
     * 
     * @return String array
     */
    @JsProperty
    public JsArray<String> getBusinessDays() {
        return businessCalendarDescriptor.getBusinessDaysList()
                .map((p0, p1) -> JsDayOfWeek.values()[(int) (double) p0]);
    }

    /**
     * The business periods that are open on a business day.
     * 
     * @return dh.calendar.BusinessPeriod
     */
    @JsProperty
    public JsArray<JsBusinessPeriod> getBusinessPeriods() {
        return businessPeriods;
    }

    /**
     * All holidays defined for this calendar.
     * 
     * @return dh.calendar.Holiday
     */
    @JsProperty
    public JsArray<JsHoliday> getHolidays() {
        return holidays;
    }
}

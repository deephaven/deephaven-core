/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.web.client.api.widget.calendar;

import com.vertispan.tsdefs.annotations.TsInterface;
import com.vertispan.tsdefs.annotations.TsName;
import elemental2.core.JsArray;
import elemental2.core.JsObject;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.figuredescriptor.BusinessCalendarDescriptor;
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
                businessCalendarDescriptor.getBusinessPeriodsList().map((p0, p1, p2) -> new JsBusinessPeriod(p0));
        JsObject.freeze(businessPeriods);
        holidays = businessCalendarDescriptor.getHolidaysList().map((p0, p1, p2) -> new JsHoliday(p0));
        JsObject.freeze(holidays);
    }

    /**
     * @return Read-only. The name of the calendar.
     */
    @JsProperty
    public String getName() {
        return businessCalendarDescriptor.getName();
    }

    /**
     * @return Read-only. The time zone of this calendar.
     */
    @JsProperty
    public JsTimeZone getTimeZone() {
        return timeZone;
    }

    /**
     * @return Read-only. The days of the week that are business days.
     */
    @JsProperty
    public JsArray<String> getBusinessDays() {
        return businessCalendarDescriptor.getBusinessDaysList()
                .map((p0, p1, p2) -> JsDayOfWeek.values()[(int) (double) p0]);
    }

    /**
     * @return Read-only. The business periods that are open on a business day.
     */
    @JsProperty
    public JsArray<JsBusinessPeriod> getBusinessPeriods() {
        return businessPeriods;
    }

    /**
     * @return Read-only. All holidays defined for this calendar.
     */
    @JsProperty
    public JsArray<JsHoliday> getHolidays() {
        return holidays;
    }
}

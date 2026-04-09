//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.widget.calendar;

import com.vertispan.tsdefs.annotations.TsInterface;
import com.vertispan.tsdefs.annotations.TsName;
import elemental2.core.JsArray;
import elemental2.core.JsObject;
import io.deephaven.proto.backplane.script.grpc.FigureDescriptor;
import io.deephaven.web.client.api.i18n.JsTimeZone;
import io.deephaven.web.client.api.widget.calendar.enums.JsDayOfWeek;
import io.deephaven.web.client.fu.JsCollectors;
import jsinterop.annotations.JsProperty;
import jsinterop.base.Js;

/**
 * Defines a calendar with business hours and holidays.
 */
@TsInterface
@TsName(namespace = "dh.calendar", name = "BusinessCalendar")
public class JsBusinessCalendar {
    private final FigureDescriptor.BusinessCalendarDescriptor businessCalendarDescriptor;
    private final JsTimeZone timeZone;
    private final JsArray<JsBusinessPeriod> businessPeriods;
    private final JsArray<JsHoliday> holidays;

    public JsBusinessCalendar(FigureDescriptor.BusinessCalendarDescriptor businessCalendarDescriptor) {
        this.businessCalendarDescriptor = businessCalendarDescriptor;
        JsObject.freeze(this.businessCalendarDescriptor);
        timeZone = JsTimeZone.getTimeZone(businessCalendarDescriptor.getTimeZone());
        businessPeriods = businessCalendarDescriptor.getBusinessPeriodsList().stream()
                .map(JsBusinessPeriod::new)
                .collect(JsCollectors.toFrozenJsArray());
        holidays = businessCalendarDescriptor.getHolidaysList().stream()
                .map(JsHoliday::new)
                .collect(JsCollectors.toFrozenJsArray());
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
        return Js.uncheckedCast(businessCalendarDescriptor.getBusinessDaysList()
                .stream()
                .map(FigureDescriptor.BusinessCalendarDescriptor.DayOfWeek::name)
                .toArray());
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

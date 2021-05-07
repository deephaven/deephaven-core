package io.deephaven.web.client.api.widget.calendar;

import elemental2.core.JsObject;
import io.deephaven.web.client.api.i18n.JsTimeZone;
import io.deephaven.web.shared.data.BusinessCalendarDescriptor;
import io.deephaven.web.shared.data.Holiday;
import jsinterop.annotations.JsProperty;

import java.util.Arrays;

public class JsBusinessCalendar {
    private final BusinessCalendarDescriptor businessCalendarDescriptor;
    private final JsTimeZone timeZone;
    private final JsBusinessPeriod[] businessPeriods;
    private final JsHoliday[] holidays;

    public JsBusinessCalendar(BusinessCalendarDescriptor businessCalendarDescriptor) {
        this.businessCalendarDescriptor = businessCalendarDescriptor;
        JsObject.freeze(this.businessCalendarDescriptor);
        timeZone = JsTimeZone.getTimeZone(businessCalendarDescriptor.getTimeZone());
        businessPeriods = Arrays.stream(businessCalendarDescriptor.getBusinessPeriods()).map(JsBusinessPeriod::new).toArray(JsBusinessPeriod[]::new);
        JsObject.freeze(businessPeriods);
        final Holiday[] calendarHolidays = businessCalendarDescriptor.getHolidays();
        holidays = Arrays.stream(businessCalendarDescriptor.getHolidays()).map(JsHoliday::new).toArray(JsHoliday[]::new);
        for (int i = 0; i < calendarHolidays.length; i++) {
            holidays[i] = new JsHoliday(calendarHolidays[i]);
        }
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
    public BusinessCalendarDescriptor.DayOfWeek[] getBusinessDays() {
        return businessCalendarDescriptor.getBusinessDays();
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

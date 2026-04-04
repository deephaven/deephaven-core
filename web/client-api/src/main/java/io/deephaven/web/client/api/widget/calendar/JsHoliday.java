//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.widget.calendar;

import com.vertispan.tsdefs.annotations.TsInterface;
import com.vertispan.tsdefs.annotations.TsName;
import elemental2.core.JsArray;
import elemental2.core.JsObject;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.console_pb.figuredescriptor.businesscalendardescriptor.Holiday;
import io.deephaven.web.client.api.LocalDateWrapper;
import jsinterop.annotations.JsProperty;

/**
 * A holiday in a {@link JsBusinessCalendar dh.calendar.BusinessCalendar}.
 *
 * A holiday includes a {@link #getDate() date} and may define one or more {@link JsBusinessPeriod business periods}.
 */
@TsInterface
@TsName(namespace = "dh.calendar", name = "Holiday")
public class JsHoliday {
    private final LocalDateWrapper date;
    private final JsArray<JsBusinessPeriod> businessPeriods;

    public JsHoliday(Holiday holiday) {
        date = new LocalDateWrapper(holiday.getDate().getYear(), holiday.getDate().getMonth(),
                holiday.getDate().getDay());
        businessPeriods = holiday.getBusinessPeriodsList().map((p0, p1) -> new JsBusinessPeriod(p0));
        JsObject.freeze(businessPeriods);
    }

    /**
     * Get the date of the holiday.
     * 
     * @return {@link LocalDateWrapper}
     */
    @JsProperty
    public LocalDateWrapper getDate() {
        return date;
    }

    /**
     * Get an array of all business periods that are open on the holiday.
     * 
     * @return {@link JsBusinessPeriod dh.calendar.BusinessPeriod} array
     */
    @JsProperty
    public JsArray<JsBusinessPeriod> getBusinessPeriods() {
        return businessPeriods;
    }
}

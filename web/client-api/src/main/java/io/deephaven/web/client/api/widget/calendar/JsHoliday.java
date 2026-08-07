//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.widget.calendar;

import com.vertispan.tsdefs.annotations.TsInterface;
import com.vertispan.tsdefs.annotations.TsName;
import elemental2.core.JsArray;
import elemental2.core.JsObject;
import io.deephaven.proto.backplane.script.grpc.FigureDescriptor;
import io.deephaven.web.client.api.LocalDateWrapper;
import io.deephaven.web.client.fu.JsCollectors;
import jsinterop.annotations.JsProperty;
import jsinterop.base.Js;

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

    public JsHoliday(FigureDescriptor.BusinessCalendarDescriptor.Holiday holiday) {
        date = new LocalDateWrapper(holiday.getDate().getYear(), holiday.getDate().getMonth(),
                holiday.getDate().getDay());
        businessPeriods = holiday.getBusinessPeriodsList().stream()
                .map((p0) -> new JsBusinessPeriod(p0))
                .collect(JsCollectors.toFrozenJsArray());
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

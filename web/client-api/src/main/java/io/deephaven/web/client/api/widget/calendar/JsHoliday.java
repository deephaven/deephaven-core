//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.widget.calendar;

import com.vertispan.tsdefs.annotations.TsInterface;
import com.vertispan.tsdefs.annotations.TsName;
import elemental2.core.JsArray;
import elemental2.core.JsObject;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.console_pb.figuredescriptor.businesscalendardescriptor.Holiday;
import io.deephaven.web.client.api.LocalDateWrapper;
import jsinterop.annotations.JsProperty;

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
     * The date of the Holiday.
     * 
     * @return {@link LocalDateWrapper}
     */
    @JsProperty
    public LocalDateWrapper getDate() {
        return date;
    }

    /**
     * The business periods that are open on the holiday.
     * 
     * @return dh.calendar.BusinessPeriod
     */
    @JsProperty
    public JsArray<JsBusinessPeriod> getBusinessPeriods() {
        return businessPeriods;
    }
}

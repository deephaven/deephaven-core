/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.web.client.api.widget.calendar;

import elemental2.core.JsArray;
import elemental2.core.JsObject;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.figuredescriptor.businesscalendardescriptor.Holiday;
import io.deephaven.web.client.api.LocalDateWrapper;
import jsinterop.annotations.JsProperty;

public class JsHoliday {
    private final LocalDateWrapper date;
    private final JsArray<JsBusinessPeriod> businessPeriods;

    public JsHoliday(Holiday holiday) {
        date = new LocalDateWrapper(holiday.getDate().getYear(), holiday.getDate().getMonth(),
                holiday.getDate().getDay());
        businessPeriods = holiday.getBusinessPeriodsList().map((p0, p1, p2) -> new JsBusinessPeriod(p0));
        JsObject.freeze(businessPeriods);
    }

    @JsProperty
    public LocalDateWrapper getDate() {
        return date;
    }

    @JsProperty
    public JsArray<JsBusinessPeriod> getBusinessPeriods() {
        return businessPeriods;
    }
}

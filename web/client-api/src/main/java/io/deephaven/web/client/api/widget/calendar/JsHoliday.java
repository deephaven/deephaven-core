package io.deephaven.web.client.api.widget.calendar;

import elemental2.core.JsObject;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.figuredescriptor.businesscalendardescriptor.Holiday;
import io.deephaven.web.client.api.LocalDateWrapper;
import jsinterop.annotations.JsProperty;

public class JsHoliday {
    private final LocalDateWrapper date;
    private final JsBusinessPeriod[] businessPeriods;

    public JsHoliday(Holiday holiday) {
        date = new LocalDateWrapper(holiday.getDate().getYear(), holiday.getDate().getMonth(),
            holiday.getDate().getDay());
        businessPeriods =
            holiday.getBusinessPeriodsList().map((p0, p1, p2) -> new JsBusinessPeriod(p0));
        JsObject.freeze(businessPeriods);
    }

    @JsProperty
    public LocalDateWrapper getDate() {
        return date;
    }

    @JsProperty
    public JsBusinessPeriod[] getBusinessPeriods() {
        return businessPeriods;
    }
}

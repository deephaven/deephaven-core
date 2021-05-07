package io.deephaven.web.client.api.widget.calendar;

import elemental2.core.JsObject;
import io.deephaven.web.client.api.LocalDateWrapper;
import io.deephaven.web.shared.data.Holiday;
import jsinterop.annotations.JsProperty;

import java.util.Arrays;

public class JsHoliday {
    private final LocalDateWrapper date;
    private final JsBusinessPeriod[] businessPeriods;

    public JsHoliday(Holiday holiday) {
        date = new LocalDateWrapper(holiday.getDate());
        businessPeriods = Arrays.stream(holiday.getBusinessPeriods()).map(JsBusinessPeriod::new).toArray(JsBusinessPeriod[]::new);
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

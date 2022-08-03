/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.web.client.api;

import elemental2.core.JsDate;
import io.deephaven.web.client.api.i18n.JsDateTimeFormat;
import jsinterop.annotations.JsMethod;

public class DateWrapper extends LongWrapper {
    public DateWrapper(long valueInNanos) {
        super(valueInNanos);
    }

    public static DateWrapper of(long dateInNanos) {
        return new DateWrapper(dateInNanos);
    }

    @JsMethod(namespace = "dh.DateWrapper")
    public static DateWrapper ofJsDate(JsDate date) {
        long valueInNanos = JsDateTimeFormat.NANOS_PER_MILLI * (long) date.getTime();
        return new DateWrapper(valueInNanos);
    }

    @JsMethod
    public JsDate asDate() {
        JsDate date = new JsDate();
        date.setTime(getWrapped() / JsDateTimeFormat.NANOS_PER_MILLI);
        return date;
    }
}

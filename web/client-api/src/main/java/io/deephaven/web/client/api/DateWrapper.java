//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import elemental2.core.JsDate;
import io.deephaven.util.QueryConstants;
import io.deephaven.web.client.api.i18n.JsDateTimeFormat;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsType;

@JsType(namespace = "dh")
public class DateWrapper extends LongWrapper {
    @JsIgnore
    public DateWrapper(long valueInNanos) {
        super(valueInNanos);
    }

    @JsIgnore
    public static DateWrapper of(long dateInNanos) {
        if (dateInNanos == QueryConstants.NULL_LONG) {
            return null;
        }
        return new DateWrapper(dateInNanos);
    }

    public static DateWrapper ofJsDate(JsDate date) {
        long valueInNanos = JsDateTimeFormat.NANOS_PER_MILLI * (long) date.getTime();
        return new DateWrapper(valueInNanos);
    }

    public JsDate asDate() {
        JsDate date = new JsDate();
        date.setTime(getWrapped() / JsDateTimeFormat.NANOS_PER_MILLI);
        return date;
    }
}

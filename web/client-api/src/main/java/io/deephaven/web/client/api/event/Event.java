//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.event;

import com.vertispan.tsdefs.annotations.TsInterface;
import com.vertispan.tsdefs.annotations.TsName;
import jsinterop.annotations.JsProperty;

/**
 * Similar to the browser {@code CustomEvent} type, this class holds only the type of the event, and optionally some
 * details about the event.
 *
 * @param <T> the type of the event detail
 */
@TsInterface
@TsName(namespace = "dh")
public class Event<T> {
    private final String type;
    private final T detail;

    public Event(String type, T detail) {
        this.type = type;
        this.detail = detail;
    }

    @JsProperty
    public String getType() {
        return type;
    }

    @JsProperty
    public T getDetail() {
        return detail;
    }
}

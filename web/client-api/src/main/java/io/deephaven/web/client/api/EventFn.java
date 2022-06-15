/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.web.client.api;

import elemental2.dom.Event;
import jsinterop.annotations.JsFunction;

/**
 */
@JsFunction
public interface EventFn {
    void onEvent(Event e);
}

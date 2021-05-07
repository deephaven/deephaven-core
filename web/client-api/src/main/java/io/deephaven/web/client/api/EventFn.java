package io.deephaven.web.client.api;

import elemental2.dom.Event;
import jsinterop.annotations.JsFunction;

/**
 */
@JsFunction
public interface EventFn {
    void onEvent(Event e);
}

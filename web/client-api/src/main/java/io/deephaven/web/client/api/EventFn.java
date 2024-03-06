//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import elemental2.dom.CustomEvent;
import elemental2.dom.Event;
import jsinterop.annotations.JsFunction;

/**
 */
@JsFunction
public interface EventFn<T> {
    void onEvent(CustomEvent<T> e);
}

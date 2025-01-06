//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.event;

import jsinterop.annotations.JsFunction;

/**
 * Event handler function. In JS/TS, this will only show up as a function type when used, but these docs will never be
 * visible.
 */
@JsFunction
public interface EventFn<T> {
    void onEvent(Event<T> e);
}

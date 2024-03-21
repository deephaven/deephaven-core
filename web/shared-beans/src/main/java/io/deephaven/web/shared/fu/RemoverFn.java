//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.shared.fu;

import jsinterop.annotations.JsFunction;

/**
 * Like Gwt's HandlerRegistration, but future-friendly (JsFunction), dependency-free, and easier to type!
 */
@JsFunction
@FunctionalInterface
public interface RemoverFn {
    void remove();
}

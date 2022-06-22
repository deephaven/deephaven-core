/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.web.shared.fu;

import jsinterop.annotations.JsFunction;

/**
 * A js-friendly BiConsumer FunctionalInterface
 */
@JsFunction
@FunctionalInterface
public interface JsBiConsumer<T1, T2> {

    @SuppressWarnings("unusable-by-js")
    void apply(T1 one, T2 two);
}

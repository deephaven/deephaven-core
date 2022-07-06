/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.web.shared.fu;

import jsinterop.annotations.JsFunction;
import jsinterop.annotations.JsOverlay;

/**
 * A js-friendly Consumer FunctionalInterface
 */
@JsFunction
@FunctionalInterface
public interface JsConsumer<T> {
    @JsOverlay
    static <T> JsConsumer<T> doNothing() {
        // noinspection unchecked
        return (JsConsumer<T>) ConsumerHelper.DO_NOTHING;
    }


    @SuppressWarnings("unusable-by-js")
    void apply(T value);
}


class ConsumerHelper {
    static JsConsumer<?> DO_NOTHING = ignore -> {
    };
}

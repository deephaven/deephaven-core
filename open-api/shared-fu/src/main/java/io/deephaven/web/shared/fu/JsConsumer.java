package io.deephaven.web.shared.fu;

import jsinterop.annotations.JsFunction;

/**
 * A js-friendly Consumer FunctionalInterface
 */
@JsFunction
@FunctionalInterface
public interface JsConsumer<T> {

    @SuppressWarnings("unusable-by-js")
    void apply(T value);
}

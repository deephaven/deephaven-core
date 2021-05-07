package io.deephaven.web.shared.fu;

import jsinterop.annotations.JsFunction;

/**
 * A js-friendly Supplier / Provider FunctionalInterface
 */
@JsFunction
@FunctionalInterface
public interface JsProvider<T> {

    @SuppressWarnings("unusable-by-js")
    T valueOf();
}

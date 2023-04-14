package io.deephaven.web.client.api;

import jsinterop.annotations.JsFunction;

@JsFunction
@FunctionalInterface
public interface JsPredicate<I> {

    @SuppressWarnings("unusable-by-js")
    boolean test(I input);
}
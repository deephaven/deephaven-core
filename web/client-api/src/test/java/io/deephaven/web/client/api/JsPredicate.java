//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import jsinterop.annotations.JsFunction;

@JsFunction
@FunctionalInterface
public interface JsPredicate<I> {

    @SuppressWarnings("unusable-by-js")
    boolean test(I input);
}

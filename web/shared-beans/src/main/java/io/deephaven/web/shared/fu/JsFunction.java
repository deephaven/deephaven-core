//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.shared.fu;

/**
 * A js-friendly Function FunctionalInterface
 */
@jsinterop.annotations.JsFunction
@FunctionalInterface
public interface JsFunction<I, O> {

    @SuppressWarnings("unusable-by-js")
    O apply(I input);
}

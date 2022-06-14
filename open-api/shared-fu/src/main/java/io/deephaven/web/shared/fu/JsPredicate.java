/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.web.shared.fu;

/**
 * A js-friendly Predicate FunctionalInterface
 */
@jsinterop.annotations.JsFunction
@FunctionalInterface
public interface JsPredicate<I> {

    @SuppressWarnings("unusable-by-js")
    boolean test(I input);
}

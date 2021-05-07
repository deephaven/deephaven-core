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

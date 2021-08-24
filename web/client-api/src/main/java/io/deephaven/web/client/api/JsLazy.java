package io.deephaven.web.client.api;

import java.util.function.BiFunction;
import java.util.function.Function;

/**
 */
public class JsLazy<T> {
    @FunctionalInterface
    public interface LazyProvider<T> {
        T valueOf();
    }

    private static final LazyProvider NO_REENTRY = () -> {
        throw new IllegalStateException("no reentry");
    };

    private static final class ImmutableProvider<T> implements LazyProvider<T> {

        private final T value;

        private ImmutableProvider(T value) {
            this.value = value;
        }

        @Override
        public T valueOf() {
            return value;
        }
    }

    private volatile LazyProvider<T> supplier;

    public JsLazy(LazyProvider<T> val) {
        supplier = () -> {
            supplier = NO_REENTRY;
            T resolved = val.valueOf();
            supplier = new ImmutableProvider<>(resolved);
            return resolved;
        };
    }

    public synchronized T get() {
        return supplier.valueOf();
    }

    public synchronized boolean isAvailable() {
        return supplier instanceof ImmutableProvider;
    }

    public static <A1, A2, T> JsLazy<T> of(BiFunction<A1, A2, T> factory, A1 arg1, A2 arg2) {
        return new JsLazy<>(() -> factory.apply(arg1, arg2));
    }

    public static <A1, T> JsLazy<T> of(Function<A1, T> factory, A1 arg1) {
        return new JsLazy<>(() -> factory.apply(arg1));
    }

    public static <T> JsLazy<T> of(LazyProvider<T> factory) {
        return new JsLazy<>(factory);
    }

    public static <T> JsLazy<T> of(T value) {
        return new JsLazy<>(new ImmutableProvider<>(value));
    }
}

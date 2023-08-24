/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.functions;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

class BooleanFunctions {

    static <T> BooleanFunction<T> primitive() {
        // noinspection unchecked
        return (BooleanFunction<T>) PrimitiveBoolean.INSTANCE;
    }

    public static <T> BooleanFunction<T> ofTrue() {
        // noinspection unchecked
        return (BooleanFunction<T>) OfTrue.INSTANCE;
    }

    public static <T> BooleanFunction<T> ofFalse() {
        // noinspection unchecked
        return (BooleanFunction<T>) OfFalse.INSTANCE;
    }

    static <T, R> BooleanFunction<T> map(Function<T, R> f, BooleanFunction<R> g) {
        return new BooleanMap<>(f, g);
    }

    static <T> BooleanFunction<T> not(BooleanFunction<T> f) {
        return f instanceof BooleanNot ? ((BooleanNot<T>) f).function() : new BooleanNot<>(f);
    }

    static <T> BooleanFunction<T> or(Collection<BooleanFunction<T>> functions) {
        if (functions.isEmpty()) {
            return ofFalse();
        }
        if (functions.size() == 1) {
            return functions.iterator().next();
        }
        return new BooleanOr<>(functions);
    }

    static <T> BooleanFunction<T> and(Collection<BooleanFunction<T>> functions) {
        if (functions.isEmpty()) {
            return ofTrue();
        }
        if (functions.size() == 1) {
            return functions.iterator().next();
        }
        return new BooleanAnd<>(functions);
    }

    private enum OfTrue implements BooleanFunction<Object> {
        INSTANCE;

        @Override
        public boolean test(Object value) {
            return true;
        }
    }

    private enum OfFalse implements BooleanFunction<Object> {
        INSTANCE;

        @Override
        public boolean test(Object value) {
            return false;
        }
    }

    private enum PrimitiveBoolean implements BooleanFunction<Object> {
        INSTANCE;

        @Override
        public boolean test(Object value) {
            return (boolean) value;
        }
    }

    private static class BooleanMap<T, R> implements BooleanFunction<T> {
        private final Function<T, R> f;
        private final BooleanFunction<R> g;

        public BooleanMap(Function<T, R> f, BooleanFunction<R> g) {
            this.f = Objects.requireNonNull(f);
            this.g = Objects.requireNonNull(g);
        }

        @Override
        public boolean test(T value) {
            return g.test(f.apply(value));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            BooleanMap<?, ?> that = (BooleanMap<?, ?>) o;
            if (!f.equals(that.f))
                return false;
            return g.equals(that.g);
        }

        @Override
        public int hashCode() {
            int result = f.hashCode();
            result = 31 * result + g.hashCode();
            return result;
        }
    }

    private static class BooleanNot<T> implements BooleanFunction<T> {
        private final BooleanFunction<T> function;

        public BooleanNot(BooleanFunction<T> function) {
            this.function = Objects.requireNonNull(function);
        }

        public BooleanFunction<T> function() {
            return function;
        }

        @Override
        public boolean test(T value) {
            return !function.test(value);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            BooleanNot<?> that = (BooleanNot<?>) o;

            return function.equals(that.function);
        }

        @Override
        public int hashCode() {
            return function.hashCode();
        }
    }

    private static class BooleanAnd<T> implements BooleanFunction<T> {
        private final Collection<BooleanFunction<T>> functions;

        public BooleanAnd(Collection<BooleanFunction<T>> functions) {
            this.functions = List.copyOf(functions);
        }

        @Override
        public boolean test(T value) {
            for (BooleanFunction<T> function : functions) {
                if (!function.test(value)) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            BooleanAnd<?> that = (BooleanAnd<?>) o;

            return functions.equals(that.functions);
        }

        @Override
        public int hashCode() {
            return functions.hashCode();
        }
    }

    private static class BooleanOr<T> implements BooleanFunction<T> {
        private final Collection<BooleanFunction<T>> functions;

        public BooleanOr(Collection<BooleanFunction<T>> functions) {
            this.functions = List.copyOf(functions);
        }

        @Override
        public boolean test(T value) {
            for (BooleanFunction<T> function : functions) {
                if (function.test(value)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            BooleanOr<?> booleanOr = (BooleanOr<?>) o;

            return functions.equals(booleanOr.functions);
        }

        @Override
        public int hashCode() {
            return functions.hashCode();
        }
    }
}

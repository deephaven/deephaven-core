/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.functions;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Predicate;

class BooleanFunctions {

    static <T> ToBooleanFunction<T> cast() {
        // noinspection unchecked
        return (ToBooleanFunction<T>) PrimitiveBoolean.INSTANCE;
    }

    static <T> ToBooleanFunction<T> of(Predicate<T> predicate) {
        return predicate instanceof ToBooleanFunction ? (ToBooleanFunction<T>) predicate : predicate::test;
    }

    public static <T> ToBooleanFunction<T> ofTrue() {
        // noinspection unchecked
        return (ToBooleanFunction<T>) OfTrue.INSTANCE;
    }

    public static <T> ToBooleanFunction<T> ofFalse() {
        // noinspection unchecked
        return (ToBooleanFunction<T>) OfFalse.INSTANCE;
    }

    static <T, R> ToBooleanFunction<T> map(Function<T, R> f, Predicate<R> g) {
        return new BooleanMap<>(f, g);
    }

    static <T> ToBooleanFunction<T> not(ToBooleanFunction<T> f) {
        return f instanceof BooleanNot ? of(((BooleanNot<T>) f).function()) : new BooleanNot<>(f);
    }

    static <T> ToBooleanFunction<T> or(Collection<Predicate<T>> functions) {
        if (functions.isEmpty()) {
            return ofFalse();
        }
        if (functions.size() == 1) {
            return of(functions.iterator().next());
        }
        return new BooleanOr<>(functions);
    }

    static <T> ToBooleanFunction<T> and(Collection<Predicate<T>> functions) {
        if (functions.isEmpty()) {
            return ofTrue();
        }
        if (functions.size() == 1) {
            return of(functions.iterator().next());
        }
        return new BooleanAnd<>(functions);
    }

    private enum OfTrue implements ToBooleanFunction<Object> {
        INSTANCE;

        @Override
        public boolean test(Object value) {
            return true;
        }
    }

    private enum OfFalse implements ToBooleanFunction<Object> {
        INSTANCE;

        @Override
        public boolean test(Object value) {
            return false;
        }
    }

    private enum PrimitiveBoolean implements ToBooleanFunction<Object> {
        INSTANCE;

        @Override
        public boolean test(Object value) {
            return (boolean) value;
        }
    }

    private static class BooleanMap<T, R> implements ToBooleanFunction<T> {
        private final Function<T, R> f;
        private final Predicate<R> g;

        public BooleanMap(Function<T, R> f, Predicate<R> g) {
            this.f = Objects.requireNonNull(f);
            this.g = Objects.requireNonNull(g);
        }

        @Override
        public boolean test(T value) {
            return g.test(f.apply(value));
        }
    }

    private static class BooleanNot<T> implements ToBooleanFunction<T> {
        private final Predicate<T> function;

        public BooleanNot(ToBooleanFunction<T> function) {
            this.function = Objects.requireNonNull(function);
        }

        public Predicate<T> function() {
            return function;
        }

        @Override
        public boolean test(T value) {
            return !function.test(value);
        }
    }

    private static class BooleanAnd<T> implements ToBooleanFunction<T> {
        private final Collection<Predicate<T>> functions;

        public BooleanAnd(Collection<Predicate<T>> functions) {
            this.functions = List.copyOf(functions);
        }

        @Override
        public boolean test(T value) {
            for (Predicate<T> function : functions) {
                if (!function.test(value)) {
                    return false;
                }
            }
            return true;
        }
    }

    private static class BooleanOr<T> implements ToBooleanFunction<T> {
        private final Collection<Predicate<T>> functions;

        public BooleanOr(Collection<Predicate<T>> functions) {
            this.functions = List.copyOf(functions);
        }

        @Override
        public boolean test(T value) {
            for (Predicate<T> function : functions) {
                if (function.test(value)) {
                    return true;
                }
            }
            return false;
        }
    }
}

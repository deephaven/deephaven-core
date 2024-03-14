//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.function;

import org.jetbrains.annotations.NotNull;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class BooleanFunctions {

    static <T> ToBooleanFunction<T> cast() {
        return cast(PrimitiveBoolean.INSTANCE);
    }

    static <T> ToBooleanFunction<T> cast(ToBooleanFunction<? super T> f) {
        // noinspection unchecked
        return (ToBooleanFunction<T>) f;
    }

    static <T> ToBooleanFunction<T> of(Predicate<? super T> predicate) {
        return predicate instanceof ToBooleanFunction
                ? cast((ToBooleanFunction<? super T>) predicate)
                : predicate::test;
    }

    public static <T> ToBooleanFunction<T> ofTrue() {
        // noinspection unchecked
        return (ToBooleanFunction<T>) OfTrue.INSTANCE;
    }

    public static <T> ToBooleanFunction<T> ofFalse() {
        // noinspection unchecked
        return (ToBooleanFunction<T>) OfFalse.INSTANCE;
    }

    static <T, R> ToBooleanFunction<T> map(
            Function<? super T, ? extends R> f,
            Predicate<? super R> g) {
        return new BooleanMap<>(f, g);
    }

    static <T> ToBooleanFunction<T> not(Predicate<? super T> f) {
        return f instanceof BooleanNot
                ? cast(((BooleanNot<? super T>) f).negate())
                : new BooleanNot<>(f);
    }

    static <T> ToBooleanFunction<T> or(Collection<Predicate<? super T>> functions) {
        if (functions.isEmpty()) {
            return ofFalse();
        }
        if (functions.size() == 1) {
            return of(functions.iterator().next());
        }
        return new BooleanOr<>(functions);
    }

    static <T> ToBooleanFunction<T> and(Collection<Predicate<? super T>> functions) {
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

        @Override
        @NotNull
        public ToBooleanFunction<Object> negate() {
            return ofFalse();
        }

        @Override
        @NotNull
        public ToBooleanFunction<Object> and(@NotNull Predicate<? super Object> other) {
            // always other
            return of(other);
        }

        @Override
        @NotNull
        public ToBooleanFunction<Object> or(@NotNull Predicate<? super Object> other) {
            // always true
            return this;
        }
    }

    private enum OfFalse implements ToBooleanFunction<Object> {
        INSTANCE;

        @Override
        public boolean test(Object value) {
            return false;
        }

        @Override
        @NotNull
        public ToBooleanFunction<Object> negate() {
            return ofTrue();
        }

        @Override
        @NotNull
        public ToBooleanFunction<Object> and(@NotNull Predicate<? super Object> other) {
            // always false
            return this;
        }

        @Override
        @NotNull
        public ToBooleanFunction<Object> or(@NotNull Predicate<? super Object> other) {
            // always other
            return of(other);
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
        private final Function<? super T, ? extends R> f;
        private final Predicate<? super R> g;

        public BooleanMap(Function<? super T, ? extends R> f, Predicate<? super R> g) {
            this.f = Objects.requireNonNull(f);
            this.g = Objects.requireNonNull(g);
        }

        @Override
        public boolean test(T value) {
            return g.test(f.apply(value));
        }

        @Override
        @NotNull
        public ToBooleanFunction<T> negate() {
            return new BooleanMap<>(f, g.negate());
        }
    }

    private static class BooleanNot<T> implements ToBooleanFunction<T> {
        private final Predicate<? super T> function;

        public BooleanNot(Predicate<? super T> function) {
            this.function = Objects.requireNonNull(function);
        }

        @Override
        public boolean test(T value) {
            return !function.test(value);
        }

        @Override
        @NotNull
        public ToBooleanFunction<T> negate() {
            return of(function);
        }
    }

    private static class BooleanAnd<T> implements ToBooleanFunction<T> {
        private final Collection<Predicate<? super T>> functions;

        public BooleanAnd(Collection<Predicate<? super T>> functions) {
            this.functions = List.copyOf(functions);
        }

        @Override
        public boolean test(T value) {
            for (Predicate<? super T> function : functions) {
                if (!function.test(value)) {
                    return false;
                }
            }
            return true;
        }

        @Override
        @NotNull
        public ToBooleanFunction<T> negate() {
            return new BooleanOr<>(functions.stream().map(Predicate::negate).collect(Collectors.toList()));
        }

        @Override
        @NotNull
        public ToBooleanFunction<T> and(@NotNull Predicate<? super T> other) {
            // noinspection Convert2Diamond
            return new BooleanAnd<T>(Stream.concat(functions.stream(), Stream.of(other)).collect(Collectors.toList()));
        }
    }

    private static class BooleanOr<T> implements ToBooleanFunction<T> {
        private final Collection<Predicate<? super T>> functions;

        public BooleanOr(Collection<Predicate<? super T>> functions) {
            this.functions = List.copyOf(functions);
        }

        @Override
        public boolean test(T value) {
            for (Predicate<? super T> function : functions) {
                if (function.test(value)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        @NotNull
        public ToBooleanFunction<T> negate() {
            return new BooleanAnd<>(functions.stream().map(Predicate::negate).collect(Collectors.toList()));
        }

        @Override
        @NotNull
        public ToBooleanFunction<T> or(@NotNull Predicate<? super T> other) {
            // noinspection Convert2Diamond
            return new BooleanOr<T>(Stream.concat(functions.stream(), Stream.of(other)).collect(Collectors.toList()));
        }
    }
}

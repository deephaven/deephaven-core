//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util;

import java.util.Objects;

public class SafeCloseablePair<A extends AutoCloseable, B extends AutoCloseable> implements SafeCloseable {

    public final A first;
    public final B second;

    public SafeCloseablePair(A first, B second) {
        this.first = first;
        this.second = second;
    }

    public String toString() {
        return "Pair[" + this.first + "," + this.second + "]";
    }

    public boolean equals(final Object other) {
        if (!(other instanceof SafeCloseablePair)) {
            return false;
        }
        SafeCloseablePair<?, ?> otherPair = (SafeCloseablePair<?, ?>) other;
        return Objects.equals(this.first, otherPair.first) && Objects.equals(this.second, otherPair.second);
    }

    public int hashCode() {
        return Objects.hash(this.first, this.second);
    }

    @Override
    public void close() {
        SafeCloseable.closeAll(first, second);
    }

    public static <AP extends SafeCloseable, BP extends SafeCloseable, A extends AP, B extends BP> SafeCloseablePair<AP, BP> downcast(
            SafeCloseablePair<A, B> self) {
        // noinspection unchecked
        return (SafeCloseablePair<AP, BP>) self;
    }

    public static <A extends SafeCloseable, B extends SafeCloseable> SafeCloseablePair<A, B> of(final A first,
            final B second) {
        return new SafeCloseablePair<>(first, second);
    }

    public A getFirst() {
        return first;
    }

    public B getSecond() {
        return second;
    }
}

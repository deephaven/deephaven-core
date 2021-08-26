package io.deephaven.util;

import java.util.Objects;

public class SafeCloseablePair<A extends SafeCloseable, B extends SafeCloseable> implements SafeCloseable {
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
        if (this.first != null) {
            this.first.close();
        }
        if (this.second != null) {
            this.second.close();
        }
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

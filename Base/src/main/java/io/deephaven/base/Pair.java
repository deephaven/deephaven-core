/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base;

import org.jetbrains.annotations.NotNull;

import java.io.Serializable;
import java.util.Objects;

/**
 * A Simple generic pair. Can be used as a compound return value for lambdas.
 */
public class Pair<A, B> implements Comparable<Pair<A, B>>, Serializable {
    private static final long serialVersionUID = 1884389166059435494L;
    public final A first;
    public final B second;

    public Pair(final A first, final B second) {
        this.first = first;
        this.second = second;
    }

    public A getFirst() {
        return first;
    }

    public B getSecond() {
        return second;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        Pair<?, ?> pair = (Pair<?, ?>) o;

        return Objects.equals(first, pair.first) && Objects.equals(second, pair.second);
    }

    @Override
    public int hashCode() {
        int result = first != null ? first.hashCode() : 0;
        result = 31 * result + (second != null ? second.hashCode() : 0);
        return result;
    }


    @Override
    public int compareTo(@NotNull Pair<A, B> o) {
        if (first == null) {
            if (o.first != null) {
                return -1;
            }
        } else if (o.first == null) {
            return 1;
        } else {
            // noinspection unchecked
            int firstResult = ((Comparable) first).compareTo(o.first);
            if (firstResult != 0) {
                return firstResult;
            }
        }

        if (second == null) {
            if (o.second == null) {
                return 0;
            }
            return -1;
        } else if (o.second == null) {
            return 1;
        }

        // noinspection unchecked
        return ((Comparable) second).compareTo(o.second);
    }
}

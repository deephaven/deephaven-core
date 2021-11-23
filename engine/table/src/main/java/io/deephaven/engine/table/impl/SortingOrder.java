/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl;

import org.jetbrains.annotations.NotNull;

import java.util.Comparator;

/**
 * Enum value for ascending vs. descending sorts.
 */
public enum SortingOrder {
    Ascending(1, getAscendingComparable()), Descending(-1, getDescendingComparator());

    public final int direction;
    private final Comparator<Comparable> comparator;

    SortingOrder(int direction, Comparator<Comparable> comparator) {
        this.direction = direction;
        this.comparator = comparator;
    }

    public int getDirection() {
        return direction;
    }

    public Comparator<Comparable> getComparator() {
        return comparator;
    }

    public boolean isAscending() {
        return direction == 1;
    }

    public boolean isDescending() {
        return direction == -1;
    }

    @NotNull
    private static Comparator<Comparable> getAscendingComparable() {
        return (o1, o2) -> {
            if (o1 == o2) {
                return 0;
            } else if (o1 == null) {
                return -1;
            } else if (o2 == null) {
                return 1;
            }
            // noinspection unchecked
            return o1.compareTo(o2);
        };
    }

    @NotNull
    private static Comparator<Comparable> getDescendingComparator() {
        return (o1, o2) -> {
            if (o1 == o2) {
                return 0;
            } else if (o1 == null) {
                return 1;
            } else if (o2 == null) {
                return -1;
            }
            // noinspection unchecked
            return -o1.compareTo(o2);
        };
    }
}

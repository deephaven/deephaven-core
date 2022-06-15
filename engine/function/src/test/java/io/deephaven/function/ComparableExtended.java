/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.function;

class ComparableExtended implements Comparable<ComparableExtended> {

    private final Double i;

    ComparableExtended(final Double i) {
        this.i = i;
    }

    @Override
    public int compareTo(ComparableExtended o) {
        return i.compareTo(o.i);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof ComparableExtended))
            return false;

        ComparableExtended that = (ComparableExtended) o;

        return i != null ? i.equals(that.i) : that.i == null;
    }

    @Override
    public int hashCode() {
        return i != null ? i.hashCode() : 0;
    }
}

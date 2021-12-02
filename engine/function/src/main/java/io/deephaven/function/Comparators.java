/*
 *
 * * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 *
 */

package io.deephaven.function;

import java.util.Comparator;

import static io.deephaven.function.ObjectPrimitives.isDeephavenNull;
import static io.deephaven.util.QueryConstants.NULL_DOUBLE;
import static io.deephaven.util.QueryConstants.NULL_LONG;

/**
 * Comparators that handle DH null values.
 */
class Comparators {

    public static class AscendingOrderComparator<T> implements Comparator<T> {

        private static AscendingOrderComparator comparator = new AscendingOrderComparator<>();
        private static Comparator<Object> reverseComparator = new AscendingOrderComparator<>().reversed();

        private AscendingOrderComparator() {}

        public static AscendingOrderComparator getInstance() {
            return comparator;
        }

        public static Comparator<Object> getReversedInstance() {
            return reverseComparator;
        }

        @Override
        public int compare(final T o1, final T o2) {
            return Comparators.compare(o1, o2);
        }
    }

    public static class AscendingOrderComparatorNumber<T extends Number> implements Comparator<T> {

        private static AscendingOrderComparatorNumber comparatorNumber = new AscendingOrderComparatorNumber<>();
        private static Comparator<Number> reverseComparatorNumber = new AscendingOrderComparatorNumber<>().reversed();

        private AscendingOrderComparatorNumber() {}

        public static AscendingOrderComparatorNumber getInstance() {
            return comparatorNumber;
        }

        public static Comparator<Number> getReversedInstance() {
            return reverseComparatorNumber;
        }

        @Override
        public int compare(final T o1, final T o2) {
            return Comparators.compare(o1, o2);
        }
    }

    static <T> int compare(final T o1, final T o2) {
        final boolean dhNull1 = isDeephavenNull(o1);
        final boolean dhNull2 = isDeephavenNull(o2);
        if (dhNull1 && dhNull2) {
            return 0;
        } else if (dhNull1) {
            return -1;
        } else if (dhNull2) {
            return 1;
        } else if (o1.getClass() == o2.getClass() && Comparable.class.isAssignableFrom(o1.getClass())) {
            return ((Comparable) o1).compareTo(o2);
        } else if (o1.getClass() == Double.class && o2.getClass() == Long.class) {
            return compareDoubleAndLong((Double) o1, (Long) o2);
        } else if (o1.getClass() == Long.class && o2.getClass() == Double.class) {
            return -compareDoubleAndLong((Double) o2, (Long) o1);
        } else if (Number.class.isAssignableFrom(o1.getClass()) && Number.class.isAssignableFrom(o2.getClass())) {
            return Double.compare(((Number) o1).doubleValue(), ((Number) o2).doubleValue());
        } else {
            throw new IllegalArgumentException("Can not compare classes : " + o1.getClass() + " and " + o2.getClass());
        }
    }

    private static int compareDoubleAndLong(final Double o1, final Long o2) {
        final double o1Prim = o1;
        final long o2Prim = o2;
        final long o1PrimLong = (long) o1Prim;
        if (o1Prim == NULL_DOUBLE && o2Prim == NULL_LONG) {
            return 0;
        } else if (o1Prim == NULL_DOUBLE) {
            return -1;
        } else if (o2Prim == NULL_LONG || Double.isNaN(o1Prim)) {// As Double.NaN is considered the greatest
            return 1;
        } else {
            if (o1PrimLong < o2Prim) {
                return -1;
            } else if (o1PrimLong == o2Prim) {
                final double diff = o1Prim - o1PrimLong;
                if (diff > 0d) {
                    return 1;
                } else if (diff == 0d) {
                    return 0;
                } else {
                    return -1;
                }
            } else {
                return 1;
            }
        }
    }
}

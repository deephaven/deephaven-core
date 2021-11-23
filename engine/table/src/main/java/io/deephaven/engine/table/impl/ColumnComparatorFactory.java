/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.util.compare.CharComparisons;
import io.deephaven.util.compare.DoubleComparisons;
import io.deephaven.util.compare.FloatComparisons;

import java.util.function.Predicate;

import static io.deephaven.util.QueryConstants.*;

public class ColumnComparatorFactory {
    /**
     * Produce an {@link IComparator} specialized for a given left and right {@link ColumnSource}. To do this we look at
     * the underlying types of the column sources (actually we require those underlying types to be the same) and we use
     * this information to call the specific primitive type getter method (whether {@link ColumnSource#getDouble},
     * {@link ColumnSource#getLong} and so on). This approach allows us to avoid boxing on these calls. We use a similar
     * approach in order to test the null-ness of a given element.
     *
     * This method is not especially efficient, but (because we are typically not being called from an inner loop), that
     * fact is probably not relevant for performance. The point is that the returned IComparator *is* rather efficient.
     * 
     * @param lcs The left-hand ColumnSource (uses current values)
     * @param rcs The right-hand ColumnSource (uses current values)
     * @return An AbstractColumnSource.IComparator designed to compare elements from the two column sources.
     */
    public static IComparator createComparator(final ColumnSource lcs,
            final ColumnSource rcs) {
        final Class lType = lcs.getType();
        final Class rType = rcs.getType();
        Assert.eq(lType, "lType", rType, "rType");

        // lType equals rType, so we could use either here.
        final Predicate<Class> provides = lType::equals;

        if (provides.test(byte.class)) {
            return (lKey, rKey) -> {
                final byte l = lcs.getByte(lKey);
                final byte r = rcs.getByte(rKey);
                return l == r ? 0 : (l == NULL_BYTE ? -1 : (r == NULL_BYTE ? 1 : Byte.compare(l, r)));
            };
        }
        if (provides.test(char.class)) {
            return (lKey, rKey) -> CharComparisons.compare(lcs.getChar(lKey), rcs.getChar(rKey));
        }
        if (provides.test(short.class)) {
            return (lKey, rKey) -> {
                final short l = lcs.getShort(lKey);
                final short r = rcs.getShort(rKey);
                return l == r ? 0 : (l == NULL_SHORT ? -1 : (r == NULL_SHORT ? 1 : Short.compare(l, r)));
            };
        }
        if (provides.test(int.class)) {
            return (lKey, rKey) -> {
                final int l = lcs.getInt(lKey);
                final int r = rcs.getInt(rKey);
                return l == r ? 0 : (l == NULL_INT ? -1 : (r == NULL_INT ? 1 : Integer.compare(l, r)));
            };
        }
        if (provides.test(long.class)) {
            return (lKey, rKey) -> {
                final long l = lcs.getLong(lKey);
                final long r = rcs.getLong(rKey);
                return l == r ? 0 : (l == NULL_LONG ? -1 : (r == NULL_LONG ? 1 : Long.compare(l, r)));
            };
        }
        if (provides.test(float.class)) {
            return (lKey, rKey) -> FloatComparisons.compare(lcs.getFloat(lKey), rcs.getFloat(rKey));
        }
        if (provides.test(double.class)) {
            return (lKey, rKey) -> DoubleComparisons.compare(lcs.getDouble(lKey), rcs.getDouble(rKey));
        }
        // fall through to Object interface
        return (lKey, rKey) -> {
            final Comparable l = (Comparable) lcs.get(lKey);
            final Comparable r = (Comparable) rcs.get(rKey);
            return l == r ? 0 : (l == null ? -1 : (r == null ? 1 : l.compareTo(r)));
        };
    }

    /**
     * Produce an {@link IComparator} specialized for a given left and right {@link ColumnSource}. To do this we look at
     * the underlying types of the column sources (actually we require those underlying types to be the same) and we use
     * this information to call the specific primitive type getter method (whether {@link ColumnSource#getDouble},
     * {@link ColumnSource#getLong} and so on). This approach allows us to avoid boxing on these calls. We use a similar
     * approach in order to test the null-ness of a given element.
     *
     * This method is not especially efficient, but (because we are typically not being called from an inner loop), that
     * fact is probably not relevant for performance. The point is that the returned IComparatorEnhanced *is* rather
     * efficient.
     * 
     * @param lcs The left-hand ColumnSource (uses current values)
     * @param rcs The right-hand ColumnSource (uses previous values)
     * @return An AbstractColumnSource.IComparator designed to compare elements from the two column sources.
     */
    public static IComparator createComparatorLeftCurrRightPrev(
            final ColumnSource lcs, final ColumnSource rcs) {
        final Class lType = lcs.getType();
        final Class rType = rcs.getType();
        Assert.eq(lType, "lType", rType, "rType");

        // lType equals rType, so we could use either here.
        final Predicate<Class> provides = lType::equals;

        if (provides.test(byte.class)) {
            return (lKey, rKey) -> {
                final byte l = lcs.getByte(lKey);
                final byte r = rcs.getPrevByte(rKey);
                return l == r ? 0 : (l == NULL_BYTE ? -1 : (r == NULL_BYTE ? 1 : Byte.compare(l, r)));
            };
        }
        if (provides.test(char.class)) {
            return (lKey, rKey) -> {
                final char l = lcs.getChar(lKey);
                final char r = rcs.getPrevChar(rKey);
                return l == r ? 0 : (l == NULL_CHAR ? -1 : (r == NULL_CHAR ? 1 : Character.compare(l, r)));
            };
        }
        if (provides.test(short.class)) {
            return (lKey, rKey) -> {
                final short l = lcs.getShort(lKey);
                final short r = rcs.getPrevShort(rKey);
                return l == r ? 0 : (l == NULL_SHORT ? -1 : (r == NULL_SHORT ? 1 : Short.compare(l, r)));
            };
        }
        if (provides.test(int.class)) {
            return (lKey, rKey) -> {
                final int l = lcs.getInt(lKey);
                final int r = rcs.getPrevInt(rKey);
                return l == r ? 0 : (l == NULL_INT ? -1 : (r == NULL_INT ? 1 : Integer.compare(l, r)));
            };
        }
        if (provides.test(long.class)) {
            return (lKey, rKey) -> {
                final long l = lcs.getLong(lKey);
                final long r = rcs.getPrevLong(rKey);
                return l == r ? 0 : (l == NULL_LONG ? -1 : (r == NULL_LONG ? 1 : Long.compare(l, r)));
            };
        }
        if (provides.test(float.class)) {
            return (lKey, rKey) -> {
                final float l = lcs.getFloat(lKey);
                final float r = rcs.getPrevFloat(rKey);
                return l == r ? 0 : (l == NULL_FLOAT ? -1 : (r == NULL_FLOAT ? 1 : Float.compare(l, r)));
            };
        }
        if (provides.test(double.class)) {
            return (lKey, rKey) -> {
                final double l = lcs.getDouble(lKey);
                final double r = rcs.getPrevDouble(rKey);
                return l == r ? 0 : (l == NULL_DOUBLE ? -1 : (r == NULL_DOUBLE ? 1 : Double.compare(l, r)));
            };
        }
        // fall through to Object interface
        return (lKey, rKey) -> {
            final Comparable l = (Comparable) lcs.get(lKey);
            final Comparable r = (Comparable) rcs.getPrev(rKey);
            return l == r ? 0 : (l == null ? -1 : (r == null ? 1 : l.compareTo(r)));
        };
    }

    public interface IComparator {
        int compare(long pos1, long pos2);
    }
}

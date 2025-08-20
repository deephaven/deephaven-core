//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.comparators;

import io.deephaven.util.QueryConstants;
import org.junit.Test;

import java.math.BigInteger;
import java.util.Comparator;

import static org.junit.Assert.assertEquals;

public class TestArrayComparators {
    @Test
    public void characters() {
        final Comparator<char[]> comparator = new CharArrayComparator();

        final char[] abc = new char[] {'a', 'b', 'c'};
        final char[] ab = new char[] {'a', 'b'};
        final char[] ac = new char[] {'a', 'c'};

        // noinspection EqualsWithItself
        assertEquals(0, comparator.compare(null, null));
        assertEquals(-1, comparator.compare(null, ab));
        assertEquals(1, comparator.compare(ab, null));

        // noinspection EqualsWithItself
        assertEquals(0, comparator.compare(abc, abc));
        assertEquals(-1, comparator.compare(ab, abc));
        assertEquals(1, comparator.compare(abc, ab));
        assertEquals(-1, comparator.compare(ab, ac));
        assertEquals(1, comparator.compare(ac, ab));

        final char[] abn = new char[] {'a', 'b', QueryConstants.NULL_CHAR};
        // noinspection EqualsWithItself
        assertEquals(0, comparator.compare(abn, abn));
        assertEquals(-1, comparator.compare(ab, abn));
        assertEquals(1, comparator.compare(abn, ab));
        assertEquals(-1, comparator.compare(abn, abc));
        assertEquals(1, comparator.compare(abc, abn));
    }

    @Test
    public void doubles() {
        final Comparator<double[]> comparator = new DoubleArrayComparator();

        final double[] abc = new double[] {'a', 'b', 'c'};
        final double[] ab = new double[] {'a', 'b'};
        final double[] ac = new double[] {'a', 'c'};

        // noinspection EqualsWithItself
        assertEquals(0, comparator.compare(null, null));
        assertEquals(-1, comparator.compare(null, ab));
        assertEquals(1, comparator.compare(ab, null));

        // noinspection EqualsWithItself
        assertEquals(0, comparator.compare(abc, abc));
        assertEquals(-1, comparator.compare(ab, abc));
        assertEquals(1, comparator.compare(abc, ab));
        assertEquals(-1, comparator.compare(ab, ac));
        assertEquals(1, comparator.compare(ac, ab));

        final double[] abn = new double[] {'a', 'b', QueryConstants.NULL_DOUBLE};
        // noinspection EqualsWithItself
        assertEquals(0, comparator.compare(abn, abn));
        assertEquals(-1, comparator.compare(ab, abn));
        assertEquals(1, comparator.compare(abn, ab));
        assertEquals(-1, comparator.compare(abn, abc));
        assertEquals(1, comparator.compare(abc, abn));

        final double[] abnan = new double[] {'a', 'b', Double.NaN};
        // noinspection EqualsWithItself
        assertEquals(0, comparator.compare(abnan, abnan));
        assertEquals(-1, comparator.compare(ab, abnan));
        assertEquals(1, comparator.compare(abnan, ab));
        assertEquals(1, comparator.compare(abnan, abc));
        assertEquals(-1, comparator.compare(abc, abnan));

        final double[] pz = new double[] {1.0, 0.0, 3};
        final double[] nz = new double[] {1.0, -0.0, 3};
        assertEquals(0, comparator.compare(pz, nz));

        final double[] nipi = new double[] {Double.NEGATIVE_INFINITY, 1};
        final double[] nullpi = new double[] {QueryConstants.NULL_DOUBLE, Double.POSITIVE_INFINITY};
        assertEquals(-1, comparator.compare(nullpi, nipi));
        assertEquals(1, comparator.compare(nipi, nullpi));

        final double[] abpi = new double[] {'a', 'b', Float.POSITIVE_INFINITY};
        assertEquals(-1, comparator.compare(abpi, abnan));
        assertEquals(1, comparator.compare(abnan, abpi));
    }

    @Test
    public void floats() {
        final Comparator<float[]> comparator = new FloatArrayComparator();

        final float[] abc = new float[] {'a', 'b', 'c'};
        final float[] ab = new float[] {'a', 'b'};
        final float[] ac = new float[] {'a', 'c'};

        // noinspection EqualsWithItself
        assertEquals(0, comparator.compare(null, null));
        assertEquals(-1, comparator.compare(null, ab));
        assertEquals(1, comparator.compare(ab, null));

        // noinspection EqualsWithItself
        assertEquals(0, comparator.compare(abc, abc));
        assertEquals(-1, comparator.compare(ab, abc));
        assertEquals(1, comparator.compare(abc, ab));
        assertEquals(-1, comparator.compare(ab, ac));
        assertEquals(1, comparator.compare(ac, ab));

        final float[] abn = new float[] {'a', 'b', QueryConstants.NULL_FLOAT};
        // noinspection EqualsWithItself
        assertEquals(0, comparator.compare(abn, abn));
        assertEquals(-1, comparator.compare(ab, abn));
        assertEquals(1, comparator.compare(abn, ab));
        assertEquals(-1, comparator.compare(abn, abc));
        assertEquals(1, comparator.compare(abc, abn));

        final float[] abnan = new float[] {'a', 'b', Float.NaN};
        // noinspection EqualsWithItself
        assertEquals(0, comparator.compare(abnan, abnan));
        assertEquals(-1, comparator.compare(ab, abnan));
        assertEquals(1, comparator.compare(abnan, ab));
        assertEquals(1, comparator.compare(abnan, abc));
        assertEquals(-1, comparator.compare(abc, abnan));

        final float[] pz = new float[] {1.0f, 0.0f, 3};
        final float[] nz = new float[] {1.0f, -0.0f, 3};
        assertEquals(0, comparator.compare(pz, nz));

        final float[] nipi = new float[] {Float.NEGATIVE_INFINITY, 1};
        final float[] nullpi = new float[] {QueryConstants.NULL_FLOAT, Float.POSITIVE_INFINITY};
        assertEquals(-1, comparator.compare(nullpi, nipi));
        assertEquals(1, comparator.compare(nipi, nullpi));

        final float[] abpi = new float[] {'a', 'b', Float.POSITIVE_INFINITY};
        assertEquals(-1, comparator.compare(abpi, abnan));
        assertEquals(1, comparator.compare(abnan, abpi));
    }

    @Test
    public void strings() {
        final Comparator<Object[]> comparator = new ObjectArrayComparator();

        final String[] abc = new String[] {"a", "b", "c"};
        final String[] ab = new String[] {"a", "b"};
        final String[] ac = new String[] {"a", "c"};

        // noinspection EqualsWithItself
        assertEquals(0, comparator.compare(null, null));
        assertEquals(-1, comparator.compare(null, ab));
        assertEquals(1, comparator.compare(ab, null));

        // noinspection EqualsWithItself
        assertEquals(0, comparator.compare(abc, abc));
        assertEquals(-1, comparator.compare(ab, abc));
        assertEquals(1, comparator.compare(abc, ab));
        assertEquals(-1, comparator.compare(ab, ac));
        assertEquals(1, comparator.compare(ac, ab));

        final String[] abn = new String[] {"a", "b", null};
        // noinspection EqualsWithItself
        assertEquals(0, comparator.compare(abn, abn));
        assertEquals(-1, comparator.compare(ab, abn));
        assertEquals(1, comparator.compare(abn, ab));
        assertEquals(-1, comparator.compare(abn, abc));
        assertEquals(1, comparator.compare(abc, abn));
    }


    @Test
    public void bigint() {
        final Comparator<Object[]> comparator = new ObjectArrayComparator();

        final BigInteger[] abc = new BigInteger[] {BigInteger.ONE, BigInteger.TEN, BigInteger.valueOf(20)};
        final BigInteger[] ab = new BigInteger[] {BigInteger.ONE, BigInteger.TEN};
        final BigInteger[] ac = new BigInteger[] {BigInteger.ONE, BigInteger.valueOf(20)};

        // noinspection EqualsWithItself
        assertEquals(0, comparator.compare(null, null));
        assertEquals(-1, comparator.compare(null, ab));
        assertEquals(1, comparator.compare(ab, null));

        // noinspection EqualsWithItself
        assertEquals(0, comparator.compare(abc, abc));
        assertEquals(-1, comparator.compare(ab, abc));
        assertEquals(1, comparator.compare(abc, ab));
        assertEquals(-1, comparator.compare(ab, ac));
        assertEquals(1, comparator.compare(ac, ab));

        final BigInteger[] abn = new BigInteger[] {BigInteger.ONE, BigInteger.TEN, null};
        // noinspection EqualsWithItself
        assertEquals(0, comparator.compare(abn, abn));
        assertEquals(-1, comparator.compare(ab, abn));
        assertEquals(1, comparator.compare(abn, ab));
        assertEquals(-1, comparator.compare(abn, abc));
        assertEquals(1, comparator.compare(abc, abn));
    }
}

/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.function;

import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.vector.*;

import static io.deephaven.function.Casting.*;
import static io.deephaven.util.QueryConstants.*;

public class TestCasting extends BaseArrayTestCase {

    public void testCastLong() {
        assertNull(castLong((byte[]) null));
        assertNull(castLong((short[]) null));
        assertNull(castLong((int[]) null));
        assertNull(castLong((long[]) null));

        assertEquals(new long[] {1, 2, 3, NULL_LONG}, castLong(new byte[] {1, 2, 3, NULL_BYTE}));
        assertEquals(new long[] {1, 2, 3, NULL_LONG}, castLong(new short[] {1, 2, 3, NULL_SHORT}));
        assertEquals(new long[] {1, 2, 3, NULL_LONG}, castLong(new int[] {1, 2, 3, NULL_INT}));
        assertEquals(new long[] {1, 2, 3, NULL_LONG}, castLong(new long[] {1, 2, 3, NULL_LONG}));

        assertNull(castLong((ByteVector) null));
        assertNull(castLong((ShortVector) null));
        assertNull(castLong((IntVector) null));
        assertNull(castLong((LongVector) null));

        assertEquals(new long[] {1, 2, 3, NULL_LONG}, castLong(new ByteVectorDirect(new byte[] {1, 2, 3, NULL_BYTE})));
        assertEquals(new long[] {1, 2, 3, NULL_LONG},
                castLong(new ShortVectorDirect(new short[] {1, 2, 3, NULL_SHORT})));
        assertEquals(new long[] {1, 2, 3, NULL_LONG}, castLong(new IntVectorDirect(new int[] {1, 2, 3, NULL_INT})));
        assertEquals(new long[] {1, 2, 3, NULL_LONG}, castLong(new LongVectorDirect(new long[] {1, 2, 3, NULL_LONG})));
    }

    public void testCastDouble() {
        assertNull(castDouble((byte[]) null));
        assertNull(castDouble((short[]) null));
        assertNull(castDouble((int[]) null));
        assertNull(castDouble((long[]) null));
        assertNull(castDouble((float[]) null));
        assertNull(castDouble((double[]) null));

        assertEquals(new double[] {1, 2, 3, NULL_DOUBLE}, castDouble(new byte[] {1, 2, 3, NULL_BYTE}));
        assertEquals(new double[] {1, 2, 3, NULL_DOUBLE}, castDouble(new short[] {1, 2, 3, NULL_SHORT}));
        assertEquals(new double[] {1, 2, 3, NULL_DOUBLE}, castDouble(new int[] {1, 2, 3, NULL_INT}));
        assertEquals(new double[] {1, 2, 3, NULL_DOUBLE}, castDouble(new long[] {1, 2, 3, NULL_LONG}));
        assertEquals(new double[] {1, 2, 3, NULL_DOUBLE}, castDouble(new float[] {1, 2, 3, NULL_FLOAT}));
        assertEquals(new double[] {1, 2, 3, NULL_DOUBLE}, castDouble(new double[] {1, 2, 3, NULL_DOUBLE}));

        assertNull(castDouble((ByteVector) null));
        assertNull(castDouble((ShortVector) null));
        assertNull(castDouble((IntVector) null));
        assertNull(castDouble((LongVector) null));
        assertNull(castDouble((FloatVector) null));
        assertNull(castDouble((DoubleVector) null));

        assertEquals(new double[] {1, 2, 3, NULL_DOUBLE},
                castDouble(new ByteVectorDirect(new byte[] {1, 2, 3, NULL_BYTE})));
        assertEquals(new double[] {1, 2, 3, NULL_DOUBLE},
                castDouble(new ShortVectorDirect(new short[] {1, 2, 3, NULL_SHORT})));
        assertEquals(new double[] {1, 2, 3, NULL_DOUBLE},
                castDouble(new IntVectorDirect(new int[] {1, 2, 3, NULL_INT})));
        assertEquals(new double[] {1, 2, 3, NULL_DOUBLE},
                castDouble(new LongVectorDirect(new long[] {1, 2, 3, NULL_LONG})));
        assertEquals(new double[] {1, 2, 3, NULL_DOUBLE},
                castDouble(new FloatVectorDirect(new float[] {1, 2, 3, NULL_FLOAT})));
        assertEquals(new double[] {1, 2, 3, NULL_DOUBLE},
                castDouble(new DoubleVectorDirect(new double[] {1, 2, 3, NULL_DOUBLE})));
    }

    public void testIntToDouble() {
        assertNull(intToDouble((int[]) null));
        assertEquals(new double[] {1, 2, 3, NULL_DOUBLE}, intToDouble(new int[] {1, 2, 3, NULL_INT}));

        assertNull(intToDouble((IntVector) null));
        assertEquals(new double[] {1, 2, 3, NULL_DOUBLE},
                intToDouble(new IntVectorDirect(new int[] {1, 2, 3, NULL_INT})).toArray());
    }

    public void testLongToDouble() {
        assertNull(longToDouble((long[]) null));
        assertEquals(new double[] {1, 2, 3, NULL_DOUBLE}, longToDouble(new long[] {1, 2, 3, NULL_LONG}));

        assertNull(longToDouble((LongVector) null));
        assertEquals(new double[] {1, 2, 3, NULL_DOUBLE},
                longToDouble(new LongVectorDirect(new long[] {1, 2, 3, NULL_LONG})).toArray());
    }
}

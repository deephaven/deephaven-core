/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.libs.primitives;

import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.db.tables.dbarrays.*;

import static io.deephaven.libs.primitives.Casting.*;
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

        assertNull(castLong((DbByteArray) null));
        assertNull(castLong((DbShortArray) null));
        assertNull(castLong((DbIntArray) null));
        assertNull(castLong((DbLongArray) null));

        assertEquals(new long[] {1, 2, 3, NULL_LONG}, castLong(new DbByteArrayDirect(new byte[] {1, 2, 3, NULL_BYTE})));
        assertEquals(new long[] {1, 2, 3, NULL_LONG},
                castLong(new DbShortArrayDirect(new short[] {1, 2, 3, NULL_SHORT})));
        assertEquals(new long[] {1, 2, 3, NULL_LONG}, castLong(new DbIntArrayDirect(new int[] {1, 2, 3, NULL_INT})));
        assertEquals(new long[] {1, 2, 3, NULL_LONG}, castLong(new DbLongArrayDirect(new long[] {1, 2, 3, NULL_LONG})));
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

        assertNull(castDouble((DbByteArray) null));
        assertNull(castDouble((DbShortArray) null));
        assertNull(castDouble((DbIntArray) null));
        assertNull(castDouble((DbLongArray) null));
        assertNull(castDouble((DbFloatArray) null));
        assertNull(castDouble((DbDoubleArray) null));

        assertEquals(new double[] {1, 2, 3, NULL_DOUBLE},
                castDouble(new DbByteArrayDirect(new byte[] {1, 2, 3, NULL_BYTE})));
        assertEquals(new double[] {1, 2, 3, NULL_DOUBLE},
                castDouble(new DbShortArrayDirect(new short[] {1, 2, 3, NULL_SHORT})));
        assertEquals(new double[] {1, 2, 3, NULL_DOUBLE},
                castDouble(new DbIntArrayDirect(new int[] {1, 2, 3, NULL_INT})));
        assertEquals(new double[] {1, 2, 3, NULL_DOUBLE},
                castDouble(new DbLongArrayDirect(new long[] {1, 2, 3, NULL_LONG})));
        assertEquals(new double[] {1, 2, 3, NULL_DOUBLE},
                castDouble(new DbFloatArrayDirect(new float[] {1, 2, 3, NULL_FLOAT})));
        assertEquals(new double[] {1, 2, 3, NULL_DOUBLE},
                castDouble(new DbDoubleArrayDirect(new double[] {1, 2, 3, NULL_DOUBLE})));
    }

    public void testIntToDouble() {
        assertNull(intToDouble((int[]) null));
        assertEquals(new double[] {1, 2, 3, NULL_DOUBLE}, intToDouble(new int[] {1, 2, 3, NULL_INT}));

        assertNull(intToDouble((DbIntArray) null));
        assertEquals(new double[] {1, 2, 3, NULL_DOUBLE},
                intToDouble(new DbIntArrayDirect(new int[] {1, 2, 3, NULL_INT})).toArray());
    }

    public void testLongToDouble() {
        assertNull(longToDouble((long[]) null));
        assertEquals(new double[] {1, 2, 3, NULL_DOUBLE}, longToDouble(new long[] {1, 2, 3, NULL_LONG}));

        assertNull(longToDouble((DbLongArray) null));
        assertEquals(new double[] {1, 2, 3, NULL_DOUBLE},
                longToDouble(new DbLongArrayDirect(new long[] {1, 2, 3, NULL_LONG})).toArray());
    }
}

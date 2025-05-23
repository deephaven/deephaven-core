//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources.regioned;

import io.deephaven.base.testing.BaseCachedJMockTestCase;
import io.deephaven.base.verify.RequirementFailure;
import io.deephaven.chunk.attributes.Values;
import junit.framework.TestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Array;

import static io.deephaven.engine.table.impl.sources.regioned.RegionedColumnSource.getFirstRowKey;
import static io.deephaven.engine.table.impl.sources.regioned.RegionedColumnSource.getLastRowKey;
import static io.deephaven.util.QueryConstants.*;

/**
 * Base class for testing {@link RegionedColumnSourceArray} implementations.
 */
@SuppressWarnings({"AnonymousInnerClassMayBeStatic", "JUnit4AnnotatedMethodInJUnit3TestCase"})
public abstract class TstRegionedColumnSourcePrimitive<DATA_TYPE, ATTR extends Values, REGION_TYPE extends ColumnRegion<ATTR>, CS_REGION_TYPE extends ColumnRegion<ATTR>>
        extends BaseCachedJMockTestCase {

    static final byte[] TEST_BYTES =
            new byte[] {NULL_BYTE, 0, 1, 2, Byte.MIN_VALUE + 1, Byte.MAX_VALUE, 100, 126, -56, -1};
    @SuppressWarnings("AutoBoxing")
    static final Boolean[] TEST_BOOLEANS =
            new Boolean[] {NULL_BOOLEAN, true, false, NULL_BOOLEAN, false, true, true, false, false, true};
    static final char[] TEST_CHARS = new char[] {NULL_CHAR, 'A', 'B', 'C', 'D', '1', '2', '3', '4', '5'};
    static final short[] TEST_SHORTS =
            new short[] {NULL_SHORT, 0, 1, 2, Short.MIN_VALUE + 1, Short.MAX_VALUE, 10000, 126, -5600, -1};
    static final int[] TEST_INTS =
            new int[] {NULL_INT, 0, 1, 2, Integer.MIN_VALUE + 1, Integer.MAX_VALUE, 1000000000, 126, -560000000, -1};
    static final long[] TEST_LONGS = new long[] {NULL_LONG, 0, 1, 2, Long.MIN_VALUE + 1, Long.MAX_VALUE,
            1000000000000000000L, 12659, -5600000000000000000L, -1L};
    static final float[] TEST_FLOATS = new float[] {NULL_FLOAT, 0.1f, 1.2f, 2.3f, Float.MIN_VALUE + 1.4f,
            Float.MAX_VALUE, 100.123f, 126000f, -56869.2f, -1.0f};
    static final double[] TEST_DOUBLES = new double[] {NULL_DOUBLE, 0.1, 1.2, 2.3, Double.MIN_VALUE + 1.4,
            Double.MAX_VALUE, 100.123, 126000, -56869.2, -1.0};

    REGION_TYPE[] cr;
    RegionedColumnSourceManager manager;
    RegionedColumnSourceBase<DATA_TYPE, ATTR, CS_REGION_TYPE> SUT;

    private final Class<?> regionTypeClass;

    TstRegionedColumnSourcePrimitive(Class<?> regionTypeClass) {
        this.regionTypeClass = regionTypeClass;
    }

    void fillRegions() {
        for (REGION_TYPE region : cr) {
            SUT.addRegionForUnitTests(region);
        }
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();

        manager = mock(RegionedColumnSourceManager.class, "manager");

        // noinspection unchecked
        cr = (REGION_TYPE[]) Array.newInstance(regionTypeClass, 10);
        for (int cri = 0; cri < cr.length; ++cri) {
            // noinspection unchecked
            cr[cri] = (REGION_TYPE) mock(regionTypeClass, "CR_" + cri);
        }

        // Sub-classes are responsible for setting up SUT.
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
    }

    @Test
    public void testOverflow() {
        try {
            SUT.get(0);
            TestCase.fail();
        } catch (ArrayIndexOutOfBoundsException expected) {
        }
    }

    REGION_TYPE doLookupRegion(long elementRowKey) {
        // noinspection unchecked
        return (REGION_TYPE) SUT.lookupRegion(elementRowKey);
    }

    @Test
    public void testAddRegions() {
        // Test validity checks.
        try {
            SUT.addRegionForUnitTests(null);
            TestCase.fail();
        } catch (RequirementFailure | IllegalArgumentException expected) {
        }

        // Add the 0th region.
        SUT.addRegionForUnitTests(cr[0]);
        TestCase.assertEquals(cr[0], doLookupRegion(getFirstRowKey(0)));
        TestCase.assertEquals(cr[0], doLookupRegion(getLastRowKey(0)));

        // Add the 1st region.
        SUT.addRegionForUnitTests(cr[1]);
        TestCase.assertEquals(cr[1], doLookupRegion(getFirstRowKey(1)));
        TestCase.assertEquals(cr[1], doLookupRegion(getLastRowKey(1)));

        // Prove that the 2nd region is missing.
        try {
            TestCase.assertNull(doLookupRegion(getFirstRowKey(2)));
        } catch (ArrayIndexOutOfBoundsException expected) {
        }
        try {
            TestCase.assertNull(doLookupRegion(getLastRowKey(2)));
        } catch (ArrayIndexOutOfBoundsException expected) {
        }

        // Prove that 9th region is missing.
        try {
            TestCase.assertNull(doLookupRegion(getFirstRowKey(9)));
        } catch (ArrayIndexOutOfBoundsException expected) {
        }
        try {
            TestCase.assertNull(doLookupRegion(getLastRowKey(9)));
        } catch (ArrayIndexOutOfBoundsException expected) {
        }
    }

    @Test
    public void testGetNegativeOne() {
        TestCase.assertNull(SUT.get(-1));
    }

    @Test
    public void testGetPrevNegativeOne() {
        TestCase.assertNull(SUT.getPrev(-1));
    }

    @Test
    public abstract void testGet();

    @Test
    public abstract void testGetPrev();

    @Test
    public void testGetBoolean() {
        try {
            SUT.getBoolean(0);
            TestCase.fail();
        } catch (UnsupportedOperationException expected) {
        }
    }

    @Test
    public void testGetPrevBoolean() {
        try {
            SUT.getPrevBoolean(0);
            TestCase.fail();
        } catch (UnsupportedOperationException expected) {
        }
    }

    @Test
    public void testGetByte() {
        try {
            SUT.getByte(0);
            TestCase.fail();
        } catch (UnsupportedOperationException expected) {
        }
    }

    @Test
    public void testGetPrevByte() {
        try {
            SUT.getPrevByte(0);
            TestCase.fail();
        } catch (UnsupportedOperationException expected) {
        }
    }

    @Test
    public void testGetChar() {
        try {
            SUT.getChar(0);
            TestCase.fail();
        } catch (UnsupportedOperationException expected) {
        }
    }

    @Test
    public void testGetPrevChar() {
        try {
            SUT.getPrevChar(0);
            TestCase.fail();
        } catch (UnsupportedOperationException expected) {
        }
    }

    @Test
    public void testGetDouble() {
        try {
            SUT.getDouble(0);
            TestCase.fail();
        } catch (UnsupportedOperationException expected) {
        }
    }

    @Test
    public void testGetPrevDouble() {
        try {
            SUT.getPrevDouble(0);
            TestCase.fail();
        } catch (UnsupportedOperationException expected) {
        }
    }

    @Test
    public void testGetFloat() {
        try {
            SUT.getFloat(0);
            TestCase.fail();
        } catch (UnsupportedOperationException expected) {
        }
    }

    @Test
    public void testGetPrevFloat() {
        try {
            SUT.getPrevFloat(0);
            TestCase.fail();
        } catch (UnsupportedOperationException expected) {
        }
    }

    @Test
    public void testGetInt() {
        try {
            SUT.getInt(0);
            TestCase.fail();
        } catch (UnsupportedOperationException expected) {
        }
    }

    @Test
    public void testGetPrevInt() {
        try {
            SUT.getPrevInt(0);
            TestCase.fail();
        } catch (UnsupportedOperationException expected) {
        }
    }

    @Test
    public void testGetLong() {
        try {
            SUT.getLong(0);
            TestCase.fail();
        } catch (UnsupportedOperationException expected) {
        }
    }

    @Test
    public void testGetPrevLong() {
        try {
            SUT.getPrevLong(0);
            TestCase.fail();
        } catch (UnsupportedOperationException expected) {
        }
    }

    @Test
    public void testGetShort() {
        try {
            SUT.getShort(0);
            TestCase.fail();
        } catch (UnsupportedOperationException expected) {
        }
    }

    @Test
    public void testGetPrevShort() {
        try {
            SUT.getPrevShort(0);
            TestCase.fail();
        } catch (UnsupportedOperationException expected) {
        }
    }
}

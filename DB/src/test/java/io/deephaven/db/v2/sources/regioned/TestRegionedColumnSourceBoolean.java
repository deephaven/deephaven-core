/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.sources.regioned;

import io.deephaven.db.util.BooleanUtils;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.chunk.Attributes;
import org.junit.Test;

/**
 * Test class for {@link RegionedColumnSourceBoolean}.
 */
@SuppressWarnings({"JUnit4AnnotatedMethodInJUnit3TestCase"})
public class TestRegionedColumnSourceBoolean extends TstRegionedColumnSourceReferencing<Boolean, Attributes.Values, ColumnRegionByte<Attributes.Values>> {

    private ColumnSource<Byte> SUT_AS_BYTE;

    public TestRegionedColumnSourceBoolean() {
        super(ColumnRegionByte.class);
    }

    private void assertLookup(final long elementIndex,
                              final int expectedRegionIndex,
                              final Boolean output,
                              final boolean prev,
                              final boolean boxed) {
        assertLookup(elementIndex, expectedRegionIndex, output, prev, boxed, false);
    }

    private void assertLookup(final long elementIndex,
                              final int expectedRegionIndex,
                              final Boolean output,
                              final boolean prev,
                              final boolean boxed,
                              final boolean reinterpreted) {
        checking(new Expectations() {{
            oneOf(cr[expectedRegionIndex]).getReferencedRegion();
            will(returnValue(cr_n[expectedRegionIndex]));
            oneOf(cr_n[expectedRegionIndex]).getByte(elementIndex);
            will(returnValue(BooleanUtils.booleanAsByte(output)));
        }});
        if (reinterpreted) {
            if (boxed) {
                assertEquals(Byte.valueOf(BooleanUtils.booleanAsByte(output)), prev ? SUT_AS_BYTE.getPrev(elementIndex) : SUT_AS_BYTE.get(elementIndex));
            } else {
                assertEquals(BooleanUtils.booleanAsByte(output), prev ? SUT_AS_BYTE.getPrevByte(elementIndex) : SUT_AS_BYTE.getByte(elementIndex));
            }
        } else {
            if (boxed) {
                assertEquals(output, prev ? SUT.getPrev(elementIndex) : SUT.get(elementIndex));
            } else {
                assertEquals(output, prev ? SUT.getPrevBoolean(elementIndex) : SUT.getBoolean(elementIndex));
            }
        }
        assertIsSatisfied();
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();

        SUT = new RegionedColumnSourceBoolean();
        assertEquals(Boolean.class, SUT.getType());
        SUT_AS_BYTE = SUT.reinterpret(byte.class);
        assertEquals(byte.class, SUT_AS_BYTE.getType());
        assertEquals(SUT, SUT_AS_BYTE.reinterpret(Boolean.class));
    }

    @Override
    public void testGet() {
        fillRegions();

        assertLookup(0L, 0, TEST_BOOLEANS[0], false, true);
        assertLookup(RegionedColumnSource.getLastElementIndex(0), 0, TEST_BOOLEANS[1], false, true);

        assertLookup(RegionedColumnSource.getFirstElementIndex(1) + 1, 1, TEST_BOOLEANS[2], false, true);
        assertLookup(RegionedColumnSource.getLastElementIndex(1) - 1, 1, TEST_BOOLEANS[3], false, true);

        assertLookup(RegionedColumnSource.getFirstElementIndex(4) + 2, 4, TEST_BOOLEANS[4], false, true);
        assertLookup(RegionedColumnSource.getLastElementIndex(4) - 2, 4, TEST_BOOLEANS[5], false, true);

        assertLookup(RegionedColumnSource.getFirstElementIndex(8) + 3, 8, TEST_BOOLEANS[6], false, true);
        assertLookup(RegionedColumnSource.getLastElementIndex(8) - 3, 8, TEST_BOOLEANS[7], false, true);

        assertLookup(RegionedColumnSource.getFirstElementIndex(9) + 4, 9, TEST_BOOLEANS[8], false, true);
        assertLookup(RegionedColumnSource.getLastElementIndex(9) - 4, 9, TEST_BOOLEANS[9], false, true);
    }

    @Override
    public void testGetPrev() {
        fillRegions();

        assertLookup(0L, 0, TEST_BOOLEANS[0], true, true);
        assertLookup(RegionedColumnSource.getLastElementIndex(0), 0, TEST_BOOLEANS[1], true, true);

        assertLookup(RegionedColumnSource.getFirstElementIndex(1) + 1, 1, TEST_BOOLEANS[2], true, true);
        assertLookup(RegionedColumnSource.getLastElementIndex(1) - 1, 1, TEST_BOOLEANS[3], true, true);

        assertLookup(RegionedColumnSource.getFirstElementIndex(4) + 2, 4, TEST_BOOLEANS[4], true, true);
        assertLookup(RegionedColumnSource.getLastElementIndex(4) - 2, 4, TEST_BOOLEANS[5], true, true);

        assertLookup(RegionedColumnSource.getFirstElementIndex(8) + 3, 8, TEST_BOOLEANS[6], true, true);
        assertLookup(RegionedColumnSource.getLastElementIndex(8) - 3, 8, TEST_BOOLEANS[7], true, true);

        assertLookup(RegionedColumnSource.getFirstElementIndex(9) + 4, 9, TEST_BOOLEANS[8], true, true);
        assertLookup(RegionedColumnSource.getLastElementIndex(9) - 4, 9, TEST_BOOLEANS[9], true, true);
    }

    @Override
    public void testGetBoolean() {
        fillRegions();

        assertLookup(0L, 0, TEST_BOOLEANS[0], false, false);
        assertLookup(RegionedColumnSource.getLastElementIndex(0), 0, TEST_BOOLEANS[1], false, false);

        assertLookup(RegionedColumnSource.getFirstElementIndex(1) + 1, 1, TEST_BOOLEANS[2], false, false);
        assertLookup(RegionedColumnSource.getLastElementIndex(1) - 1, 1, TEST_BOOLEANS[3], false, false);

        assertLookup(RegionedColumnSource.getFirstElementIndex(4) + 2, 4, TEST_BOOLEANS[4], false, false);
        assertLookup(RegionedColumnSource.getLastElementIndex(4) - 2, 4, TEST_BOOLEANS[5], false, false);

        assertLookup(RegionedColumnSource.getFirstElementIndex(8) + 3, 8, TEST_BOOLEANS[6], false, false);
        assertLookup(RegionedColumnSource.getLastElementIndex(8) - 3, 8, TEST_BOOLEANS[7], false, false);

        assertLookup(RegionedColumnSource.getFirstElementIndex(9) + 4, 9, TEST_BOOLEANS[8], false, false);
        assertLookup(RegionedColumnSource.getLastElementIndex(9) - 4, 9, TEST_BOOLEANS[9], false, false);
    }

    @Override
    public void testGetPrevBoolean() {
        fillRegions();

        assertLookup(0L, 0, TEST_BOOLEANS[0], true, false);
        assertLookup(RegionedColumnSource.getLastElementIndex(0), 0, TEST_BOOLEANS[1], true, false);

        assertLookup(RegionedColumnSource.getFirstElementIndex(1) + 1, 1, TEST_BOOLEANS[2], true, false);
        assertLookup(RegionedColumnSource.getLastElementIndex(1) - 1, 1, TEST_BOOLEANS[3], true, false);

        assertLookup(RegionedColumnSource.getFirstElementIndex(4) + 2, 4, TEST_BOOLEANS[4], true, false);
        assertLookup(RegionedColumnSource.getLastElementIndex(4) - 2, 4, TEST_BOOLEANS[5], true, false);

        assertLookup(RegionedColumnSource.getFirstElementIndex(8) + 3, 8, TEST_BOOLEANS[6], true, false);
        assertLookup(RegionedColumnSource.getLastElementIndex(8) - 3, 8, TEST_BOOLEANS[7], true, false);

        assertLookup(RegionedColumnSource.getFirstElementIndex(9) + 4, 9, TEST_BOOLEANS[8], true, false);
        assertLookup(RegionedColumnSource.getLastElementIndex(9) - 4, 9, TEST_BOOLEANS[9], true, false);
    }

    @Test
    public void testGetReinterpreted() {
        fillRegions();

        assertLookup(0L, 0, TEST_BOOLEANS[0], false, true, true);
        assertLookup(RegionedColumnSource.getLastElementIndex(0), 0, TEST_BOOLEANS[1], false, true, true);

        assertLookup(RegionedColumnSource.getFirstElementIndex(1) + 1, 1, TEST_BOOLEANS[2], false, true, true);
        assertLookup(RegionedColumnSource.getLastElementIndex(1) - 1, 1, TEST_BOOLEANS[3], false, true, true);

        assertLookup(RegionedColumnSource.getFirstElementIndex(4) + 2, 4, TEST_BOOLEANS[4], false, true, true);
        assertLookup(RegionedColumnSource.getLastElementIndex(4) - 2, 4, TEST_BOOLEANS[5], false, true, true);

        assertLookup(RegionedColumnSource.getFirstElementIndex(8) + 3, 8, TEST_BOOLEANS[6], false, true, true);
        assertLookup(RegionedColumnSource.getLastElementIndex(8) - 3, 8, TEST_BOOLEANS[7], false, true, true);

        assertLookup(RegionedColumnSource.getFirstElementIndex(9) + 4, 9, TEST_BOOLEANS[8], false, true, true);
        assertLookup(RegionedColumnSource.getLastElementIndex(9) - 4, 9, TEST_BOOLEANS[9], false, true, true);
    }

    @Test
    public void testGetPrevReinterpreted() {
        fillRegions();

        assertLookup(0L, 0, TEST_BOOLEANS[0], true, true);
        assertLookup(RegionedColumnSource.getLastElementIndex(0), 0, TEST_BOOLEANS[1], true, true, true);

        assertLookup(RegionedColumnSource.getFirstElementIndex(1) + 1, 1, TEST_BOOLEANS[2], true, true, true);
        assertLookup(RegionedColumnSource.getLastElementIndex(1) - 1, 1, TEST_BOOLEANS[3], true, true, true);

        assertLookup(RegionedColumnSource.getFirstElementIndex(4) + 2, 4, TEST_BOOLEANS[4], true, true, true);
        assertLookup(RegionedColumnSource.getLastElementIndex(4) - 2, 4, TEST_BOOLEANS[5], true, true, true);

        assertLookup(RegionedColumnSource.getFirstElementIndex(8) + 3, 8, TEST_BOOLEANS[6], true, true, true);
        assertLookup(RegionedColumnSource.getLastElementIndex(8) - 3, 8, TEST_BOOLEANS[7], true, true, true);

        assertLookup(RegionedColumnSource.getFirstElementIndex(9) + 4, 9, TEST_BOOLEANS[8], true, true, true);
        assertLookup(RegionedColumnSource.getLastElementIndex(9) - 4, 9, TEST_BOOLEANS[9], true, true, true);
    }

    @Test
    public void testGetByteReinterpreted() {
        fillRegions();

        assertLookup(0L, 0, TEST_BOOLEANS[0], false, false, true);
        assertLookup(RegionedColumnSource.getLastElementIndex(0), 0, TEST_BOOLEANS[1], false, false, true);

        assertLookup(RegionedColumnSource.getFirstElementIndex(1) + 1, 1, TEST_BOOLEANS[2], false, false, true);
        assertLookup(RegionedColumnSource.getLastElementIndex(1) - 1, 1, TEST_BOOLEANS[3], false, false, true);

        assertLookup(RegionedColumnSource.getFirstElementIndex(4) + 2, 4, TEST_BOOLEANS[4], false, false, true);
        assertLookup(RegionedColumnSource.getLastElementIndex(4) - 2, 4, TEST_BOOLEANS[5], false, false, true);

        assertLookup(RegionedColumnSource.getFirstElementIndex(8) + 3, 8, TEST_BOOLEANS[6], false, false, true);
        assertLookup(RegionedColumnSource.getLastElementIndex(8) - 3, 8, TEST_BOOLEANS[7], false, false, true);

        assertLookup(RegionedColumnSource.getFirstElementIndex(9) + 4, 9, TEST_BOOLEANS[8], false, false, true);
        assertLookup(RegionedColumnSource.getLastElementIndex(9) - 4, 9, TEST_BOOLEANS[9], false, false, true);
    }

    @Test
    public void testGetBytePrevReinterpreted() {
        fillRegions();

        assertLookup(0L, 0, TEST_BOOLEANS[0], true, true);
        assertLookup(RegionedColumnSource.getLastElementIndex(0), 0, TEST_BOOLEANS[1], true, false, true);

        assertLookup(RegionedColumnSource.getFirstElementIndex(1) + 1, 1, TEST_BOOLEANS[2], true, false, true);
        assertLookup(RegionedColumnSource.getLastElementIndex(1) - 1, 1, TEST_BOOLEANS[3], true, false, true);

        assertLookup(RegionedColumnSource.getFirstElementIndex(4) + 2, 4, TEST_BOOLEANS[4], true, false, true);
        assertLookup(RegionedColumnSource.getLastElementIndex(4) - 2, 4, TEST_BOOLEANS[5], true, false, true);

        assertLookup(RegionedColumnSource.getFirstElementIndex(8) + 3, 8, TEST_BOOLEANS[6], true, false, true);
        assertLookup(RegionedColumnSource.getLastElementIndex(8) - 3, 8, TEST_BOOLEANS[7], true, false, true);

        assertLookup(RegionedColumnSource.getFirstElementIndex(9) + 4, 9, TEST_BOOLEANS[8], true, false, true);
        assertLookup(RegionedColumnSource.getLastElementIndex(9) - 4, 9, TEST_BOOLEANS[9], true, false, true);
    }
}

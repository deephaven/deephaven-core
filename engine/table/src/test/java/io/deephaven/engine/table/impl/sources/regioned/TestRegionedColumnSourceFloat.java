/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit TestRegionedColumnSourceChar and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl.sources.regioned;

import io.deephaven.chunk.attributes.Values;

import static io.deephaven.util.QueryConstants.NULL_FLOAT;

/**
 * Test class for {@link RegionedColumnSourceFloat}.
 */
@SuppressWarnings("JUnit4AnnotatedMethodInJUnit3TestCase")
public class TestRegionedColumnSourceFloat extends TstRegionedColumnSourcePrimitive<Float, Values, ColumnRegionFloat<Values>> {

    public TestRegionedColumnSourceFloat() {
        super(ColumnRegionFloat.class);
    }

    @SuppressWarnings("AutoBoxing")
    private void assertLookup(final long elementIndex,
                              final int expectedRegionIndex,
                              final float output,
                              final boolean prev,
                              final boolean boxed) {
        checking(new Expectations() {{
            oneOf(cr[expectedRegionIndex]).getFloat(elementIndex);
            will(returnValue(output));
        }});
        if (boxed) {
            assertEquals(output == NULL_FLOAT ? null : output, prev ? SUT.getPrev(elementIndex) : SUT.get(elementIndex));
        } else {
            assertEquals(output, prev ? SUT.getPrevFloat(elementIndex) : SUT.getFloat(elementIndex));
        }
        assertIsSatisfied();
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();

        SUT = new RegionedColumnSourceFloat.AsValues();
        assertEquals(float.class, SUT.getType());
    }

    @Override
    public void testGet() {
        fillRegions();

        assertLookup(0L, 0, TEST_FLOATS[0], false, true);
        assertLookup(RegionedColumnSource.getFirstRowKey(0), 0, TEST_FLOATS[1], false, true);

        assertLookup(RegionedColumnSource.getFirstRowKey(1) + 1, 1, TEST_FLOATS[2], false, true);
        assertLookup(RegionedColumnSource.getLastRowKey(1) - 1, 1, TEST_FLOATS[3], false, true);

        assertLookup(RegionedColumnSource.getFirstRowKey(4) + 2, 4, TEST_FLOATS[4], false, true);
        assertLookup(RegionedColumnSource.getLastRowKey(4) - 2, 4, TEST_FLOATS[5], false, true);

        assertLookup(RegionedColumnSource.getFirstRowKey(8) + 3, 8, TEST_FLOATS[6], false, true);
        assertLookup(RegionedColumnSource.getLastRowKey(8) - 3, 8, TEST_FLOATS[7], false, true);

        assertLookup(RegionedColumnSource.getFirstRowKey(9) + 4, 9, TEST_FLOATS[8], false, true);
        assertLookup(RegionedColumnSource.getLastRowKey(9) - 4, 9, TEST_FLOATS[9], false, true);
    }

    @Override
    public void testGetPrev() {
        fillRegions();

        assertLookup(0L, 0, TEST_FLOATS[0], true, true);
        assertLookup(RegionedColumnSource.getLastRowKey(0), 0, TEST_FLOATS[1], true, true);

        assertLookup(RegionedColumnSource.getFirstRowKey(1) + 1, 1, TEST_FLOATS[2], true, true);
        assertLookup(RegionedColumnSource.getLastRowKey(1) - 1, 1, TEST_FLOATS[3], true, true);

        assertLookup(RegionedColumnSource.getFirstRowKey(4) + 2, 4, TEST_FLOATS[4], true, true);
        assertLookup(RegionedColumnSource.getLastRowKey(4) - 2, 4, TEST_FLOATS[5], true, true);

        assertLookup(RegionedColumnSource.getFirstRowKey(8) + 3, 8, TEST_FLOATS[6], true, true);
        assertLookup(RegionedColumnSource.getLastRowKey(8) - 3, 8, TEST_FLOATS[7], true, true);

        assertLookup(RegionedColumnSource.getFirstRowKey(9) + 4, 9, TEST_FLOATS[8], true, true);
        assertLookup(RegionedColumnSource.getLastRowKey(9) - 4, 9, TEST_FLOATS[9], true, true);
    }

    @Override
    public void testGetFloat() {
        fillRegions();

        assertLookup(0L, 0, TEST_FLOATS[0], false, false);
        assertLookup(RegionedColumnSource.getLastRowKey(0), 0, TEST_FLOATS[1], false, false);

        assertLookup(RegionedColumnSource.getFirstRowKey(1) + 1, 1, TEST_FLOATS[2], false, false);
        assertLookup(RegionedColumnSource.getLastRowKey(1) - 1, 1, TEST_FLOATS[3], false, false);

        assertLookup(RegionedColumnSource.getFirstRowKey(4) + 2, 4, TEST_FLOATS[4], false, false);
        assertLookup(RegionedColumnSource.getLastRowKey(4) - 2, 4, TEST_FLOATS[5], false, false);

        assertLookup(RegionedColumnSource.getFirstRowKey(8) + 3, 8, TEST_FLOATS[6], false, false);
        assertLookup(RegionedColumnSource.getLastRowKey(8) - 3, 8, TEST_FLOATS[7], false, false);

        assertLookup(RegionedColumnSource.getFirstRowKey(9) + 4, 9, TEST_FLOATS[8], false, false);
        assertLookup(RegionedColumnSource.getLastRowKey(9) - 4, 9, TEST_FLOATS[9], false, false);
    }

    @Override
    public void testGetPrevFloat() {
        fillRegions();

        assertLookup(0L, 0, TEST_FLOATS[0], true, false);
        assertLookup(RegionedColumnSource.getLastRowKey(0), 0, TEST_FLOATS[1], true, false);

        assertLookup(RegionedColumnSource.getFirstRowKey(1) + 1, 1, TEST_FLOATS[2], true, false);
        assertLookup(RegionedColumnSource.getLastRowKey(1) - 1, 1, TEST_FLOATS[3], true, false);

        assertLookup(RegionedColumnSource.getFirstRowKey(4) + 2, 4, TEST_FLOATS[4], true, false);
        assertLookup(RegionedColumnSource.getLastRowKey(4) - 2, 4, TEST_FLOATS[5], true, false);

        assertLookup(RegionedColumnSource.getFirstRowKey(8) + 3, 8, TEST_FLOATS[6], true, false);
        assertLookup(RegionedColumnSource.getLastRowKey(8) - 3, 8, TEST_FLOATS[7], true, false);

        assertLookup(RegionedColumnSource.getFirstRowKey(9) + 4, 9, TEST_FLOATS[8], true, false);
        assertLookup(RegionedColumnSource.getLastRowKey(9) - 4, 9, TEST_FLOATS[9], true, false);
    }
}

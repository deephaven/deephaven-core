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

import static io.deephaven.util.QueryConstants.NULL_DOUBLE;

/**
 * Test class for {@link RegionedColumnSourceDouble}.
 */
@SuppressWarnings("JUnit4AnnotatedMethodInJUnit3TestCase")
public class TestRegionedColumnSourceDouble extends TstRegionedColumnSourcePrimitive<Double, Values, ColumnRegionDouble<Values>> {

    public TestRegionedColumnSourceDouble() {
        super(ColumnRegionDouble.class);
    }

    @SuppressWarnings("AutoBoxing")
    private void assertLookup(final long elementIndex,
                              final int expectedRegionIndex,
                              final double output,
                              final boolean prev,
                              final boolean boxed) {
        checking(new Expectations() {{
            oneOf(cr[expectedRegionIndex]).getDouble(elementIndex);
            will(returnValue(output));
        }});
        if (boxed) {
            assertEquals(output == NULL_DOUBLE ? null : output, prev ? SUT.getPrev(elementIndex) : SUT.get(elementIndex));
        } else {
            assertEquals(output, prev ? SUT.getPrevDouble(elementIndex) : SUT.getDouble(elementIndex));
        }
        assertIsSatisfied();
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();

        SUT = new RegionedColumnSourceDouble.AsValues();
        assertEquals(double.class, SUT.getType());
    }

    @Override
    public void testGet() {
        fillRegions();

        assertLookup(0L, 0, TEST_DOUBLES[0], false, true);
        assertLookup(RegionedColumnSource.getFirstRowKey(0), 0, TEST_DOUBLES[1], false, true);

        assertLookup(RegionedColumnSource.getFirstRowKey(1) + 1, 1, TEST_DOUBLES[2], false, true);
        assertLookup(RegionedColumnSource.getLastRowKey(1) - 1, 1, TEST_DOUBLES[3], false, true);

        assertLookup(RegionedColumnSource.getFirstRowKey(4) + 2, 4, TEST_DOUBLES[4], false, true);
        assertLookup(RegionedColumnSource.getLastRowKey(4) - 2, 4, TEST_DOUBLES[5], false, true);

        assertLookup(RegionedColumnSource.getFirstRowKey(8) + 3, 8, TEST_DOUBLES[6], false, true);
        assertLookup(RegionedColumnSource.getLastRowKey(8) - 3, 8, TEST_DOUBLES[7], false, true);

        assertLookup(RegionedColumnSource.getFirstRowKey(9) + 4, 9, TEST_DOUBLES[8], false, true);
        assertLookup(RegionedColumnSource.getLastRowKey(9) - 4, 9, TEST_DOUBLES[9], false, true);
    }

    @Override
    public void testGetPrev() {
        fillRegions();

        assertLookup(0L, 0, TEST_DOUBLES[0], true, true);
        assertLookup(RegionedColumnSource.getLastRowKey(0), 0, TEST_DOUBLES[1], true, true);

        assertLookup(RegionedColumnSource.getFirstRowKey(1) + 1, 1, TEST_DOUBLES[2], true, true);
        assertLookup(RegionedColumnSource.getLastRowKey(1) - 1, 1, TEST_DOUBLES[3], true, true);

        assertLookup(RegionedColumnSource.getFirstRowKey(4) + 2, 4, TEST_DOUBLES[4], true, true);
        assertLookup(RegionedColumnSource.getLastRowKey(4) - 2, 4, TEST_DOUBLES[5], true, true);

        assertLookup(RegionedColumnSource.getFirstRowKey(8) + 3, 8, TEST_DOUBLES[6], true, true);
        assertLookup(RegionedColumnSource.getLastRowKey(8) - 3, 8, TEST_DOUBLES[7], true, true);

        assertLookup(RegionedColumnSource.getFirstRowKey(9) + 4, 9, TEST_DOUBLES[8], true, true);
        assertLookup(RegionedColumnSource.getLastRowKey(9) - 4, 9, TEST_DOUBLES[9], true, true);
    }

    @Override
    public void testGetDouble() {
        fillRegions();

        assertLookup(0L, 0, TEST_DOUBLES[0], false, false);
        assertLookup(RegionedColumnSource.getLastRowKey(0), 0, TEST_DOUBLES[1], false, false);

        assertLookup(RegionedColumnSource.getFirstRowKey(1) + 1, 1, TEST_DOUBLES[2], false, false);
        assertLookup(RegionedColumnSource.getLastRowKey(1) - 1, 1, TEST_DOUBLES[3], false, false);

        assertLookup(RegionedColumnSource.getFirstRowKey(4) + 2, 4, TEST_DOUBLES[4], false, false);
        assertLookup(RegionedColumnSource.getLastRowKey(4) - 2, 4, TEST_DOUBLES[5], false, false);

        assertLookup(RegionedColumnSource.getFirstRowKey(8) + 3, 8, TEST_DOUBLES[6], false, false);
        assertLookup(RegionedColumnSource.getLastRowKey(8) - 3, 8, TEST_DOUBLES[7], false, false);

        assertLookup(RegionedColumnSource.getFirstRowKey(9) + 4, 9, TEST_DOUBLES[8], false, false);
        assertLookup(RegionedColumnSource.getLastRowKey(9) - 4, 9, TEST_DOUBLES[9], false, false);
    }

    @Override
    public void testGetPrevDouble() {
        fillRegions();

        assertLookup(0L, 0, TEST_DOUBLES[0], true, false);
        assertLookup(RegionedColumnSource.getLastRowKey(0), 0, TEST_DOUBLES[1], true, false);

        assertLookup(RegionedColumnSource.getFirstRowKey(1) + 1, 1, TEST_DOUBLES[2], true, false);
        assertLookup(RegionedColumnSource.getLastRowKey(1) - 1, 1, TEST_DOUBLES[3], true, false);

        assertLookup(RegionedColumnSource.getFirstRowKey(4) + 2, 4, TEST_DOUBLES[4], true, false);
        assertLookup(RegionedColumnSource.getLastRowKey(4) - 2, 4, TEST_DOUBLES[5], true, false);

        assertLookup(RegionedColumnSource.getFirstRowKey(8) + 3, 8, TEST_DOUBLES[6], true, false);
        assertLookup(RegionedColumnSource.getLastRowKey(8) - 3, 8, TEST_DOUBLES[7], true, false);

        assertLookup(RegionedColumnSource.getFirstRowKey(9) + 4, 9, TEST_DOUBLES[8], true, false);
        assertLookup(RegionedColumnSource.getLastRowKey(9) - 4, 9, TEST_DOUBLES[9], true, false);
    }
}

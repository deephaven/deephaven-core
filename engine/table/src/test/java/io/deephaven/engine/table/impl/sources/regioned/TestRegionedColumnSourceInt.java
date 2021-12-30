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

import static io.deephaven.util.QueryConstants.NULL_INT;

/**
 * Test class for {@link RegionedColumnSourceInt}.
 */
@SuppressWarnings("JUnit4AnnotatedMethodInJUnit3TestCase")
public class TestRegionedColumnSourceInt extends TstRegionedColumnSourcePrimitive<Integer, Values, ColumnRegionInt<Values>> {

    public TestRegionedColumnSourceInt() {
        super(ColumnRegionInt.class);
    }

    @SuppressWarnings("AutoBoxing")
    private void assertLookup(final long elementIndex,
                              final int expectedRegionIndex,
                              final int output,
                              final boolean prev,
                              final boolean boxed) {
        checking(new Expectations() {{
            oneOf(cr[expectedRegionIndex]).getInt(elementIndex);
            will(returnValue(output));
        }});
        if (boxed) {
            assertEquals(output == NULL_INT ? null : output, prev ? SUT.getPrev(elementIndex) : SUT.get(elementIndex));
        } else {
            assertEquals(output, prev ? SUT.getPrevInt(elementIndex) : SUT.getInt(elementIndex));
        }
        assertIsSatisfied();
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();

        SUT = new RegionedColumnSourceInt.AsValues();
        assertEquals(int.class, SUT.getType());
    }

    @Override
    public void testGet() {
        fillRegions();

        assertLookup(0L, 0, TEST_INTS[0], false, true);
        assertLookup(RegionedColumnSource.getFirstRowKey(0), 0, TEST_INTS[1], false, true);

        assertLookup(RegionedColumnSource.getFirstRowKey(1) + 1, 1, TEST_INTS[2], false, true);
        assertLookup(RegionedColumnSource.getLastRowKey(1) - 1, 1, TEST_INTS[3], false, true);

        assertLookup(RegionedColumnSource.getFirstRowKey(4) + 2, 4, TEST_INTS[4], false, true);
        assertLookup(RegionedColumnSource.getLastRowKey(4) - 2, 4, TEST_INTS[5], false, true);

        assertLookup(RegionedColumnSource.getFirstRowKey(8) + 3, 8, TEST_INTS[6], false, true);
        assertLookup(RegionedColumnSource.getLastRowKey(8) - 3, 8, TEST_INTS[7], false, true);

        assertLookup(RegionedColumnSource.getFirstRowKey(9) + 4, 9, TEST_INTS[8], false, true);
        assertLookup(RegionedColumnSource.getLastRowKey(9) - 4, 9, TEST_INTS[9], false, true);
    }

    @Override
    public void testGetPrev() {
        fillRegions();

        assertLookup(0L, 0, TEST_INTS[0], true, true);
        assertLookup(RegionedColumnSource.getLastRowKey(0), 0, TEST_INTS[1], true, true);

        assertLookup(RegionedColumnSource.getFirstRowKey(1) + 1, 1, TEST_INTS[2], true, true);
        assertLookup(RegionedColumnSource.getLastRowKey(1) - 1, 1, TEST_INTS[3], true, true);

        assertLookup(RegionedColumnSource.getFirstRowKey(4) + 2, 4, TEST_INTS[4], true, true);
        assertLookup(RegionedColumnSource.getLastRowKey(4) - 2, 4, TEST_INTS[5], true, true);

        assertLookup(RegionedColumnSource.getFirstRowKey(8) + 3, 8, TEST_INTS[6], true, true);
        assertLookup(RegionedColumnSource.getLastRowKey(8) - 3, 8, TEST_INTS[7], true, true);

        assertLookup(RegionedColumnSource.getFirstRowKey(9) + 4, 9, TEST_INTS[8], true, true);
        assertLookup(RegionedColumnSource.getLastRowKey(9) - 4, 9, TEST_INTS[9], true, true);
    }

    @Override
    public void testGetInt() {
        fillRegions();

        assertLookup(0L, 0, TEST_INTS[0], false, false);
        assertLookup(RegionedColumnSource.getLastRowKey(0), 0, TEST_INTS[1], false, false);

        assertLookup(RegionedColumnSource.getFirstRowKey(1) + 1, 1, TEST_INTS[2], false, false);
        assertLookup(RegionedColumnSource.getLastRowKey(1) - 1, 1, TEST_INTS[3], false, false);

        assertLookup(RegionedColumnSource.getFirstRowKey(4) + 2, 4, TEST_INTS[4], false, false);
        assertLookup(RegionedColumnSource.getLastRowKey(4) - 2, 4, TEST_INTS[5], false, false);

        assertLookup(RegionedColumnSource.getFirstRowKey(8) + 3, 8, TEST_INTS[6], false, false);
        assertLookup(RegionedColumnSource.getLastRowKey(8) - 3, 8, TEST_INTS[7], false, false);

        assertLookup(RegionedColumnSource.getFirstRowKey(9) + 4, 9, TEST_INTS[8], false, false);
        assertLookup(RegionedColumnSource.getLastRowKey(9) - 4, 9, TEST_INTS[9], false, false);
    }

    @Override
    public void testGetPrevInt() {
        fillRegions();

        assertLookup(0L, 0, TEST_INTS[0], true, false);
        assertLookup(RegionedColumnSource.getLastRowKey(0), 0, TEST_INTS[1], true, false);

        assertLookup(RegionedColumnSource.getFirstRowKey(1) + 1, 1, TEST_INTS[2], true, false);
        assertLookup(RegionedColumnSource.getLastRowKey(1) - 1, 1, TEST_INTS[3], true, false);

        assertLookup(RegionedColumnSource.getFirstRowKey(4) + 2, 4, TEST_INTS[4], true, false);
        assertLookup(RegionedColumnSource.getLastRowKey(4) - 2, 4, TEST_INTS[5], true, false);

        assertLookup(RegionedColumnSource.getFirstRowKey(8) + 3, 8, TEST_INTS[6], true, false);
        assertLookup(RegionedColumnSource.getLastRowKey(8) - 3, 8, TEST_INTS[7], true, false);

        assertLookup(RegionedColumnSource.getFirstRowKey(9) + 4, 9, TEST_INTS[8], true, false);
        assertLookup(RegionedColumnSource.getLastRowKey(9) - 4, 9, TEST_INTS[9], true, false);
    }
}

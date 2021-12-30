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

import static io.deephaven.util.QueryConstants.NULL_LONG;

/**
 * Test class for {@link RegionedColumnSourceLong}.
 */
@SuppressWarnings("JUnit4AnnotatedMethodInJUnit3TestCase")
public class TestRegionedColumnSourceLong extends TstRegionedColumnSourcePrimitive<Long, Values, ColumnRegionLong<Values>> {

    public TestRegionedColumnSourceLong() {
        super(ColumnRegionLong.class);
    }

    @SuppressWarnings("AutoBoxing")
    private void assertLookup(final long elementIndex,
                              final int expectedRegionIndex,
                              final long output,
                              final boolean prev,
                              final boolean boxed) {
        checking(new Expectations() {{
            oneOf(cr[expectedRegionIndex]).getLong(elementIndex);
            will(returnValue(output));
        }});
        if (boxed) {
            assertEquals(output == NULL_LONG ? null : output, prev ? SUT.getPrev(elementIndex) : SUT.get(elementIndex));
        } else {
            assertEquals(output, prev ? SUT.getPrevLong(elementIndex) : SUT.getLong(elementIndex));
        }
        assertIsSatisfied();
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();

        SUT = new RegionedColumnSourceLong.AsValues();
        assertEquals(long.class, SUT.getType());
    }

    @Override
    public void testGet() {
        fillRegions();

        assertLookup(0L, 0, TEST_LONGS[0], false, true);
        assertLookup(RegionedColumnSource.getFirstRowKey(0), 0, TEST_LONGS[1], false, true);

        assertLookup(RegionedColumnSource.getFirstRowKey(1) + 1, 1, TEST_LONGS[2], false, true);
        assertLookup(RegionedColumnSource.getLastRowKey(1) - 1, 1, TEST_LONGS[3], false, true);

        assertLookup(RegionedColumnSource.getFirstRowKey(4) + 2, 4, TEST_LONGS[4], false, true);
        assertLookup(RegionedColumnSource.getLastRowKey(4) - 2, 4, TEST_LONGS[5], false, true);

        assertLookup(RegionedColumnSource.getFirstRowKey(8) + 3, 8, TEST_LONGS[6], false, true);
        assertLookup(RegionedColumnSource.getLastRowKey(8) - 3, 8, TEST_LONGS[7], false, true);

        assertLookup(RegionedColumnSource.getFirstRowKey(9) + 4, 9, TEST_LONGS[8], false, true);
        assertLookup(RegionedColumnSource.getLastRowKey(9) - 4, 9, TEST_LONGS[9], false, true);
    }

    @Override
    public void testGetPrev() {
        fillRegions();

        assertLookup(0L, 0, TEST_LONGS[0], true, true);
        assertLookup(RegionedColumnSource.getLastRowKey(0), 0, TEST_LONGS[1], true, true);

        assertLookup(RegionedColumnSource.getFirstRowKey(1) + 1, 1, TEST_LONGS[2], true, true);
        assertLookup(RegionedColumnSource.getLastRowKey(1) - 1, 1, TEST_LONGS[3], true, true);

        assertLookup(RegionedColumnSource.getFirstRowKey(4) + 2, 4, TEST_LONGS[4], true, true);
        assertLookup(RegionedColumnSource.getLastRowKey(4) - 2, 4, TEST_LONGS[5], true, true);

        assertLookup(RegionedColumnSource.getFirstRowKey(8) + 3, 8, TEST_LONGS[6], true, true);
        assertLookup(RegionedColumnSource.getLastRowKey(8) - 3, 8, TEST_LONGS[7], true, true);

        assertLookup(RegionedColumnSource.getFirstRowKey(9) + 4, 9, TEST_LONGS[8], true, true);
        assertLookup(RegionedColumnSource.getLastRowKey(9) - 4, 9, TEST_LONGS[9], true, true);
    }

    @Override
    public void testGetLong() {
        fillRegions();

        assertLookup(0L, 0, TEST_LONGS[0], false, false);
        assertLookup(RegionedColumnSource.getLastRowKey(0), 0, TEST_LONGS[1], false, false);

        assertLookup(RegionedColumnSource.getFirstRowKey(1) + 1, 1, TEST_LONGS[2], false, false);
        assertLookup(RegionedColumnSource.getLastRowKey(1) - 1, 1, TEST_LONGS[3], false, false);

        assertLookup(RegionedColumnSource.getFirstRowKey(4) + 2, 4, TEST_LONGS[4], false, false);
        assertLookup(RegionedColumnSource.getLastRowKey(4) - 2, 4, TEST_LONGS[5], false, false);

        assertLookup(RegionedColumnSource.getFirstRowKey(8) + 3, 8, TEST_LONGS[6], false, false);
        assertLookup(RegionedColumnSource.getLastRowKey(8) - 3, 8, TEST_LONGS[7], false, false);

        assertLookup(RegionedColumnSource.getFirstRowKey(9) + 4, 9, TEST_LONGS[8], false, false);
        assertLookup(RegionedColumnSource.getLastRowKey(9) - 4, 9, TEST_LONGS[9], false, false);
    }

    @Override
    public void testGetPrevLong() {
        fillRegions();

        assertLookup(0L, 0, TEST_LONGS[0], true, false);
        assertLookup(RegionedColumnSource.getLastRowKey(0), 0, TEST_LONGS[1], true, false);

        assertLookup(RegionedColumnSource.getFirstRowKey(1) + 1, 1, TEST_LONGS[2], true, false);
        assertLookup(RegionedColumnSource.getLastRowKey(1) - 1, 1, TEST_LONGS[3], true, false);

        assertLookup(RegionedColumnSource.getFirstRowKey(4) + 2, 4, TEST_LONGS[4], true, false);
        assertLookup(RegionedColumnSource.getLastRowKey(4) - 2, 4, TEST_LONGS[5], true, false);

        assertLookup(RegionedColumnSource.getFirstRowKey(8) + 3, 8, TEST_LONGS[6], true, false);
        assertLookup(RegionedColumnSource.getLastRowKey(8) - 3, 8, TEST_LONGS[7], true, false);

        assertLookup(RegionedColumnSource.getFirstRowKey(9) + 4, 9, TEST_LONGS[8], true, false);
        assertLookup(RegionedColumnSource.getLastRowKey(9) - 4, 9, TEST_LONGS[9], true, false);
    }
}

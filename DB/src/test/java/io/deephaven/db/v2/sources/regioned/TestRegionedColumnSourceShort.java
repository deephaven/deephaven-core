/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit TestRegionedColumnSourceChar and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.sources.regioned;

import io.deephaven.db.v2.sources.chunk.Attributes;

import static io.deephaven.util.QueryConstants.NULL_SHORT;

/**
 * Test class for {@link RegionedColumnSourceShort}.
 */
@SuppressWarnings("JUnit4AnnotatedMethodInJUnit3TestCase")
public class TestRegionedColumnSourceShort extends TstRegionedColumnSourcePrimitive<Short, Attributes.Values, ColumnRegionShort<Attributes.Values>> {

    public TestRegionedColumnSourceShort() {
        super(ColumnRegionShort.class);
    }

    @SuppressWarnings("AutoBoxing")
    private void assertLookup(final long elementIndex,
                              final int expectedRegionIndex,
                              final short output,
                              final boolean prev,
                              final boolean boxed) {
        checking(new Expectations() {{
            oneOf(cr[expectedRegionIndex]).getShort(elementIndex);
            will(returnValue(output));
        }});
        if (boxed) {
            assertEquals(output == NULL_SHORT ? null : output, prev ? SUT.getPrev(elementIndex) : SUT.get(elementIndex));
        } else {
            assertEquals(output, prev ? SUT.getPrevShort(elementIndex) : SUT.getShort(elementIndex));
        }
        assertIsSatisfied();
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();

        SUT = new RegionedColumnSourceShort.AsValues();
        assertEquals(short.class, SUT.getType());
    }

    @Override
    public void testGet() {
        fillRegions();

        assertLookup(0L, 0, TEST_SHORTS[0], false, true);
        assertLookup(RegionedColumnSource.getFirstElementIndex(0), 0, TEST_SHORTS[1], false, true);

        assertLookup(RegionedColumnSource.getFirstElementIndex(1) + 1, 1, TEST_SHORTS[2], false, true);
        assertLookup(RegionedColumnSource.getLastElementIndex(1) - 1, 1, TEST_SHORTS[3], false, true);

        assertLookup(RegionedColumnSource.getFirstElementIndex(4) + 2, 4, TEST_SHORTS[4], false, true);
        assertLookup(RegionedColumnSource.getLastElementIndex(4) - 2, 4, TEST_SHORTS[5], false, true);

        assertLookup(RegionedColumnSource.getFirstElementIndex(8) + 3, 8, TEST_SHORTS[6], false, true);
        assertLookup(RegionedColumnSource.getLastElementIndex(8) - 3, 8, TEST_SHORTS[7], false, true);

        assertLookup(RegionedColumnSource.getFirstElementIndex(9) + 4, 9, TEST_SHORTS[8], false, true);
        assertLookup(RegionedColumnSource.getLastElementIndex(9) - 4, 9, TEST_SHORTS[9], false, true);
    }

    @Override
    public void testGetPrev() {
        fillRegions();

        assertLookup(0L, 0, TEST_SHORTS[0], true, true);
        assertLookup(RegionedColumnSource.getLastElementIndex(0), 0, TEST_SHORTS[1], true, true);

        assertLookup(RegionedColumnSource.getFirstElementIndex(1) + 1, 1, TEST_SHORTS[2], true, true);
        assertLookup(RegionedColumnSource.getLastElementIndex(1) - 1, 1, TEST_SHORTS[3], true, true);

        assertLookup(RegionedColumnSource.getFirstElementIndex(4) + 2, 4, TEST_SHORTS[4], true, true);
        assertLookup(RegionedColumnSource.getLastElementIndex(4) - 2, 4, TEST_SHORTS[5], true, true);

        assertLookup(RegionedColumnSource.getFirstElementIndex(8) + 3, 8, TEST_SHORTS[6], true, true);
        assertLookup(RegionedColumnSource.getLastElementIndex(8) - 3, 8, TEST_SHORTS[7], true, true);

        assertLookup(RegionedColumnSource.getFirstElementIndex(9) + 4, 9, TEST_SHORTS[8], true, true);
        assertLookup(RegionedColumnSource.getLastElementIndex(9) - 4, 9, TEST_SHORTS[9], true, true);
    }

    @Override
    public void testGetShort() {
        fillRegions();

        assertLookup(0L, 0, TEST_SHORTS[0], false, false);
        assertLookup(RegionedColumnSource.getLastElementIndex(0), 0, TEST_SHORTS[1], false, false);

        assertLookup(RegionedColumnSource.getFirstElementIndex(1) + 1, 1, TEST_SHORTS[2], false, false);
        assertLookup(RegionedColumnSource.getLastElementIndex(1) - 1, 1, TEST_SHORTS[3], false, false);

        assertLookup(RegionedColumnSource.getFirstElementIndex(4) + 2, 4, TEST_SHORTS[4], false, false);
        assertLookup(RegionedColumnSource.getLastElementIndex(4) - 2, 4, TEST_SHORTS[5], false, false);

        assertLookup(RegionedColumnSource.getFirstElementIndex(8) + 3, 8, TEST_SHORTS[6], false, false);
        assertLookup(RegionedColumnSource.getLastElementIndex(8) - 3, 8, TEST_SHORTS[7], false, false);

        assertLookup(RegionedColumnSource.getFirstElementIndex(9) + 4, 9, TEST_SHORTS[8], false, false);
        assertLookup(RegionedColumnSource.getLastElementIndex(9) - 4, 9, TEST_SHORTS[9], false, false);
    }

    @Override
    public void testGetPrevShort() {
        fillRegions();

        assertLookup(0L, 0, TEST_SHORTS[0], true, false);
        assertLookup(RegionedColumnSource.getLastElementIndex(0), 0, TEST_SHORTS[1], true, false);

        assertLookup(RegionedColumnSource.getFirstElementIndex(1) + 1, 1, TEST_SHORTS[2], true, false);
        assertLookup(RegionedColumnSource.getLastElementIndex(1) - 1, 1, TEST_SHORTS[3], true, false);

        assertLookup(RegionedColumnSource.getFirstElementIndex(4) + 2, 4, TEST_SHORTS[4], true, false);
        assertLookup(RegionedColumnSource.getLastElementIndex(4) - 2, 4, TEST_SHORTS[5], true, false);

        assertLookup(RegionedColumnSource.getFirstElementIndex(8) + 3, 8, TEST_SHORTS[6], true, false);
        assertLookup(RegionedColumnSource.getLastElementIndex(8) - 3, 8, TEST_SHORTS[7], true, false);

        assertLookup(RegionedColumnSource.getFirstElementIndex(9) + 4, 9, TEST_SHORTS[8], true, false);
        assertLookup(RegionedColumnSource.getLastElementIndex(9) - 4, 9, TEST_SHORTS[9], true, false);
    }
}

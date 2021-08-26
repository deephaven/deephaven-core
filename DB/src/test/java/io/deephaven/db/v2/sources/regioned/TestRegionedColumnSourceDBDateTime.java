/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.sources.regioned;

import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.tables.utils.DBTimeUtils;
import io.deephaven.db.tables.utils.DBTimeZone;
import io.deephaven.util.QueryConstants;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.chunk.Attributes;
import org.junit.Test;

@SuppressWarnings("JUnit4AnnotatedMethodInJUnit3TestCase")
public class TestRegionedColumnSourceDBDateTime
        extends TstRegionedColumnSourceReferencing<DBDateTime, Attributes.Values, ColumnRegionLong<Attributes.Values>> {

    public TestRegionedColumnSourceDBDateTime() {
        super(ColumnRegionLong.class);
    }

    private static final DBDateTime[] TEST_DATES = new DBDateTime[] {
            null,
            DBTimeUtils.currentTime(),
            DBTimeUtils.dateAtMidnight(DBTimeUtils.currentTime(), DBTimeZone.TZ_NY),
            DBTimeUtils.convertDateTime("2013-01-15T12:19:32.000 NY"),
            DBTimeUtils.convertDateTime("2013-01-15T09:30:00.000 NY"),
            DBTimeUtils.convertDateTime("2013-01-15T16:00:00.000 NY"),
            DBTimeUtils.convertDateTime("2013-01-15T16:15:00.000 NY"),
            DBTimeUtils.convertDateTime("2000-01-01T00:00:00.000 NY"),
            DBTimeUtils.convertDateTime("1999-12-31T23:59:59.000 NY"),
            DBTimeUtils.convertDateTime("1981-02-22T19:50:00.000 NY")
    };

    private ColumnSource<Long> SUT_AS_LONG;

    private void assertLookup(final long elementIndex,
            final int expectedRegionIndex,
            final DBDateTime output,
            final boolean prev,
            final boolean reinterpreted) {
        checking(new Expectations() {
            {
                oneOf(cr[expectedRegionIndex]).getReferencedRegion();
                will(returnValue(cr_n[expectedRegionIndex]));
                oneOf(cr_n[expectedRegionIndex]).getLong(elementIndex);
                will(returnValue(output == null ? QueryConstants.NULL_LONG : output.getNanos()));
            }
        });
        if (reinterpreted) {
            assertEquals(output == null ? QueryConstants.NULL_LONG : output.getNanos(),
                    prev ? SUT_AS_LONG.getPrevLong(elementIndex) : SUT_AS_LONG.getLong(elementIndex));
        } else {
            assertEquals(output, prev ? SUT.getPrev(elementIndex) : SUT.get(elementIndex));
        }
        assertIsSatisfied();
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();

        SUT = new RegionedColumnSourceDBDateTime();
        assertEquals(DBDateTime.class, SUT.getType());
        SUT_AS_LONG = SUT.reinterpret(long.class);
        assertEquals(long.class, SUT_AS_LONG.getType());
        assertEquals(SUT, SUT_AS_LONG.reinterpret(DBDateTime.class));
    }

    @Override
    public void testGet() {
        fillRegions();

        // noinspection ConstantConditions
        assertLookup(0L, 0, TEST_DATES[0], false, false);
        assertLookup(RegionedColumnSource.getLastElementIndex(0), 0, TEST_DATES[1], false, false);

        assertLookup(RegionedColumnSource.getFirstElementIndex(1) + 1, 1, TEST_DATES[2], false, false);
        assertLookup(RegionedColumnSource.getLastElementIndex(1) - 1, 1, TEST_DATES[3], false, false);

        assertLookup(RegionedColumnSource.getFirstElementIndex(4) + 2, 4, TEST_DATES[4], false, false);
        assertLookup(RegionedColumnSource.getLastElementIndex(4) - 2, 4, TEST_DATES[5], false, false);

        assertLookup(RegionedColumnSource.getFirstElementIndex(8) + 3, 8, TEST_DATES[6], false, false);
        assertLookup(RegionedColumnSource.getLastElementIndex(8) - 3, 8, TEST_DATES[7], false, false);

        assertLookup(RegionedColumnSource.getFirstElementIndex(9) + 4, 9, TEST_DATES[8], false, false);
        assertLookup(RegionedColumnSource.getLastElementIndex(9) - 4, 9, TEST_DATES[9], false, false);
    }

    @Override
    public void testGetPrev() {
        fillRegions();

        // noinspection ConstantConditions
        assertLookup(0L, 0, TEST_DATES[0], true, false);
        assertLookup(RegionedColumnSource.getLastElementIndex(0), 0, TEST_DATES[1], true, false);

        assertLookup(RegionedColumnSource.getFirstElementIndex(1) + 1, 1, TEST_DATES[2], true, false);
        assertLookup(RegionedColumnSource.getLastElementIndex(1) - 1, 1, TEST_DATES[3], true, false);

        assertLookup(RegionedColumnSource.getFirstElementIndex(4) + 2, 4, TEST_DATES[4], true, false);
        assertLookup(RegionedColumnSource.getLastElementIndex(4) - 2, 4, TEST_DATES[5], true, false);

        assertLookup(RegionedColumnSource.getFirstElementIndex(8) + 3, 8, TEST_DATES[6], true, false);
        assertLookup(RegionedColumnSource.getLastElementIndex(8) - 3, 8, TEST_DATES[7], true, false);

        assertLookup(RegionedColumnSource.getFirstElementIndex(9) + 4, 9, TEST_DATES[8], true, false);
        assertLookup(RegionedColumnSource.getLastElementIndex(9) - 4, 9, TEST_DATES[9], true, false);
    }

    @Test
    public void testGetReinterpreted() {
        fillRegions();

        // noinspection ConstantConditions
        assertLookup(0L, 0, TEST_DATES[0], false, true);
        assertLookup(RegionedColumnSource.getLastElementIndex(0), 0, TEST_DATES[1], false, true);

        assertLookup(RegionedColumnSource.getFirstElementIndex(1) + 1, 1, TEST_DATES[2], false, true);
        assertLookup(RegionedColumnSource.getLastElementIndex(1) - 1, 1, TEST_DATES[3], false, true);

        assertLookup(RegionedColumnSource.getFirstElementIndex(4) + 2, 4, TEST_DATES[4], false, true);
        assertLookup(RegionedColumnSource.getLastElementIndex(4) - 2, 4, TEST_DATES[5], false, true);

        assertLookup(RegionedColumnSource.getFirstElementIndex(8) + 3, 8, TEST_DATES[6], false, true);
        assertLookup(RegionedColumnSource.getLastElementIndex(8) - 3, 8, TEST_DATES[7], false, true);

        assertLookup(RegionedColumnSource.getFirstElementIndex(9) + 4, 9, TEST_DATES[8], false, true);
        assertLookup(RegionedColumnSource.getLastElementIndex(9) - 4, 9, TEST_DATES[9], false, true);
    }

    @Test
    public void testGetPrevReinterpreted() {
        fillRegions();

        // noinspection ConstantConditions
        assertLookup(0L, 0, TEST_DATES[0], true, true);
        assertLookup(RegionedColumnSource.getLastElementIndex(0), 0, TEST_DATES[1], true, true);

        assertLookup(RegionedColumnSource.getFirstElementIndex(1) + 1, 1, TEST_DATES[2], true, true);
        assertLookup(RegionedColumnSource.getLastElementIndex(1) - 1, 1, TEST_DATES[3], true, true);

        assertLookup(RegionedColumnSource.getFirstElementIndex(4) + 2, 4, TEST_DATES[4], true, true);
        assertLookup(RegionedColumnSource.getLastElementIndex(4) - 2, 4, TEST_DATES[5], true, true);

        assertLookup(RegionedColumnSource.getFirstElementIndex(8) + 3, 8, TEST_DATES[6], true, true);
        assertLookup(RegionedColumnSource.getLastElementIndex(8) - 3, 8, TEST_DATES[7], true, true);

        assertLookup(RegionedColumnSource.getFirstElementIndex(9) + 4, 9, TEST_DATES[8], true, true);
        assertLookup(RegionedColumnSource.getLastElementIndex(9) - 4, 9, TEST_DATES[9], true, true);
    }
}

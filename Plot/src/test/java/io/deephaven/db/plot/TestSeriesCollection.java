package io.deephaven.db.plot;

import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.db.plot.datasets.category.CategoryDataSeriesInternal;
import io.deephaven.db.plot.datasets.multiseries.MultiSeriesInternal;
import io.deephaven.db.plot.datasets.xy.XYDataSeriesInternal;

public class TestSeriesCollection extends BaseArrayTestCase {

    public void testSeriesCollection() {
        final SeriesCollection sc = new SeriesCollection(null);

        assertEquals(0, sc.getSeriesDescriptions().size());

        final CategoryDataSeriesInternal s1 = mock(CategoryDataSeriesInternal.class);
        final MultiSeriesInternal s2 = mock(MultiSeriesInternal.class);
        final XYDataSeriesInternal s3 = mock(XYDataSeriesInternal.class);

        checking(new Expectations() {
            {
                atLeast(1).of(s1).name();
                will(returnValue("S1"));
                atLeast(1).of(s1).id();
                will(returnValue(1));
                atLeast(1).of(s2).name();
                will(returnValue("S2"));
                atLeast(1).of(s2).id();
                will(returnValue(2));
                atLeast(1).of(s3).name();
                will(returnValue("S2"));
                atLeast(0).of(s3).id();
                will(returnValue(3));
            }
        });


        sc.add(SeriesCollection.SeriesType.CATEGORY, false, s1);
        assertEquals(1, sc.getSeriesDescriptions().size());
        assertSeriesDescription(sc.getSeriesDescriptions().get("S1"),
            SeriesCollection.SeriesType.CATEGORY, false, s1);

        sc.add(SeriesCollection.SeriesType.XY, true, s2);
        assertEquals(2, sc.getSeriesDescriptions().size());
        assertSeriesDescription(sc.getSeriesDescriptions().get("S2"),
            SeriesCollection.SeriesType.XY, true, s2);

        try {
            sc.add(SeriesCollection.SeriesType.XY, true, s3);
            fail("Should throw an exception for multiple series with the same name.");
        } catch (UnsupportedOperationException e) {
            // pass
        }

        assertEquals(s1, sc.series(1));
        assertEquals(s2, sc.series(2));
        assertEquals(s1, sc.series("S1"));
        assertEquals(s2, sc.series("S2"));

        sc.remove("S2");
        assertEquals(1, sc.getSeriesDescriptions().size());
        assertTrue(sc.getSeriesDescriptions().containsKey("S1"));
        assertEquals(s1, sc.series(1));
        assertNull(sc.series(2));
    }

    private void assertSeriesDescription(final SeriesCollection.SeriesDescription sd,
        final SeriesCollection.SeriesType type, final boolean isMultiSeries,
        final SeriesInternal series) {
        assertNotNull(sd);
        assertEquals(type, sd.getType());
        assertEquals(isMultiSeries, sd.isMultiSeries());
        assertEquals(series, sd.getSeries());
    }

    public void testNextId() {
        final SeriesCollection sc = new SeriesCollection(null);
        assertEquals(0, sc.nextId());
        assertEquals(1, sc.nextId());
        assertEquals(2, sc.nextId());
    }

    public void testCopy() {
        final SeriesCollection sc = new SeriesCollection(null);

        final CategoryDataSeriesInternal s1 = mock(CategoryDataSeriesInternal.class, "S1");
        final CategoryDataSeriesInternal s1copy = mock(CategoryDataSeriesInternal.class, "S1C");
        final MultiSeriesInternal s2 = mock(MultiSeriesInternal.class, "S2");
        final MultiSeriesInternal s2copy = mock(MultiSeriesInternal.class, "S2C");
        final AxesImpl axes = mock(AxesImpl.class);

        checking(new Expectations() {
            {
                atLeast(1).of(s1).name();
                will(returnValue("S1"));
                atLeast(1).of(s1).id();
                will(returnValue(1));
                atLeast(1).of(s2).name();
                will(returnValue("S2"));
                atLeast(1).of(s2).id();
                will(returnValue(2));

                atLeast(1).of(s1).copy(axes);
                will(returnValue(s1copy));
                atLeast(1).of(s2).copy(axes);
                will(returnValue(s2copy));

                atLeast(1).of(s1copy).name();
                will(returnValue("S1C"));
                atLeast(1).of(s1copy).id();
                will(returnValue(11));
                atLeast(1).of(s2copy).name();
                will(returnValue("S2C"));
                atLeast(1).of(s2copy).id();
                will(returnValue(22));
                // noinspection ResultOfMethodCallIgnored
                atLeast(1).of(axes).getPlotInfo();
            }
        });


        sc.add(SeriesCollection.SeriesType.CATEGORY, false, s1);
        sc.add(SeriesCollection.SeriesType.XY, true, s2);

        final SeriesCollection copy = sc.copy(axes);

        assertNotNull(copy);
        assertEquals(2, copy.getSeriesDescriptions().size());
        assertSeriesDescription(copy.getSeriesDescriptions().get("S1C"),
            SeriesCollection.SeriesType.CATEGORY, false, s1copy);
        assertSeriesDescription(copy.getSeriesDescriptions().get("S2C"),
            SeriesCollection.SeriesType.XY, true, s2copy);
    }

}

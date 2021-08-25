/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.plot.datasets.category;

import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.db.plot.*;
import io.deephaven.db.plot.datasets.data.IndexableDataArray;
import io.deephaven.db.plot.datasets.data.IndexableNumericDataArrayInt;
import io.deephaven.db.plot.util.PlotUtils;
import io.deephaven.db.plot.util.functions.ClosureFunction;
import io.deephaven.gui.color.Color;
import io.deephaven.gui.shape.NamedShape;
import groovy.lang.Closure;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class TestAbstractCategoryDataSeries extends BaseArrayTestCase {
    private static final String[] categories = {"A", "B", "C"};
    private static final int[] values = {1, 2, 3};
    private final Color c1 = new Color(0, 0, 0);
    private final Color c2 = new Color(100, 100, 100);
    private final Color c3 = new Color(255, 255, 255);
    private final Color[] colors = {c1, c2, c3};
    private static String name = "test";

    private static class TestCat extends CategoryDataSeriesMap {

        TestCat() {
            this(new BaseFigureImpl().newChart().newAxes());
        }

        TestCat(AxesImpl axes) {
            super(axes, 1, name, new IndexableDataArray<>(categories, null),
                    new IndexableNumericDataArrayInt(values, null));
            assertEquals(name(), "test");
        }
    }

    public void testVisibility() {
        final TestCat data = new TestCat();

        assertNull(data.getLinesVisible());
        data.linesVisible(false);
        assertFalse(data.getLinesVisible());

        assertNull(data.getPointsVisible());
        data.pointsVisible(false);
        assertFalse(data.getPointsVisible());
    }

    public void testLineStyle() {
        final TestCat data = new TestCat();
        final LineStyle style = LineStyle.lineStyle(0.5);

        assertNull(data.getLineStyle());
        data.lineStyle(style);
        assertEquals(data.getLineStyle(), style);
    }

    public void testLineColor() {
        final ChartImpl chart = new BaseFigureImpl().newChart();
        final TestCat data = new TestCat(chart.newAxes());
        final Color color = new Color(0, 0, 0);
        assertNull(data.getLineColor());
        data.lineColor(color);
        assertEquals(data.getLineColor(), color);

        data.lineColor(1);
        assertEquals(data.getLineColor(), PlotUtils.intToColor(1));
    }

    public void testErrorBarColor() {
        final ChartImpl chart = new BaseFigureImpl().newChart();
        final TestCat data = new TestCat(chart.newAxes());
        final Color color = new Color(0, 0, 0);
        assertNull(data.getErrorBarColor());
        data.errorBarColor(color);
        assertEquals(data.getErrorBarColor(), color);

        data.errorBarColor(1);
        assertEquals(data.getErrorBarColor(), PlotUtils.intToColor(1));
    }

    public void testPointSize() {
        final TestCat data = new TestCat();

        data.pointSize(1);
        assertEquals(data.getPointSize(categories[0]), 1.0);

        data.pointSize(2.0);
        assertEquals(data.getPointSize(categories[1]), 2.0);

        data.pointSize(3L);
        assertEquals(data.getPointSize(categories[2]), 3.0);

        data.pointSize(new AtomicInteger(4));
        assertEquals(data.getPointSize(categories[2]), 4.0);

        data.pointSize(categories[0], 1);
        assertEquals(data.getPointSize(categories[0]), 1.0);

        data.pointSize(categories[1], 2.0);
        assertEquals(data.getPointSize(categories[1]), 2.0);

        data.pointSize(categories[2], 3L);
        assertEquals(data.getPointSize(categories[2]), 3.0);

        data.pointSize(categories[0], new AtomicInteger(4));
        assertEquals(data.getPointSize(categories[0]), 4.0);

        int[] sizes = {1, 2, 3};
        data.pointSize(categories, sizes);
        for (int i = 0; i < sizes.length; i++) {
            assertEquals(data.getPointSize(categories[i]), (double) sizes[i]);
        }

        long[] lsizes = {1, 2, 3};
        data.pointSize(categories, lsizes);
        for (int i = 0; i < lsizes.length; i++) {
            assertEquals(data.getPointSize(categories[i]), (double) lsizes[i]);
        }

        double[] dsizes = {1, 2, 3};
        data.pointSize(categories, dsizes);
        for (int i = 0; i < sizes.length; i++) {
            assertEquals(data.getPointSize(categories[i]), dsizes[i]);
        }

        AtomicInteger[] asizes = {new AtomicInteger(1), new AtomicInteger(2), new AtomicInteger(3)};
        data.pointSize(categories, asizes);
        for (int i = 0; i < asizes.length; i++) {
            assertEquals(data.getPointSize(categories[i]), asizes[i].doubleValue());
        }

        Map<String, Number> map = new HashMap<>();
        for (int i = 0; i < categories.length; i++) {
            map.put(categories[i], values[i]);
        }
        data.pointSize(map);
        for (int i = 0; i < categories.length; i++) {
            assertEquals(data.getPointSize(categories[i]), (double) values[i]);
        }

        assertEquals(0, data.chart().figure().getFigureFunctionList().size());

        data.pointSize(s -> 1.0);

        for (Comparable category : categories) {
            assertEquals(data.getPointSize(category), 1.0);
        }


        Closure<Double> op = new Closure<Double>(null) {
            @Override
            public void setResolveStrategy(int resolveStrategy) {
                super.setResolveStrategy(resolveStrategy);
            }

            @Override
            public Double call(Object arguments) {
                return 2.0;
            }
        };
        data.pointSize(op);
        data.pointSize(s -> 2.0);
        for (String category : categories) {
            assertEquals(data.getPointSize(category), 2.0);
        }
    }

    public void testPointColor() {
        final ChartImpl chart = new BaseFigureImpl().newChart();
        final TestCat data = new TestCat(chart.newAxes());

        data.pointColor(1);
        assertEquals(data.getColor(0), PlotUtils.intToColor(1));

        data.pointColor(categories[0], 1);
        assertEquals(data.getColor(categories[0]), PlotUtils.intToColor(1));

        data.pointColor(c1);
        assertEquals(data.getColor(0), c1);

        data.pointColor(categories[0], colors[0]);
        assertEquals(data.getColor(categories[0]), colors[0]);


        final Map<String, Color> colorMap = new HashMap<>();
        for (int i = 0; i < categories.length; i++) {
            colorMap.put(categories[i], colors[i]);
        }
        data.pointColor(colorMap);
        for (int i = 0; i < colors.length; i++) {
            assertEquals(data.getColor(categories[i]), colors[i]);
        }

        int[] icolors = {1, 2, 3};
        final Map<String, Integer> intMap = new HashMap<>();
        for (int i = 0; i < categories.length; i++) {
            intMap.put(categories[i], icolors[i]);
        }
        data.pointColorInteger(intMap);
        for (int i = 0; i < icolors.length; i++) {
            assertEquals(data.getColor(categories[i]), PlotUtils.intToColor(icolors[i]));
        }

        data.pointColorInteger(c -> 0);
        assertEquals(data.getColor(categories[0]), PlotUtils.intToColor(0));

        data.pointColorInteger(new Closure<Integer>(null) {
            @Override
            public void setResolveStrategy(int resolveStrategy) {
                super.setResolveStrategy(resolveStrategy);
            }

            @Override
            public Integer call() {
                return 1;
            }

            @Override
            public Integer call(Object... args) {
                return 1;
            }

            @Override
            public Integer call(Object arguments) {
                return 1;
            }
        });
        assertEquals(data.getColor(categories[0]), PlotUtils.intToColor(1));

        data.pointColor(x -> c1);
        assertEquals(data.getColor(1), c1);
        assertEquals(data.getColor(-10), c1);

        data.pointColor(colorMap);
    }

    public void testSeriesColor() {
        final AbstractCategoryDataSeries series = new TestCat();
        series.seriesColor("red");
        for (int i = 0; i < series.size(); i++) {
            assertEquals(Color.color("red"), series.getLineColor());
            assertEquals(Color.color("red"), series.getColor(categories[i]));
        }

        series.seriesColor(c1);
        for (int i = 0; i < series.size(); i++) {
            assertEquals(c1, series.getLineColor());
            assertEquals(c1, series.getColor(categories[i]));
        }
    }

    public void testPointLabel() {
        final TestCat data = new TestCat();

        data.pointLabel(5);
        assertEquals(data.getLabel(categories[0]), "5");

        data.pointLabel(categories[1], colors[1]);
        assertEquals(data.getLabel(categories[1]), colors[1].toString());

        int[] ilabels = {1, 2, 3};
        final Map<String, Integer> intMap = new HashMap<>();
        for (int i = 0; i < categories.length; i++) {
            intMap.put(categories[i], ilabels[i]);
        }
        data.pointLabel(intMap);
        assertEquals(data.getLabel(categories[2]), ilabels[2] + "");

        data.pointLabel(s -> "Label");
        assertEquals(data.getLabel(categories[1]), "Label");

        data.pointLabel(new ClosureFunction<Comparable, String>(new Closure<String>(null) {
            @Override
            public void setResolveStrategy(int resolveStrategy) {
                super.setResolveStrategy(resolveStrategy);
            }

            @Override
            public String call() {
                return "S";
            }

            @Override
            public String call(Object... args) {
                return "S";
            }

            @Override
            public String call(Object arguments) {
                return "S";
            }
        }));
        assertEquals(data.getLabel(categories[0]), "S");
    }

    public void testPointShape() {
        final TestCat data = new TestCat();

        data.pointShape(NamedShape.CIRCLE);
        assertEquals(NamedShape.CIRCLE, data.getPointShape(categories[0]));

        data.pointShape("square");
        assertEquals(NamedShape.SQUARE, data.getPointShape(categories[0]));

        data.pointShape(categories[0], "up_triangle");
        assertEquals(NamedShape.UP_TRIANGLE, data.getPointShape(categories[0]));
        assertEquals(NamedShape.SQUARE, data.getPointShape(categories[1]));

        data.pointShape(categories[0], NamedShape.DOWN_TRIANGLE);
        assertEquals(NamedShape.DOWN_TRIANGLE, data.getPointShape(categories[0]));
        assertEquals(NamedShape.SQUARE, data.getPointShape(categories[1]));

        final Map<String, String> shapesMap = new HashMap<>();
        shapesMap.put(categories[0], "circle");
        shapesMap.put(categories[1], "square");
        data.pointShape(shapesMap);
        assertEquals(NamedShape.valueOf(shapesMap.get(categories[0]).toUpperCase()), data.getPointShape(categories[0]));
        assertEquals(NamedShape.valueOf(shapesMap.get(categories[1]).toUpperCase()), data.getPointShape(categories[1]));
        assertEquals(NamedShape.SQUARE, data.getPointShape(categories[2]));

        data.pointShape(shapesMap::get);
        assertEquals(NamedShape.valueOf(shapesMap.get(categories[0]).toUpperCase()), data.getPointShape(categories[0]));
        assertEquals(NamedShape.valueOf(shapesMap.get(categories[1]).toUpperCase()), data.getPointShape(categories[1]));
        assertEquals(NamedShape.SQUARE, data.getPointShape(categories[2]));
    }

    public void testPointShapeExceptions() {
        final TestCat data = new TestCat();

        final String pointShape = "squar";
        try {
            data.pointShape(pointShape);
            fail("Shouldn't come here for pointShape(String shape)");
        } catch (IllegalArgumentException iae) {
            assertTrue(iae.getMessage().contains("squar"));
        }

        try {
            data.pointShape(categories[0], "up_triangl");
            fail("Shouldn't come here for pointShape(String... shape)");
        } catch (IllegalArgumentException iae) {
            assertTrue(iae.getMessage().contains("Not a valid"));
        }
    }

    public void testGroup() {
        final AbstractCategoryDataSeries series = new TestCat();
        series.group(10);
        assertEquals(10, series.getGroup());
    }
}

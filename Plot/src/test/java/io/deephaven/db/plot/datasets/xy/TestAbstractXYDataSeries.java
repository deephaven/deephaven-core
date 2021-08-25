/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.plot.datasets.xy;

import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.db.plot.*;
import io.deephaven.db.plot.datasets.data.IndexableData;
import io.deephaven.db.plot.datasets.data.IndexableDataArray;
import io.deephaven.db.plot.datasets.data.IndexableDataInteger;
import io.deephaven.db.plot.util.PlotUtils;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.utils.TableTools;
import io.deephaven.db.v2.DynamicTable;
import io.deephaven.gui.color.Color;
import io.deephaven.gui.color.Paint;
import io.deephaven.gui.shape.JShapes;
import io.deephaven.gui.shape.NamedShape;
import groovy.lang.Closure;
import junit.framework.TestCase;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class TestAbstractXYDataSeries extends BaseArrayTestCase {

    private static class TestAXYDS extends AbstractXYDataSeries {

        TestAXYDS() {
            this(new BaseFigureImpl().newChart().newAxes());
        }

        TestAXYDS(AxesImpl axes) {
            super(axes, 1, "test", null);
            assertEquals(name(), "test");
        }

        @Override
        public double getX(int i) {
            return 0;
        }

        @Override
        public double getY(int i) {
            return i;
        }

        @Override
        public XYDataSeriesInternal copy(AxesImpl axes) {
            return null;
        }

        @Override
        public int size() {
            return 3;
        }
    }

    public void testVisibility() {
        final TestAXYDS data = new TestAXYDS();

        assertNull(data.getLinesVisible());
        data.linesVisible(false);
        assertFalse(data.getLinesVisible());

        assertNull(data.getPointsVisible());
        data.pointsVisible(false);
        assertFalse(data.getPointsVisible());


        data.gradientVisible(false);
        assertFalse(data.getGradientVisible());
    }

    public void testLineStyle() {
        final TestAXYDS data = new TestAXYDS();
        final LineStyle style = LineStyle.lineStyle(0.5);

        assertNull(data.getLineStyle());
        data.lineStyle(style);
        assertEquals(data.getLineStyle(), style);
    }

    public void testLineColor() {
        final AxesImpl axes = new BaseFigureImpl().newChart().newAxes();
        final TestAXYDS data = new TestAXYDS(axes);
        final Color color = new Color(0, 0, 0);
        assertNull(data.getLineColor());
        data.lineColor(color);
        assertEquals(data.getLineColor(), color);

        data.lineColor(1);
        assertEquals(data.getLineColor(), PlotUtils.intToColor(1));

        data.lineColor("Red");
        assertEquals(Color.color("red"), data.getLineColor());
    }

    public void testErrorBarColor() {
        final AxesImpl axes = new BaseFigureImpl().newChart().newAxes();
        final TestAXYDS data = new TestAXYDS(axes);
        final Color color = new Color(0, 0, 0);
        assertNull(data.getErrorBarColor());
        data.errorBarColor(color);
        assertEquals(data.getErrorBarColor(), color);

        data.errorBarColor(1);
        assertEquals(data.getErrorBarColor(), PlotUtils.intToColor(1));

        data.errorBarColor("Red");
        assertEquals(Color.color("red"), data.getErrorBarColor());
    }

    public void testPointSize() {
        final TestAXYDS data = new TestAXYDS();

        data.pointSize(1);
        assertEquals(data.getPointSize(0), 1.0);

        data.pointSize(2.0);
        assertEquals(data.getPointSize(0), 2.0);

        data.pointSize(3L);
        assertEquals(data.getPointSize(0), 3.0);

        data.pointSize(new AtomicInteger(4));
        assertEquals(data.getPointSize(0), 4.0);

        int[] sizes = {1, 2, 3};
        data.pointSize(sizes);
        for (int i = 0; i < sizes.length; i++) {
            assertEquals(data.getPointSize(i), (double) sizes[i]);
        }

        long[] lsizes = {1, 2, 3};
        data.pointSize(lsizes);
        for (int i = 0; i < sizes.length; i++) {
            assertEquals(data.getPointSize(i), (double) lsizes[i]);
        }

        double[] dsizes = {1, 2, 3};
        data.pointSize(dsizes);
        for (int i = 0; i < sizes.length; i++) {
            assertEquals(data.getPointSize(i), (double) dsizes[i]);
        }

        AtomicInteger[] asizes = {new AtomicInteger(1), new AtomicInteger(2), new AtomicInteger(3)};
        data.pointSize(asizes);
        for (int i = 0; i < asizes.length; i++) {
            assertEquals(data.getPointSize(i), asizes[i].doubleValue());
        }

        final String[] cats = {"A", "B", "C"};
        Table t = TableTools
            .newTable(TableTools.doubleCol("Dubs", dsizes), TableTools.col("Str", cats)).ungroup();
        data.pointSize(t, "Dubs");
        for (int i = 0; i < t.size(); i++) {
            assertEquals(data.getPointSize(i), dsizes[i]);
        }

        try {
            data.pointSize(t, "Str");
            TestCase.fail("Expected an exception");
        } catch (UnsupportedOperationException e) {
            assertTrue(e.getMessage().contains("Unsupported numeric data type"));
        }


        // dsizes[0] = 2;
        // t = TableTools.newTable(TableTools.doubleCol("Dubs", dsizes), TableTools.col("Str",
        // cats)).ungroup();
        // Set<String> set = new HashSet<>();
        // Collections.addAll(set, cats);
        //
        // SwappableTableTestUtils.testSwappableTableMethod(t, "Str", set, data,
        // XYDataSeries::pointSize, "Dubs");
        // data.getPointSize(0);
        // SwingTestUtils.emptySwingQueue();
        // for (int i = 0; i < t.size(); i++) {
        // assertEquals(dsizes[i], data.getPointSize(i));
        // }
    }

    public void testPointColor() {
        final AxesImpl axes = new BaseFigureImpl().newChart().newAxes();
        final TestAXYDS data = new TestAXYDS(axes);

        final Color c1 = new Color(0, 0, 0);
        final Color c2 = new Color(100, 100, 100);
        final Color c3 = new Color(255, 255, 255);

        data.pointColor(1);
        assertEquals(data.getPointColor(0), PlotUtils.intToColor(1));

        data.pointColor(c1);
        assertEquals(data.getPointColor(0), c1);

        data.pointColor(new Integer(1));
        assertEquals(data.getPointColor(0), PlotUtils.intToColor(1));

        data.pointColor("Red");
        for (int i = 0; i < data.size(); i++) {
            assertEquals(Color.color("red"), data.getPointColor(i));
        }

        final String[] colorStrings = {"red", "blue", "green"};
        data.pointColor(colorStrings);
        for (int i = 0; i < data.size(); i++) {
            assertEquals(Color.color(colorStrings[i]), data.getPointColor(i));
        }

        final Color[] colors = {c1, c2, c3};
        data.pointColor(colors);
        for (int i = 0; i < colors.length; i++) {
            assertEquals(data.getPointColor(i), colors[i]);
        }

        int[] icolors = {1, 2, 3};
        data.pointColor(icolors);
        for (int i = 0; i < icolors.length; i++) {
            assertEquals(data.getPointColor(i), PlotUtils.intToColor(icolors[i]));
        }

        IndexableData<Integer> idiColors = new IndexableDataInteger(icolors, null);
        data.pointColorInteger(idiColors);
        for (int i = 0; i < idiColors.size(); i++) {
            assertEquals(data.getPointColor(i), PlotUtils.intToColor(idiColors.get(i)));
        }

        final Integer[] asizes = {new Integer(1), new Integer(2), new Integer(3)};
        data.pointColor(asizes);
        for (int i = 0; i < asizes.length; i++) {
            assertEquals(data.getPointColor(i), PlotUtils.intToColor(asizes[i]));
        }

        final String[] cats = {"A", "B", "C"};
        Table t = TableTools.newTable(TableTools.intCol("ints", icolors),
            TableTools.col("Str", cats), TableTools.col("Paints", colors));
        data.pointColor(t, "ints");
        for (int i = 0; i < t.size(); i++) {
            assertEquals(data.getPointColor(i), PlotUtils.intToColor(icolors[i]));
        }

        colors[0] = c3;
        t = TableTools.newTable(TableTools.intCol("ints", icolors), TableTools.col("Str", cats),
            TableTools.col("Paints", colors));
        data.pointColor(t, "Paints");
        for (int i = 0; i < t.size(); i++) {
            assertEquals(colors[i], data.getPointColor(i));
        }

        // colors[0] = c1;
        // t = TableTools.newTable(TableTools.intCol("ints", icolors), TableTools.col("Str", cats),
        // TableTools.col("Paints", colors));
        // Set<String> set = new HashSet<>();
        // Collections.addAll(set, cats);
        // SwappableTableTestUtils.testSwappableTableMethod(t, "Str", set, data,
        // XYDataSeries::pointColor, "Paints");
        // for (int i = 0; i < t.size(); i++) {
        // assertEquals(colors[i], data.getPointColor(i));
        // }
        //
        // SwappableTableTestUtils.testSwappableTableMethod(t, "Str", set, data,
        // XYDataSeries::pointColor, "ints");
        // for (int i = 0; i < t.size(); i++) {
        // assertEquals(theme.getSeriesColor(icolors[i]), data.getPointColor(i));
        // }

        try {
            data.pointColor(t, "Str");
            TestCase.fail("Expected an exception");
        } catch (UnsupportedOperationException e) {
            assertTrue(e.getMessage().contains("Unsupported numeric data type"));
        }

        try {
            data.pointColor(t, "Str");
            TestCase.fail("Expected an exception");
        } catch (UnsupportedOperationException e) {
            assertTrue(e.getMessage().contains("Unsupported numeric data type"));
        }

        t = t.updateView("Dubs = (double) i");
        try {
            data.pointColor(t, "Dubs");
            TestCase.fail("Expected an exception");
        } catch (UnsupportedOperationException e) {
            assertTrue(e.getMessage().contains("converted"));
        }

        data.pointColorByY(x -> x > 1 ? c1 : c2);
        assertEquals(data.getPointColor(2), c1);
        assertEquals(data.getPointColor(1), c2);


        data.pointColorByY(new Closure<Paint>(null) {
            @Override
            public Paint call() {
                return c1;
            }

            @Override
            public Paint call(Object... args) {
                return c1;
            }

            @Override
            public Paint call(Object arguments) {
                return c1;
            }
        });
        assertEquals(data.getPointColor(1), c1);
    }

    public void testPointLabel() {
        final TestAXYDS data = new TestAXYDS();
        final String[] labelArray = {"A", "B", "C"};

        data.pointLabel(5);
        assertEquals(data.getPointLabel(0), "5");

        IndexableData<String> labels = new IndexableDataArray<>(labelArray, null);
        data.pointLabel(labels);
        for (int i = 0; i < labelArray.length; i++) {
            assertEquals(data.getPointLabel(i), labelArray[i]);
        }

        data.pointLabel((Object[]) labelArray);
        for (int i = 0; i < labelArray.length; i++) {
            assertEquals(data.getPointLabel(i), labelArray[i]);
        }

        Table table = TableTools.newTable(TableTools.col("Labels", labelArray));
        data.pointLabel(table, "Labels");
        for (int i = 0; i < table.size(); i++) {
            assertEquals(data.getPointLabel(i), labelArray[i]);
        }

        labelArray[0] = "B";
        final Set<String> set = new HashSet<>();
        Collections.addAll(set, labelArray);

        final String labelFormat = "XXX";
        data.pointLabelFormat(labelFormat);
        assertEquals(labelFormat, data.getPointLabelFormat());
    }

    public void testPointShape() {
        final TestAXYDS data = new TestAXYDS();
        data.pointShape(NamedShape.UP_TRIANGLE);
        assertEquals(data.getPointShape(1), NamedShape.UP_TRIANGLE);

        final String pointShape = "square";
        data.pointShape(pointShape);
        assertEquals(data.getPointShape(0), NamedShape.valueOf(pointShape.toUpperCase()));

        data.pointShape("circle", "up_triangle");
        assertEquals(data.getPointShape(0), NamedShape.valueOf("circle".toUpperCase()));
        assertEquals(data.getPointShape(1), NamedShape.valueOf("up_triangle".toUpperCase()));
        assertEquals(data.getPointShape(2), NamedShape.valueOf("square".toUpperCase()));

        data.pointShape(new String[] {"up_triangle", "circle"});
        assertEquals(JShapes.shape((NamedShape) data.getPointShape(0)),
            JShapes.shape(NamedShape.valueOf("up_triangle".toUpperCase())));
        assertEquals(data.getPointShape(1), NamedShape.valueOf("circle".toUpperCase()));
        assertEquals(data.getPointShape(2), NamedShape.valueOf("square".toUpperCase()));

        data.pointShape(NamedShape.UP_TRIANGLE, NamedShape.CIRCLE);
        assertEquals(data.getPointShape(0), NamedShape.valueOf("up_triangle".toUpperCase()));
        assertEquals(data.getPointShape(1), NamedShape.valueOf("circle".toUpperCase()));
        assertEquals(data.getPointShape(2), NamedShape.valueOf("square".toUpperCase()));

        data.pointShape(new NamedShape[] {NamedShape.CIRCLE, NamedShape.UP_TRIANGLE});
        assertEquals(data.getPointShape(0), NamedShape.valueOf("circle".toUpperCase()));
        assertEquals(data.getPointShape(1), NamedShape.valueOf("up_triangle".toUpperCase()));
        assertEquals(data.getPointShape(2), NamedShape.valueOf("square".toUpperCase()));

        final String[] shapes = new String[] {"up_triangle", "down_triangle", "right_triangle"};
        IndexableData<String> indexableData = new IndexableDataArray<>(shapes, null);
        data.pointShape(indexableData);
        assertEquals(data.getPointShape(0), NamedShape.valueOf("up_triangle".toUpperCase()));
        assertEquals(data.getPointShape(1), NamedShape.valueOf("down_triangle".toUpperCase()));
        assertEquals(data.getPointShape(2), NamedShape.valueOf("right_triangle".toUpperCase()));

        final DynamicTable shapeTable =
            TableTools.newTable(TableTools.col("shapes", "diamond", "circle", "ellipse"));
        data.pointShape(shapeTable, "shapes");
        data.getPointShape(0);
        assertEquals(data.getPointShape(0), NamedShape.valueOf("diamond".toUpperCase()));
        assertEquals(data.getPointShape(1), NamedShape.valueOf("circle".toUpperCase()));
        assertEquals(data.getPointShape(2), NamedShape.valueOf("ellipse".toUpperCase()));

        final DynamicTable shapeObjectTable = TableTools.newTable(TableTools.col("shapes",
            NamedShape.DIAMOND, NamedShape.ELLIPSE, NamedShape.UP_TRIANGLE));
        data.pointShape(shapeObjectTable, "shapes");
        data.getPointShape(0);
        assertEquals(data.getPointShape(0), NamedShape.valueOf("diamond".toUpperCase()));
        assertEquals(data.getPointShape(1), NamedShape.valueOf("ellipse".toUpperCase()));
        assertEquals(data.getPointShape(2), NamedShape.valueOf("up_triangle".toUpperCase()));

    }

    public void testPointShapeExceptions() {
        final TestAXYDS data = new TestAXYDS();

        final String pointShape = "squar";
        try {
            data.pointShape(pointShape);
            fail("Shouldn't come here for pointShape(String shape)");
        } catch (IllegalArgumentException iae) {
            assertTrue(iae.getMessage().contains("squar"));
        }

        try {
            data.pointShape("circl", "up_triangle");
            fail("Shouldn't come here for pointShape(String... shape)");
        } catch (IllegalArgumentException iae) {
            assertTrue(iae.getMessage().contains("0"));
        }

        try {
            data.pointShape("circle", "up_triangl");
            fail("Shouldn't come here for pointShape(String... shape)");
        } catch (IllegalArgumentException iae) {
            assertTrue(iae.getMessage().contains("1"));
        }

        final String[] shapes = new String[] {"up_triangle", "down_triangl", "right_triangle"};
        new IndexableData<String>(null) {

            @Override
            public int size() {
                return shapes.length;
            }

            @Override
            public String get(int index) {
                return shapes[index];
            }
        };
        try {
            data.pointShape(shapes);
            fail("Shouldn't come here for pointShape(IndexableData<String> shape)");
        } catch (IllegalArgumentException iae) {
            assertTrue(iae.getMessage().contains("1"));
        }

        final DynamicTable dt = TableTools.newTable(TableTools.col("shapes", 1, 2, 3));
        try {
            data.pointShape(dt, "shapes");
            fail("Shouldn't come here for pointShape(Table t, String columnName)");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("String"));
        }

        final DynamicTable shapeTable =
            TableTools.newTable(TableTools.col("shapes", "diamond", "circle", "ellips"));
        data.pointShape(shapeTable, "shapes");
        try {
            data.getPointShape(2);
            fail("Shouldn't come here for pointShape(Table t, String columnName)");
        } catch (IllegalArgumentException iae) {
            assertTrue(iae.getMessage().contains("ellips"));
        }
    }

    public static void testCopy(final AbstractXYDataSeries original,
        final AbstractXYDataSeries copy) {
        testCopy(original, copy, true);
    }

    public static void testCopy(final AbstractXYDataSeries original,
        final AbstractXYDataSeries copy, final boolean testTables) {
        assertEquals(original.name(), copy.name());
        assertEquals(original.size(), copy.size());
        for (int i = 0; i < original.size(); i++) {
            assertEquals(original.getX(i), copy.getX(i));
            assertEquals(original.getY(i), copy.getY(i));
            assertEquals(original.getPointLabel(i), copy.getPointLabel(i));
            assertEquals(original.getPointColor(i), copy.getPointColor(i));
            assertEquals(original.getPointSize(i), copy.getPointSize(i));
        }

        if (testTables) {
            assertEquals(original.getTableHandles(), copy.getTableHandles());
            assertEquals(original.getSwappableTables(), copy.getSwappableTables());
        }

        assertEquals(original.getPointsVisible(), copy.getPointsVisible());
        assertEquals(original.getGradientVisible(), copy.getGradientVisible());
        assertEquals(original.getLineStyle(), copy.getLineStyle());
        assertEquals(original.getLineColor(), copy.getLineColor());
        assertEquals(original.getPointLabelFormat(), copy.getPointLabelFormat());
        assertEquals(original.getXToolTipPattern(), copy.getXToolTipPattern());
        assertEquals(original.getYToolTipPattern(), copy.getYToolTipPattern());
        assertEquals(original.getSeriesColor(), copy.getSeriesColor());
    }
}

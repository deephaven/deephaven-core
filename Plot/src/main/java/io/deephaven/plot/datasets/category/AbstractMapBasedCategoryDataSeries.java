/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.plot.datasets.category;

import io.deephaven.plot.AxesImpl;
import io.deephaven.plot.util.ArgumentValidations;
import io.deephaven.gui.color.Paint;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public abstract class AbstractMapBasedCategoryDataSeries extends AbstractCategoryDataSeries {
    public AbstractMapBasedCategoryDataSeries(AxesImpl axes, int id, Comparable name) {
        super(axes, id, name);
    }

    public AbstractMapBasedCategoryDataSeries(final AxesImpl axes, final int id, final Comparable name,
            final AbstractCategoryDataSeries series) {
        super(axes, id, name, series);
    }

    public AbstractMapBasedCategoryDataSeries(final AbstractCategoryDataSeries series, final AxesImpl axes) {
        super(series, axes);
    }

    @Override
    public CategoryDataSeries pointShape(final Function<Comparable, String> shapes) {
        return super.pointShape(constructMapFromData(shapes));
    }

    @Override
    public <NUMBER extends Number> AbstractCategoryDataSeries pointSize(final Function<Comparable, NUMBER> factors) {
        return super.pointSize(constructMapFromData(factors));
    }

    @Override
    public <COLOR extends Paint> AbstractCategoryDataSeries pointColor(final Function<Comparable, COLOR> colors) {
        return super.pointColor(constructMapFromData(colors));
    }

    @Override
    public <COLOR extends Integer> AbstractCategoryDataSeries pointColorInteger(
            final Function<Comparable, COLOR> colors) {
        return super.pointColorInteger(constructMapFromData(colors));
    }

    @Override
    public <T extends Paint> AbstractCategoryDataSeries pointColorByY(Function<Double, T> colors) {
        return super.pointColorByY(constructMapFromNumericalData(colors));
    }

    @Override
    public <LABEL> AbstractCategoryDataSeries pointLabel(final Function<Comparable, LABEL> labels) {
        return super.pointLabel(constructMapFromData(labels));
    }

    private <T> Map<Double, T> constructMapFromNumericalData(final Function<Double, T> function) {
        ArgumentValidations.assertNotNull(function, "function", getPlotInfo());
        final Map<Double, T> map = new HashMap<>();

        for (final Comparable category : categories()) {
            final double value = getValue(category).doubleValue();
            map.put(value, function.apply(value));
        }

        return map;
    }

    private <T> Map<Comparable, T> constructMapFromData(final Function<Comparable, T> function) {
        ArgumentValidations.assertNotNull(function, "function", getPlotInfo());
        final Map<Comparable, T> map = new HashMap<>();

        for (final Comparable category : categories()) {
            map.put(category, function.apply(category));
        }

        return map;
    }
}

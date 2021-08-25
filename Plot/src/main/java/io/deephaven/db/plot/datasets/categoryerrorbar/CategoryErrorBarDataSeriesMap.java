/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.plot.datasets.categoryerrorbar;

import io.deephaven.db.plot.AxesImpl;
import io.deephaven.db.plot.datasets.category.AbstractCategoryDataSeries;
import io.deephaven.db.plot.datasets.category.AbstractMapBasedCategoryDataSeries;
import io.deephaven.db.plot.datasets.data.IndexableData;
import io.deephaven.db.plot.datasets.data.IndexableNumericData;
import io.deephaven.db.plot.errors.PlotIllegalArgumentException;
import io.deephaven.db.plot.errors.PlotUnsupportedOperationException;
import io.deephaven.db.plot.util.PlotUtils;
import io.deephaven.db.util.tuples.generated.ObjectIntTuple;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

import static io.deephaven.db.plot.util.NullCategory.INSTANCE;

/**
 * A dataset for categorical data which maps a category to it's numerical value.
 * <p>
 * The number of categories and the number of values must be the same. Does not support repeat
 * categories.
 */
public class CategoryErrorBarDataSeriesMap extends AbstractMapBasedCategoryDataSeries
    implements CategoryErrorBarDataSeriesInternal {

    private static final long serialVersionUID = 3326261675883932559L;
    private final Map<Comparable, ObjectIntTuple> data = new LinkedHashMap<>();
    private final Map<Comparable, Number> dataToYStart = new LinkedHashMap<>();
    private final Map<Comparable, Number> dataToYEnd = new LinkedHashMap<>();

    private double yMin = Double.NaN;
    private double yMax = Double.NaN;

    /**
     * Creates an instance of CategoryDataSeriesMap, which maps a category to it's numerical value.
     * <p>
     * The number of categories and the number of values must be the same. Does not support null or
     * repeat categories.
     *
     * @param axes {@link AxesImpl} on which this dataset is being plotted
     * @param id data series id
     * @param name series name
     * @param categories categorical data
     * @param y numerical data
     * @param yLow low error bar data
     * @param yHigh high error bar data
     * @param <T> type of the categorical data
     * @throws IllegalArgumentException {@code categories} and {@code values} must not be null
     *         {@code categories} and {@code values} must have equal sizes
     * @throws UnsupportedOperationException {@code categories} must not contain null values
     *         {@code categories} must not contain repeat values
     */
    public <T extends Comparable> CategoryErrorBarDataSeriesMap(final AxesImpl axes,
        final int id,
        final Comparable name,
        final IndexableData<T> categories,
        final IndexableNumericData y,
        final IndexableNumericData yLow,
        final IndexableNumericData yHigh) {
        this(axes, id, name, categories, y, yLow, yHigh, null);
    }

    public <T extends Comparable> CategoryErrorBarDataSeriesMap(final AxesImpl axes,
        final int id,
        final Comparable name,
        final IndexableData<T> categories,
        final IndexableNumericData y,
        final IndexableNumericData yLow,
        final IndexableNumericData yHigh,
        final AbstractCategoryDataSeries series) {
        super(axes, id, name, series);

        if (categories == null || y == null || yLow == null || yHigh == null) {
            throw new PlotIllegalArgumentException("Null input array", this);
        }

        if (categories.size() != y.size() || y.size() != yLow.size()
            || yLow.size() != yHigh.size()) {
            throw new PlotIllegalArgumentException("Categories and values lengths do not match",
                this);
        }

        for (int i = 0; i < categories.size(); i++) {
            Comparable<?> category = categories.get(i);
            category = category == null ? INSTANCE : category;

            if (data.containsKey(category)) {
                throw new PlotUnsupportedOperationException(
                    "Category value repeated multiple times in dataset: series=" + name
                        + "category=" + categories.get(i),
                    this);
            }

            setValueIndexed(category, y.get(i), data, i);
            setValue(category, yLow.get(i), dataToYStart);
            setValue(category, yHigh.get(i), dataToYEnd);
        }
    }

    /**
     * Creates a copy of a series using a different Axes.
     *
     * @param series series to copy.
     * @param axes new axes to use.
     */
    private CategoryErrorBarDataSeriesMap(final CategoryErrorBarDataSeriesMap series,
        final AxesImpl axes) {
        super(series, axes);
        this.data.putAll(series.data);
        this.dataToYStart.putAll(series.dataToYStart);
        this.dataToYEnd.putAll(series.dataToYEnd);
        this.yMin = series.yMin;
        this.yMax = series.yMax;
    }


    @Override
    public CategoryErrorBarDataSeriesMap copy(AxesImpl axes) {
        return new CategoryErrorBarDataSeriesMap(this, axes);
    }

    @Override
    public int size() {
        return data.size();
    }

    @Override
    public Collection<Comparable> categories() {
        return data.keySet();
    }

    @Override
    public Number getValue(final Comparable category) {
        final ObjectIntTuple catItem = data.get(category == null ? INSTANCE : category);
        return catItem == null ? null : (Number) catItem.getFirstElement();
    }

    @Override
    public long getCategoryLocation(Comparable category) {
        final ObjectIntTuple catItem = data.get(category == null ? INSTANCE : category);
        return catItem == null ? -1 : catItem.getSecondElement();
    }

    @Override
    public Number getStartY(final Comparable category) {
        return dataToYStart.get(category);
    }

    @Override
    public Number getEndY(final Comparable category) {
        return dataToYEnd.get(category);
    }

    private void setValueIndexed(final Comparable category, final Number value,
        final Map<Comparable, ObjectIntTuple> data, int index) {
        if (value == null) {
            data.remove(category);
        } else {
            data.put(category, new ObjectIntTuple(value, index));
        }

        yMin = PlotUtils.minIgnoreNaN(yMin, value == null ? Double.NaN : value.doubleValue());
        yMax = PlotUtils.maxIgnoreNaN(yMax, value == null ? Double.NaN : value.doubleValue());
    }

    private void setValue(final Comparable category, final Number value,
        final Map<Comparable, Number> data) {
        if (value == null) {
            data.remove(category);
        } else {
            data.put(category, value);
        }

        yMin = PlotUtils.minIgnoreNaN(yMin, value == null ? Double.NaN : value.doubleValue());
        yMax = PlotUtils.maxIgnoreNaN(yMax, value == null ? Double.NaN : value.doubleValue());
    }
}

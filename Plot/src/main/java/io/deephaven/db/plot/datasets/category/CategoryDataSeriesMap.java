/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.plot.datasets.category;

import io.deephaven.db.plot.AxesImpl;
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
public class CategoryDataSeriesMap extends AbstractMapBasedCategoryDataSeries {

    private static final long serialVersionUID = 3326261675883932559L;
    private final Map<Comparable, ObjectIntTuple> data = new LinkedHashMap<>();

    private double yMin;
    private double yMax;

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
     * @param values numerical data
     * @param <T> type of the categorical data
     * @throws IllegalArgumentException {@code categories} and {@code values} must not be null
     *         {@code categories} and {@code values} must have equal sizes
     * @throws UnsupportedOperationException {@code categories} must not contain null values
     *         {@code categories} must not contain repeat values
     */
    public <T extends Comparable<?>> CategoryDataSeriesMap(final AxesImpl axes,
        final int id,
        final Comparable<?> name,
        final IndexableData<T> categories,
        final IndexableNumericData values) {
        this(axes, id, name, categories, values, null);
    }

    public <T extends Comparable<?>> CategoryDataSeriesMap(final AxesImpl axes,
        final int id,
        final Comparable<?> name,
        final IndexableData<T> categories,
        final IndexableNumericData values,
        final AbstractCategoryDataSeries series) {
        super(axes, id, name, series);

        if (categories == null || values == null) {
            throw new PlotIllegalArgumentException("Null input array", this);
        }

        if (categories.size() != values.size()) {
            throw new PlotIllegalArgumentException("Categories and Values lengths do not match",
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

            final double value = values.get(i);

            setValue(category, value, i);

            yMin = PlotUtils.minIgnoreNaN(yMin, value);
            yMax = PlotUtils.maxIgnoreNaN(yMax, value);
        }
    }

    /**
     * Creates a copy of a series using a different Axes.
     *
     * @param series series to copy.
     * @param axes new axes to use.
     */
    private CategoryDataSeriesMap(final CategoryDataSeriesMap series, final AxesImpl axes) {
        super(series, axes);
        this.data.putAll(series.data);
        this.yMin = series.yMin;
        this.yMax = series.yMax;
    }

    @Override
    public CategoryDataSeriesMap copy(AxesImpl axes) {
        return new CategoryDataSeriesMap(this, axes);
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

    private void setValue(final Comparable<?> category, final Number value, int index) {
        if (value == null) {
            data.remove(category);
        } else {
            data.put(category, new ObjectIntTuple(value, index));
        }
    }

    @Override
    public long getCategoryLocation(Comparable category) {
        final ObjectIntTuple catItem = data.get(category == null ? INSTANCE : category);
        return catItem == null ? -1 : catItem.getSecondElement();
    }
}

/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.plot.datasets.category;


import io.deephaven.db.plot.AxesImpl;
import io.deephaven.db.plot.datasets.DataSeriesInternal;
import io.deephaven.gui.color.Paint;
import io.deephaven.gui.shape.Shape;

import java.util.Collection;

/**
 * Dataset with discrete and numeric components. Discrete values must extend {@link Comparable} and are called
 * categories.
 */
public interface CategoryDataSeriesInternal extends CategoryDataSeries, DataSeriesInternal {

    ////////////////////////// internal //////////////////////////

    @Override
    CategoryDataSeriesInternal copy(final AxesImpl axes);

    /**
     * Gets the categories of the dataset.
     *
     * @return all categories in the dataset
     */
    Collection<Comparable> categories();

    /**
     * Gets the numeric value corresponding to the given {@code category}.
     *
     * @param category category
     * @return numeric value corresponding to the given {@code category}
     */
    Number getValue(final Comparable category);

    /**
     * Gets the {@link Paint} for the given data point.
     *
     * @param category category
     * @return {@link Paint} for the given data point
     */
    Paint getColor(final Comparable category);

    /**
     * Gets the point size for the given data point.
     *
     * @param category category
     * @return point size for the given data point
     */
    Double getPointSize(final Comparable category);


    /**
     * Gets the point label for the given data point.
     *
     * @param category category
     * @return point label for the given data point
     */
    String getLabel(final Comparable category);

    /**
     * Gets the point shape for the given data point.
     *
     * @param category category
     * @return point shape for the given data point
     */
    Shape getPointShape(final Comparable category);

    /**
     * Gets the group for this dataset.
     *
     * @return group for this dataset
     */
    int getGroup();

    default Number getStartY(final Comparable category) {
        return getValue(category);
    }

    default Number getEndY(final Comparable category) {
        return getValue(category);
    }

    default boolean drawYError() {
        return false;
    }

    /**
     * Gets the pie plot percent label format.
     *
     * @return pie plot percent label format
     */
    String getPiePercentLabelFormat();

    /**
     * Get the row key of the specified series.
     *
     * @return
     */
    long getCategoryLocation(Comparable category);
}

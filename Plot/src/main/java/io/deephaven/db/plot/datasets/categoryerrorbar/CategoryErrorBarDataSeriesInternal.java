/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.plot.datasets.categoryerrorbar;

import io.deephaven.db.plot.datasets.category.CategoryDataSeriesInternal;

/**
 * An {@link CategoryDataSeriesInternal} with error bars.
 */
public interface CategoryErrorBarDataSeriesInternal extends CategoryErrorBarDataSeries, CategoryDataSeriesInternal {
    @Override
    default boolean drawYError() {
        return true;
    }
}

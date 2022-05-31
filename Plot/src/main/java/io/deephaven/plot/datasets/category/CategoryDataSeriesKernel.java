package io.deephaven.plot.datasets.category;

import io.deephaven.plot.errors.PlotInfo;
import io.deephaven.plot.util.ArgumentValidations;
import io.deephaven.engine.table.Table;
import gnu.trove.map.hash.TObjectLongHashMap;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;

/**
 * The core of the Category data series update and indexing. This is used by the regular category data series to both
 * update the values of existing categories as well as to ensure that new categories are maintained in order of the
 * original table.
 */
@SuppressWarnings("rawtypes")
public class CategoryDataSeriesKernel {
    /** The table to which we are currently listening */
    private transient Table listenedToTable;

    /** The column in the table that defines the category */
    private final String categoryCol;

    /** The column in the table that defines the values */
    private final String valueColumn;

    /** The ordered categories at the end of an update cycle */
    private List<Comparable> categories = Collections.emptyList();

    /** A lookup for the index of a particular category within the series. */
    private final TObjectLongHashMap<Comparable> catIndex = new TObjectLongHashMap<>(10, .75f, -1);

    /** The mapping of category to actual value */
    private final Map<Comparable, Number> data = new HashMap<>();

    /** The current minimum value of the series */
    private double yMin;

    /** The current maximum of the series */
    private double yMax;

    /**
     * Create a standard update kernel for a Category series.
     *
     * @param categoryCol The name of the column containing the categories.
     * @param valueColumn The name of the column containing the values
     * @param plotInfo The {@link PlotInfo} object for logging
     */
    public CategoryDataSeriesKernel(@NotNull String categoryCol,
            @NotNull String valueColumn,
            @NotNull PlotInfo plotInfo) {
        ArgumentValidations.assertNotNull(categories, "categories", plotInfo);
        ArgumentValidations.assertNotNull(valueColumn, "values", plotInfo);

        this.categoryCol = categoryCol;
        this.valueColumn = valueColumn;

        initMinMax();
    }

    /**
     * Reinitialize the min and max values to Nan in preparation for reinitialization
     */
    public synchronized void initMinMax() {
        yMin = Double.NaN;
        yMax = Double.NaN;
    }

    /**
     * Get the size of the dataset (the number of categories)
     *
     * @return the number of categories in the dataset.
     */
    public synchronized int size() {
        return data.size();
    }

    /**
     * Get an ordered list of the categories in the data.
     *
     * @return the ordered categories of the data
     */
    public synchronized Collection<Comparable> categories() {
        return categories;
    }

    /**
     * Get the value of the specified category, or null if there was none.
     *
     * @param category The category to get the value of
     * @return the value of the specified category
     */
    @Nullable
    public synchronized Number getValue(final Comparable<?> category) {
        return data.get(category);
    }

    /**
     * Get the index key of the category within the original dataset. This can be used to enforce a global ordering of a
     * MultiSeries cat plot.
     *
     * @param category The category to locate.
     * @return the key of the specified category within the original data set. or -1 if not present.
     */
    public synchronized long getCategoryKey(final Comparable<?> category) {
        return catIndex.get(category);
    }
}

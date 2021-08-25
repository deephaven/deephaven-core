/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.plot;

import io.deephaven.db.plot.filters.SelectableDataSet;
import io.deephaven.db.tables.Table;
import io.deephaven.gui.color.Paint;

import java.io.Serializable;

import static io.deephaven.db.plot.DynamicChartTitle.DynamicChartTitleTable.defaultTitleFormat;
import static io.deephaven.db.plot.DynamicChartTitle.DynamicChartTitleTable.defaultTitleFormatWithColumnNames;

/**
 * Represents a graph. Contains {@link Axes} objects.
 */
public interface Chart extends Serializable {


    ////////////////////////// convenience //////////////////////////


    /**
     * Removes the series with the specified {@code names} from this Chart.
     *
     * @param names series names
     * @return this Chart
     */
    Chart chartRemoveSeries(final String... names);


    ////////////////////////// Title //////////////////////////


    /**
     * Sets the title of this Chart.
     *
     * @param title title
     * @return this {@link Chart}
     */
    Chart chartTitle(final String title);

    /**
     * Sets the title of this Chart.
     *
     * @param t table
     * @param titleColumns columns to include in the chart title
     * @return this {@link Chart} with the title set to display comma-separated values from the table
     */
    default Chart chartTitle(final Table t, final String... titleColumns) {
        return chartTitle(false, t, titleColumns);
    }

    /**
     * Sets the title of this Chart.
     *
     * @param showColumnNamesInTitle Whether to show column names in title. If this is true, the title format will
     *        include the column name before the comma separated values; otherwise only the comma separated values will
     *        be included.
     * @param t table
     * @param titleColumns columns to include in the chart title
     * @return this {@link Chart} with the title set to display comma-separated values from the table
     */
    default Chart chartTitle(final boolean showColumnNamesInTitle, final Table t, final String... titleColumns) {
        return chartTitle(showColumnNamesInTitle ? defaultTitleFormatWithColumnNames(titleColumns)
                : defaultTitleFormat(titleColumns), t, titleColumns);
    }

    /**
     * Sets the title of this Chart.
     *
     * @param titleFormat a {@link java.text.MessageFormat} format string for the chart title
     * @param t table
     * @param titleColumns columns to include in the chart title
     * @return this {@link Chart} with the title set to display values from the table
     */
    Chart chartTitle(final String titleFormat, final Table t, final String... titleColumns);

    /**
     * Sets the title of this Chart.
     *
     * @param sds selectable data set (e.g. OneClick table)
     * @param titleColumns columns to include in the chart title
     * @return this {@link Chart} with the title set to display comma-separated values from the table
     */
    default Chart chartTitle(final SelectableDataSet sds, final String... titleColumns) {
        return chartTitle(false, sds, titleColumns);
    }

    /**
     * Sets the title of this Chart.
     *
     * @param showColumnNamesInTitle Whether to show column names in title. If this is true, the title format will
     *        include the column name before the comma separated values; otherwise only the comma separated values will
     *        be included.
     * @param sds selectable data set (e.g. OneClick table)
     * @param titleColumns columns to include in the chart title
     * @return this {@link Chart} with the title set to display comma-separated values from the table
     */
    default Chart chartTitle(final boolean showColumnNamesInTitle, final SelectableDataSet sds,
            final String... titleColumns) {
        return chartTitle(showColumnNamesInTitle ? defaultTitleFormatWithColumnNames(titleColumns)
                : defaultTitleFormat(titleColumns), sds, titleColumns);
    }

    /**
     * Sets the title of this Chart.
     *
     * @param titleFormat a {@link java.text.MessageFormat} format string for the chart title
     * @param sds selectable data set (e.g. OneClick table)
     * @param titleColumns columns to include in the chart title
     * @return this {@link Chart} with the title set to display values from the table
     */
    Chart chartTitle(final String titleFormat, final SelectableDataSet sds, final String... titleColumns);

    /**
     * Sets the maximum row values that will be shown in title.
     * <p>
     * If total rows < {@code maxRowsCount}, then all the values will be shown separated by comma, otherwise just
     * {@code maxRowsCount} values will be shown along with ellipsis. <br/>
     * if {@code maxRowsCount} is < 0, all values will be shown. <br/>
     * if {@code maxRowsCount} is 0, then just first value will be shown without ellipsis. <br/>
     * The default is 0.
     *
     * @param maxRowsCount maximum number of row values to show in chart title
     * @return this Chart
     */
    Chart maxRowsInTitle(final int maxRowsCount);

    /**
     * Sets the font of this Chart's title.
     *
     * @param font font
     * @return this Chart
     */
    Chart chartTitleFont(final Font font);

    /**
     * Sets the font of this Chart's title.
     *
     * @param family font family; if null, set to Arial
     * @param style font style; if null, set to {@link Font.FontStyle} PLAIN
     * @param size the point size of the Font
     * @return this Chart
     */
    Chart chartTitleFont(final String family, final String style, final int size);

    /**
     * Sets the color of this Chart's title.
     *
     * @param color color
     * @return this Chart
     */
    Chart chartTitleColor(Paint color);

    /**
     * Sets the color of this Chart's title.
     *
     * @param color color
     * @return this Chart
     */
    Chart chartTitleColor(String color);

    ////////////////////////// Grid Lines //////////////////////////

    /**
     * Sets whether the Chart has grid lines.
     *
     * @param visible whether the Chart's grid lines are drawn
     * @return this Chart
     */
    Chart gridLinesVisible(final boolean visible);

    /**
     * Sets whether the Chart has grid lines in the x direction.
     *
     * @param visible whether the Chart's x grid lines are drawn
     * @return this Chart
     */
    Chart xGridLinesVisible(final boolean visible);

    /**
     * Sets whether the Chart has grid lines in the y direction
     *
     * @param visible whether the Chart's y grid lines are drawn
     * @return this Chart
     */
    Chart yGridLinesVisible(final boolean visible);

    ////////////////////////// Legend //////////////////////////


    /**
     * Sets whether the Chart's legend is shown or hidden.
     *
     * @param visible whether the Chart's legend is shown or hidden
     * @return this Chart
     */
    Chart legendVisible(final boolean visible);

    /**
     * Sets the font of this Chart's legend.
     *
     * @param font font
     * @return this Chart
     */
    Chart legendFont(final Font font);

    /**
     * Sets the font of this Chart's legend.
     *
     * @param family font family; if null, set to Arial
     * @param style font style; if null, set to {@link Font.FontStyle} PLAIN
     * @param size the point size of the Font
     * @return this Chart
     */
    Chart legendFont(final String family, final String style, final int size);

    /**
     * Sets the color of the text inside the Chart's legend.
     *
     * @param color color
     * @return this Chart
     */
    Chart legendColor(final Paint color);

    /**
     * Sets the color of the text inside the Chart's legend.
     *
     * @param color color
     * @return this Chart
     */
    Chart legendColor(final String color);


    ////////////////////////// Chart Size //////////////////////////


    /**
     * Sets the size of this Chart within the grid of the figure.
     *
     * @param rowSpan how many rows tall
     * @param colSpan how many columns wide
     * @return this Chart
     */
    Chart span(final int rowSpan, final int colSpan);

    /**
     * Sets the size of this Chart within the grid of the figure.
     *
     * @param n how many columns wide
     * @return this Chart
     */
    Chart colSpan(final int n);

    /**
     * Sets the size of this Chart within the grid of the figure.
     *
     * @param n how many rows tall
     * @return this Chart
     */
    Chart rowSpan(final int n);


    ////////////////////////// Axes Creation //////////////////////////


    /**
     * Creates new {@link Axes} on this Chart.
     *
     * @return newly created {@link Axes} with dimension 2 on this Chart
     */
    Axes newAxes();

    /**
     * Creates new {@link Axes} on this Chart.
     *
     * @param name name for the axes
     * @return newly created {@link Axes} with dimension 2 on this Chart
     */
    Axes newAxes(final String name);

    /**
     * Creates new {@link Axes} on this Chart.
     *
     * @param dim dimensions of the {@link Axes}
     * @return newly created {@link Axes} with dimension {@code dim} on this Chart
     */
    Axes newAxes(final int dim);

    /**
     * Creates new {@link Axes} on this Chart.
     *
     * @param name name for the axes
     * @param dim dimensions of the {@link Axes}
     * @return newly created {@link Axes} with dimension {@code dim} on this Chart
     */
    Axes newAxes(final String name, final int dim);


    ////////////////////////// Axes retrieval //////////////////////////


    /**
     * Gets an axes.
     *
     * @param id axes id.
     * @return selected axes.
     */
    Axes axes(final int id);

    /**
     * Gets an axes.
     *
     * @param name axes name.
     * @return selected axes.
     */
    Axes axes(final String name);


    ///////////////////// Plot Orientation ///////////////////////////


    /**
     * Sets the orientation of plots in this Chart.
     *
     * @param orientation plot orientation
     * @return this Chart
     */
    Chart plotOrientation(final String orientation);

}

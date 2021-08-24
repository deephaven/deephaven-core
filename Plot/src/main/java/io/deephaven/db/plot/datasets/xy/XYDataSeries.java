/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.plot.datasets.xy;

import io.deephaven.db.plot.datasets.DataSeries;
import io.deephaven.db.plot.datasets.data.IndexableData;
import io.deephaven.db.plot.filters.SelectableDataSet;
import io.deephaven.db.tables.Table;
import io.deephaven.gui.color.Paint;
import io.deephaven.gui.shape.Shape;

/**
 * {@link DataSeries} with two numerical components, x and y. Data points are numbered and are
 * accessed with an index.
 */
public interface XYDataSeries extends DataSeries {


    ////////////////////////// color //////////////////////////


    ////////////////////////// point sizes //////////////////////////


    /**
     * Sets the point size. A scale factor of 1 is the default size. A scale factor of 2 is 2x the
     * default size. Unspecified points use the default size.
     *
     * @param factors factors to multiply the default size (1) by
     * @return this XYDataSeries
     */
    XYDataSeries pointSize(IndexableData<Double> factors);

    /**
     * Sets the point size. A scale factor of 1 is the default size. A scale factor of 2 is 2x the
     * default size. Unspecified points use the default size.
     *
     * @param factors factors to multiply the default size (1) by
     * @return this XYDataSeries
     */
    XYDataSeries pointSize(int... factors);

    /**
     * Sets the point size. A scale factor of 1 is the default size. A scale factor of 2 is 2x the
     * default size. Unspecified points use the default size.
     *
     * @param factors factors to multiply the default size (1) by
     * @return this XYDataSeries
     */
    XYDataSeries pointSize(long... factors);

    /**
     * Sets the point size. A scale factor of 1 is the default size. A scale factor of 2 is 2x the
     * default size. Unspecified points use the default size.
     *
     * @param factors factors to multiply the default size (1) by
     * @return this XYDataSeries
     */
    XYDataSeries pointSize(double... factors);

    /**
     * Sets the point size. A scale factor of 1 is the default size. A scale factor of 2 is 2x the
     * default size. Unspecified points use the default size.
     *
     * @param factors factors to multiply the default size (1) by
     * @param <T> data type of the {@code factors}
     * @return this XYDataSeries
     */
    <T extends Number> XYDataSeries pointSize(T[] factors);

    /**
     * Sets the point size. A scale factor of 1 is the default size. A scale factor of 2 is 2x the
     * default size. Unspecified points use the default size.
     *
     * @param t table containing factors to multiply the default size (1) by
     * @param columnName column in {@code t} containing size scaling factors. The size data for
     *        point i comes from row i.
     * @return this XYDataSeries
     */
    XYDataSeries pointSize(Table t, String columnName);

    /**
     * Sets the point size. A scale factor of 1 is the default size. A scale factor of 2 is 2x the
     * default size. Unspecified points use the default size.
     *
     * @param sds selectable data set (e.g. OneClick filterable table) containing factors to
     *        multiply the default size (1) by
     * @param columnName column in {@code sds} containing size scaling factors. The size data for
     *        point i comes from row i.
     * @return this XYDataSeries
     */
    XYDataSeries pointSize(SelectableDataSet sds, String columnName);


    ////////////////////////// point colors //////////////////////////


    /**
     * Sets the point color. Unspecified points use the default color.
     *
     * @param colors colors. The color for data point i comes from index i.
     * @param <T> data type of the {@code colors}
     * @return this XYDataSeries
     */
    <T extends Paint> XYDataSeries pointColor(final IndexableData<T> colors);

    /**
     * Sets the point color. Unspecified points use the default color.
     *
     * @param colors colors. The color for data point i comes from index i.
     * @return this XYDataSeries
     */
    XYDataSeries pointColor(Paint... colors);

    /**
     * Sets the point color. Unspecified points use the default color.
     *
     * @param colors colors. The color for data point i comes from index i.
     * @return this XYDataSeries
     */
    XYDataSeries pointColorInteger(final IndexableData<Integer> colors);

    /**
     * Sets the point color. Unspecified points use the default color.
     *
     * @param colors color palette indices. The color for data point i comes from index i. A value
     *        of 3 corresponds to the 3rd color from the color pallette.
     * @return this XYDataSeries
     */
    XYDataSeries pointColor(int... colors);

    /**
     * Sets the point color. Unspecified points use the default color.
     *
     * @param colors color palette indices. The color for data point i comes from index i. A value
     *        of 3 corresponds to the 3rd color from the color pallette.
     * @return this XYDataSeries
     */
    XYDataSeries pointColor(Integer... colors);

    /**
     * Sets the point color. Unspecified points use the default color.
     *
     * @param colors color names. The color for data point i comes from index i.
     * @return this XYDataSeries
     */
    XYDataSeries pointColor(String... colors);

    /**
     * Sets the point color. Unspecified points use the default color.
     *
     * @param t table containing colors
     * @param columnName column in {@code t} containing colors. The color data for point i comes
     *        from row i.
     * @return this XYDataSeries
     */
    XYDataSeries pointColor(Table t, String columnName);

    /**
     * Sets the point color. Unspecified points use the default color.
     *
     * @param sds selectable data set (e.g. OneClick filterable table) containing colors
     * @param columnName column in {@code sds} containing colors. The color data for point i comes
     *        from row i.
     * @return this XYDataSeries
     */
    XYDataSeries pointColor(SelectableDataSet sds, String columnName);


    ////////////////////////// point labels //////////////////////////


    /**
     * Sets the point label for data point i from index i of the input labels. Points outside of
     * these indices are unlabeled.
     *
     * @param labels labels
     * @return this XYDataSeries
     */
    XYDataSeries pointLabel(IndexableData<?> labels);

    /**
     * Sets the point label for data point i from index i of the input labels. Points outside of
     * these indices are unlabeled.
     *
     * @param labels labels
     * @return this XYDataSeries
     */
    XYDataSeries pointLabel(Object... labels);

    /**
     * Sets the point label for data point i from index i of the input labels. Points outside of
     * these indices are unlabeled.
     *
     * @param t table containing labels
     * @param columnName column in {@code t} containing labels. The label data for point i comes
     *        from row i.
     * @return this XYDataSeries
     */
    XYDataSeries pointLabel(Table t, String columnName);

    /**
     * Sets the point label for data point i from index i of the input labels. Points outside of
     * these indices are unlabeled.
     *
     * @param sds selectable data set (e.g. OneClick filterable table) containing labels
     * @param columnName column in {@code sds} containing labels. The color data for point i comes
     *        from row i.
     * @return this XYDataSeries
     */
    XYDataSeries pointLabel(SelectableDataSet sds, String columnName);



    ////////////////////////// point shapes //////////////////////////


    /**
     * Sets the point shapes for data point i from index i of the input labels. Points outside of
     * these indices use default shapes.
     *
     * @param shapes shapes
     * @return this XYDataSeries
     */
    XYDataSeries pointShape(IndexableData<String> shapes);

    /**
     * Sets the point shapes for data point i from index i of the input labels. Points outside of
     * these indices use default shapes.
     *
     * @param shapes shapes
     * @return this XYDataSeries
     */
    XYDataSeries pointShape(String... shapes);

    /**
     * Sets the point shapes for data point i from index i of the input labels. Points outside of
     * these indices use default shapes.
     *
     * @param shapes shapes
     * @return this XYDataSeries
     */
    XYDataSeries pointShape(Shape... shapes);

    /**
     * Sets the point shapes for data point i from index i of the input labels. Points outside of
     * these indices use default shapes.
     *
     * @param t table containing shapes
     * @param columnName column in {@code t} containing shapes. The shape data for point i comes
     *        from row i.
     * @return this XYDataSeries
     */
    XYDataSeries pointShape(Table t, String columnName);

    /**
     * Sets the point shapes for data point i from index i of the input labels. Points outside of
     * these indices use default shapes.
     *
     * @param sds selectable data set (e.g. OneClick filterable table) containing shapes
     * @param columnName column in {@code sds} containing shapes. The color data for point i comes
     *        from row i.
     * @return this XYDataSeries
     */
    XYDataSeries pointShape(SelectableDataSet sds, String columnName);



    ////////////////////// tool tips /////////////////////////////


}

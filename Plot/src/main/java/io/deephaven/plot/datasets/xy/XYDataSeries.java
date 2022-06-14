/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.plot.datasets.xy;

import io.deephaven.plot.datasets.DataSeries;
import io.deephaven.plot.datasets.data.IndexableData;
import io.deephaven.plot.filters.SelectableDataSet;
import io.deephaven.engine.table.Table;
import io.deephaven.gui.color.Paint;
import io.deephaven.gui.shape.Shape;

/**
 * {@link DataSeries} with two numerical components, x and y. Data points are numbered and are accessed with an index.
 */
public interface XYDataSeries extends DataSeries {


    ////////////////////////// color //////////////////////////


    ////////////////////////// point sizes //////////////////////////


    /**
     * Sets the point size. A scale factor of 1 is the default size. A scale factor of 2 is 2x the default size.
     * Unspecified points use the default size.
     *
     * @param pointSizes factors to multiply the default size (1) by
     * @return this XYDataSeries
     */
    XYDataSeries pointSize(IndexableData<Double> pointSizes);

    /**
     * Sets the point size. A scale factor of 1 is the default size. A scale factor of 2 is 2x the default size.
     * Unspecified points use the default size.
     *
     * @param pointSizes factors to multiply the default size (1) by
     * @return this XYDataSeries
     */
    XYDataSeries pointSize(int... pointSizes);

    /**
     * Sets the point size. A scale factor of 1 is the default size. A scale factor of 2 is 2x the default size.
     * Unspecified points use the default size.
     *
     * @param pointSizes factors to multiply the default size (1) by
     * @return this XYDataSeries
     */
    XYDataSeries pointSize(long... pointSizes);

    /**
     * Sets the point size. A scale factor of 1 is the default size. A scale factor of 2 is 2x the default size.
     * Unspecified points use the default size.
     *
     * @param pointSizes factors to multiply the default size (1) by
     * @return this XYDataSeries
     */
    XYDataSeries pointSize(double... pointSizes);

    /**
     * Sets the point size. A scale factor of 1 is the default size. A scale factor of 2 is 2x the default size.
     * Unspecified points use the default size.
     *
     * @param pointSizes factors to multiply the default size (1) by
     * @param <T> data type of the {@code factors}
     * @return this XYDataSeries
     */
    <T extends Number> XYDataSeries pointSize(T[] pointSizes);

    /**
     * Sets the point size. A scale factor of 1 is the default size. A scale factor of 2 is 2x the default size.
     * Unspecified points use the default size.
     *
     * @param t table containing factors to multiply the default size (1) by
     * @param pointSizes column in {@code t} containing size scaling factors. The size data for point i comes from row
     *        i.
     * @return this XYDataSeries
     */
    XYDataSeries pointSize(Table t, String pointSizes);

    /**
     * Sets the point size. A scale factor of 1 is the default size. A scale factor of 2 is 2x the default size.
     * Unspecified points use the default size.
     *
     * @param sds selectable data set (e.g. OneClick filterable table) containing factors to multiply the default size
     *        (1) by
     * @param pointSize column in {@code sds} containing size scaling factors. The size data for point i comes from row
     *        i.
     * @return this XYDataSeries
     */
    XYDataSeries pointSize(SelectableDataSet sds, String pointSize);


    ////////////////////////// point colors //////////////////////////


    /**
     * Sets the point color. Unspecified points use the default color.
     *
     * @param pointColor colors. The color for data point i comes from index i.
     * @param <T> data type of the {@code colors}
     * @return this XYDataSeries
     */
    <T extends Paint> XYDataSeries pointColor(final IndexableData<T> pointColor);

    /**
     * Sets the point color. Unspecified points use the default color.
     *
     * @param pointColor colors. The color for data point i comes from index i.
     * @return this XYDataSeries
     */
    XYDataSeries pointColor(Paint... pointColor);

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
     * @param pointColors color palette indices. The color for data point i comes from index i. A value of 3 corresponds
     *        to the 3rd color from the color pallette.
     * @return this XYDataSeries
     */
    XYDataSeries pointColor(int... pointColors);

    /**
     * Sets the point color. Unspecified points use the default color.
     *
     * @param pointColors color palette indices. The color for data point i comes from index i. A value of 3 corresponds
     *        to the 3rd color from the color pallette.
     * @return this XYDataSeries
     */
    XYDataSeries pointColor(Integer... pointColors);

    /**
     * Sets the point color. Unspecified points use the default color.
     *
     * @param pointColors color names. The color for data point i comes from index i.
     * @return this XYDataSeries
     */
    XYDataSeries pointColor(String... pointColors);

    /**
     * Sets the point color. Unspecified points use the default color.
     *
     * @param t table containing colors
     * @param pointColors column in {@code t} containing colors. The color data for point i comes from row i.
     * @return this XYDataSeries
     */
    XYDataSeries pointColor(Table t, String pointColors);

    /**
     * Sets the point color. Unspecified points use the default color.
     *
     * @param sds selectable data set (e.g. OneClick filterable table) containing colors
     * @param pointColors column in {@code sds} containing colors. The color data for point i comes from row i.
     * @return this XYDataSeries
     */
    XYDataSeries pointColor(SelectableDataSet sds, String pointColors);


    ////////////////////////// point labels //////////////////////////


    /**
     * Sets the point label for data point i from index i of the input labels. Points outside of these indices are
     * unlabeled.
     *
     * @param pointLabels labels
     * @return this XYDataSeries
     */
    XYDataSeries pointLabel(IndexableData<?> pointLabels);

    /**
     * Sets the point label for data point i from index i of the input labels. Points outside of these indices are
     * unlabeled.
     *
     * @param pointLabels labels
     * @return this XYDataSeries
     */
    XYDataSeries pointLabel(Object... pointLabels);

    /**
     * Sets the point label for data point i from index i of the input labels. Points outside of these indices are
     * unlabeled.
     *
     * @param t table containing labels
     * @param pointLabel column in {@code t} containing labels. The label data for point i comes from row i.
     * @return this XYDataSeries
     */
    XYDataSeries pointLabel(Table t, String pointLabel);

    /**
     * Sets the point label for data point i from index i of the input labels. Points outside of these indices are
     * unlabeled.
     *
     * @param sds selectable data set (e.g. OneClick filterable table) containing labels
     * @param pointLabel column in {@code sds} containing labels. The color data for point i comes from row i.
     * @return this XYDataSeries
     */
    XYDataSeries pointLabel(SelectableDataSet sds, String pointLabel);



    ////////////////////////// point shapes //////////////////////////


    /**
     * Sets the point shapes for data point i from index i of the input labels. Points outside of these indices use
     * default shapes.
     *
     * @param pointShapes shapes
     * @return this XYDataSeries
     */
    XYDataSeries pointShape(IndexableData<String> pointShapes);

    /**
     * Sets the point shapes for data point i from index i of the input labels. Points outside of these indices use
     * default shapes.
     *
     * @param pointShapes shapes
     * @return this XYDataSeries
     */
    XYDataSeries pointShape(String... pointShapes);

    /**
     * Sets the point shapes for data point i from index i of the input labels. Points outside of these indices use
     * default shapes.
     *
     * @param pointShapes shapes
     * @return this XYDataSeries
     */
    XYDataSeries pointShape(Shape... pointShapes);

    /**
     * Sets the point shapes for data point i from index i of the input labels. Points outside of these indices use
     * default shapes.
     *
     * @param t table containing shapes
     * @param pointShape column in {@code t} containing shapes. The shape data for point i comes from row i.
     * @return this XYDataSeries
     */
    XYDataSeries pointShape(Table t, String pointShape);

    /**
     * Sets the point shapes for data point i from index i of the input labels. Points outside of these indices use
     * default shapes.
     *
     * @param sds selectable data set (e.g. OneClick filterable table) containing shapes
     * @param pointShape column in {@code sds} containing shapes. The color data for point i comes from row i.
     * @return this XYDataSeries
     */
    XYDataSeries pointShape(SelectableDataSet sds, String pointShape);



    ////////////////////// tool tips /////////////////////////////


}

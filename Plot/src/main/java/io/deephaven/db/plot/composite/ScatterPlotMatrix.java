/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.plot.composite;

import io.deephaven.base.verify.Require;
import io.deephaven.db.plot.Figure;
import io.deephaven.db.plot.FigureFactory;
import io.deephaven.db.plot.FigureImpl;
import io.deephaven.db.plot.PlotStyle;
import io.deephaven.db.plot.datasets.data.*;
import io.deephaven.db.plot.errors.PlotExceptionCause;
import io.deephaven.db.plot.errors.PlotIllegalArgumentException;
import io.deephaven.db.plot.errors.PlotInfo;
import io.deephaven.db.plot.filters.SelectableDataSet;
import io.deephaven.db.plot.util.ArgumentValidations;
import io.deephaven.db.tables.Table;

import java.util.Arrays;
import java.util.stream.IntStream;

import static io.deephaven.util.QueryConstants.NULL_INT;
import static io.deephaven.util.QueryConstants.NULL_LONG;

/**
 * Creates a scatter plot matrix by graphing each variable against every other variable.
 */
public class ScatterPlotMatrix extends FigureImpl implements PlotExceptionCause {

    private static final long serialVersionUID = 2502045888378915453L;
    private final FigureImpl figure;
    private final int numCols;

    private ScatterPlotMatrix(final FigureImpl figure, final int numCols) {
        super(figure);
        this.figure = figure;
        this.numCols = numCols;
    }

    @Override
    public PlotInfo getPlotInfo() {
        return figure.getFigure().getPlotInfo();
    }

    // copy constructor
    private ScatterPlotMatrix(final ScatterPlotMatrix spm) {
        this(spm, spm.numCols);
    }

    /**
     * Sets the default point size of all plots in this ScatterPlotMatrix.
     *
     * @param factor point size
     * @return this ScatterPlotMatrix
     */
    public ScatterPlotMatrix pointSize(final int factor) {
        return pointSize(factor == NULL_INT ? null : factor);
    }

    /**
     * Sets the default point size of all plots in this ScatterPlotMatrix.
     *
     * @param factor point size
     * @return this ScatterPlotMatrix
     */
    public ScatterPlotMatrix pointSize(final long factor) {
        return pointSize(factor == NULL_LONG ? null : factor);
    }

    /**
     * Sets the default point size of all plots in this ScatterPlotMatrix.
     *
     * @param factor point size
     * @return this ScatterPlotMatrix
     */
    public ScatterPlotMatrix pointSize(double factor) {
        FigureImpl result = new FigureImpl(this);

        for (int i = 0; i < numCols * numCols; i++) {
            result = result.chart(i).axes(0).series(0).pointSize(factor);
        }

        return new ScatterPlotMatrix(result, this.numCols);
    }

    /**
     * Sets the default point size of all plots in this ScatterPlotMatrix.
     *
     * @param factor point size
     * @return this ScatterPlotMatrix
     */
    public ScatterPlotMatrix pointSize(final Number factor) {
        FigureImpl result = new FigureImpl(this);

        for (int i = 0; i < numCols; i++) {
            for (int j = 0; j < numCols; j++) {
                result = result.chart(i, j).axes(0).series(0).pointSize(factor);
            }
        }

        return new ScatterPlotMatrix(result, this.numCols);
    }

    /**
     * Sets the point size of the plot at index {@code plotIndex}.
     *
     * The index starts at 0 in the upper left hand corner of the grid and increases going left to
     * right, top to bottom. E.g. for a 2x2 ScatterPlotMatrix, the indices would be [0, 1] [2, 3]
     *
     * @param plotIndex index
     * @param factor point size
     * @return this ScatterPlotMatrix
     */
    public ScatterPlotMatrix pointSize(final int plotIndex, final int factor) {
        return pointSize(plotIndex, factor == NULL_INT ? null : factor);
    }

    /**
     * Sets the point size of the plot at index {@code plotIndex}.
     *
     * The index starts at 0 in the upper left hand corner of the grid and increases going left to
     * right, top to bottom. E.g. for a 2x2 ScatterPlotMatrix, the indices would be [0, 1] [2, 3]
     *
     * @param plotIndex index
     * @param factor point size
     * @return this ScatterPlotMatrix
     */
    public ScatterPlotMatrix pointSize(final int plotIndex, final long factor) {
        return pointSize(plotIndex, factor == NULL_LONG ? null : factor);
    }

    /**
     * Sets the point size of the plot at index {@code plotIndex}.
     *
     * The index starts at 0 in the upper left hand corner of the grid and increases going left to
     * right, top to bottom. E.g. for a 2x2 ScatterPlotMatrix, the indices would be [0, 1] [2, 3]
     *
     * @param plotIndex index
     * @param factor point size
     * @return this ScatterPlotMatrix
     */
    public ScatterPlotMatrix pointSize(final int plotIndex, double factor) {
        if (plotIndex < 0 || plotIndex >= this.numCols * this.numCols) {
            throw new PlotIllegalArgumentException("Plot index out of bounds", this);
        }

        final FigureImpl result = new FigureImpl(this)
            .chart(plotIndex)
            .axes(0)
            .series(0)
            .pointSize(factor);

        return new ScatterPlotMatrix(result, this.numCols);
    }

    /**
     * Sets the point size of the plot at index {@code plotIndex}.
     *
     * The index starts at 0 in the upper left hand corner of the grid and increases going left to
     * right, top to bottom. E.g. for a 2x2 ScatterPlotMatrix, the indices would be [0, 1] [2, 3]
     *
     * @param plotIndex index
     * @param factor point size
     * @return this ScatterPlotMatrix
     */
    public ScatterPlotMatrix pointSize(final int plotIndex, final Number factor) {
        if (plotIndex < 0 || plotIndex >= this.numCols * this.numCols) {
            throw new PlotIllegalArgumentException("Plot index out of bounds", this);
        }

        final FigureImpl result = new FigureImpl(this)
            .chart(plotIndex)
            .axes(0)
            .series(0)
            .pointSize(factor);

        return new ScatterPlotMatrix(result, this.numCols);
    }

    /**
     * Sets the point size of the plot at index {@code plotIndex}.
     *
     * Row and column numbers start at 0 in the upper left hand corner of the grid and increase
     * going top to bottom and left to right respectively. For example, in a 2x2 ScatterPlotMatrix
     * the coordinates would be [(0,0), (0,1)] [(1,0), (1,1)]
     *
     * @param row row index of this Figure's grid
     * @param col column index of this Figure's grid
     * @param factor point size
     * @return this ScatterPlotMatrix
     */
    public ScatterPlotMatrix pointSize(final int row, final int col, final int factor) {
        return pointSize(row, col, factor == NULL_INT ? null : factor);
    }

    /**
     * Sets the point size of the plot at index {@code plotIndex}.
     *
     * Row and column numbers start at 0 in the upper left hand corner of the grid and increase
     * going top to bottom and left to right respectively. For example, in a 2x2 ScatterPlotMatrix
     * the coordinates would be [(0,0), (0,1)] [(1,0), (1,1)]
     *
     * @param row row index of this Figure's grid
     * @param col column index of this Figure's grid
     * @param factor point size
     * @return this ScatterPlotMatrix
     */
    public ScatterPlotMatrix pointSize(final int row, final int col, final long factor) {
        return pointSize(row, col, factor == NULL_LONG ? null : factor);
    }

    /**
     * Sets the point size of the plot at index {@code plotIndex}.
     *
     * Row and column numbers start at 0 in the upper left hand corner of the grid and increase
     * going top to bottom and left to right respectively. For example, in a 2x2 ScatterPlotMatrix
     * the coordinates would be [(0,0), (0,1)] [(1,0), (1,1)]
     *
     * @param row row index of this Figure's grid
     * @param col column index of this Figure's grid
     * @param factor point size
     * @return this ScatterPlotMatrix
     */
    public ScatterPlotMatrix pointSize(final int row, final int col, double factor) {
        final int plotIndex = row * numCols + col;
        return pointSize(plotIndex, factor);
    }

    /**
     * Sets the point size of the plot at index {@code plotIndex}.
     *
     * Row and column numbers start at 0 in the upper left hand corner of the grid and increase
     * going top to bottom and left to right respectively. For example, in a 2x2 ScatterPlotMatrix
     * the coordinates would be [(0,0), (0,1)] [(1,0), (1,1)]
     *
     * @param row row index of this Figure's grid
     * @param col column index of this Figure's grid
     * @param factor point size
     * @return this ScatterPlotMatrix
     */
    public ScatterPlotMatrix pointSize(final int row, final int col, final Number factor) {
        final int plotIndex = row * numCols + col;
        return pointSize(plotIndex, factor);
    }

    /**
     * Sets the point size of plot i as the factor in {@code factors} at index i.
     *
     * The index starts at 0 in the upper left hand corner of the grid and increases going left to
     * right, top to bottom. E.g. for a 2x2 ScatterPlotMatrix, the indices would be [0, 1] [2, 3]
     *
     * @throws io.deephaven.base.verify.RequirementFailure {@code factors} must not be null. The
     *         number of {@code factors} must be equal to the number of plots.
     * @param factors point sizes
     * @return this ScatterPlotMatrix
     */
    public ScatterPlotMatrix pointSize(final IndexableData<Double> factors) {
        Require.neqNull(factors, "factors");
        Require.eq(factors.size(), "number of factors", this.numCols * this.numCols,
            "number of plots");

        FigureImpl result = new FigureImpl(this);

        for (int i = 0; i < this.numCols * this.numCols; i++) {
            result = result.chart(i).axes(0).series(0).pointSize(factors.get(i));
        }

        return new ScatterPlotMatrix(result, this.numCols);
    }

    /**
     * Sets the point size of plot i as the factor in {@code factors} at index i.
     *
     * The index starts at 0 in the upper left hand corner of the grid and increases going left to
     * right, top to bottom. E.g. for a 2x2 ScatterPlotMatrix, the indices would be [0, 1] [2, 3]
     *
     * @throws io.deephaven.base.verify.RequirementFailure {@code factors} must not be null. The
     *         number of {@code factors} must be equal to the number of plots.
     * @param factors point sizes
     * @return this ScatterPlotMatrix
     */
    public ScatterPlotMatrix pointSize(final int... factors) {
        return pointSize(new IndexableDataDouble(factors, true, getPlotInfo()));
    }

    /**
     * Sets the point size of plot i as the factor in {@code factors} at index i.
     *
     * The index starts at 0 in the upper left hand corner of the grid and increases going left to
     * right, top to bottom. E.g. for a 2x2 ScatterPlotMatrix, the indices would be [0, 1] [2, 3]
     *
     * @throws io.deephaven.base.verify.RequirementFailure {@code factors} must not be null. The
     *         number of {@code factors} must be equal to the number of plots.
     * @param factors point sizes
     * @return this ScatterPlotMatrix
     */
    public ScatterPlotMatrix pointSize(final long... factors) {
        return pointSize(new IndexableDataDouble(factors, true, getPlotInfo()));
    }

    /**
     * Sets the point size of plot i as the factor in {@code factors} at index i.
     *
     * The index starts at 0 in the upper left hand corner of the grid and increases going left to
     * right, top to bottom. E.g. for a 2x2 ScatterPlotMatrix, the indices would be [0, 1] [2, 3]
     *
     * @throws io.deephaven.base.verify.RequirementFailure {@code factors} must not be null. The
     *         number of {@code factors} must be equal to the number of plots.
     * @param factors point sizes
     * @return this ScatterPlotMatrix
     */
    public ScatterPlotMatrix pointSize(final double... factors) {
        return pointSize(new IndexableDataDouble(factors, true, getPlotInfo()));
    }

    /**
     * Sets the point size of plot i as the factor in {@code factors} at index i.
     *
     * The index starts at 0 in the upper left hand corner of the grid and increases going left to
     * right, top to bottom. E.g. for a 2x2 ScatterPlotMatrix, the indices would be [0, 1] [2, 3]
     *
     * @throws io.deephaven.base.verify.RequirementFailure {@code factors} must not be null. The
     *         number of {@code factors} must be equal to the number of plots.
     * @param factors point sizes
     * @return this ScatterPlotMatrix
     */
    public <T extends Number> ScatterPlotMatrix pointSize(final T[] factors) {
        return pointSize(new IndexableDataDouble(factors, true, getPlotInfo()));
    }


    // these functions return an object which extends figure so that we can add ScatterPlotMatrix
    // specific modifier methods later.

    /**
     * Creates a scatter plot matrix by graphing each variable against every other variable.
     *
     * @param variables data to plot
     * @param <T> data type of the {@code variables}
     * @return new {@link Figure} containing the scatter plot matrix where variable names are
     *         assigned as x1, x2, ... in order.
     */
    @SuppressWarnings("unchecked")
    public static <T extends Number> ScatterPlotMatrix scatterPlotMatrix(final T[]... variables) {
        Require.neqNull(variables, "variables");

        final String[] variableNames =
            IntStream.range(0, variables.length).mapToObj(i -> "x" + i).toArray(String[]::new);
        return scatterPlotMatrix(variableNames, variables);
    }

    /**
     * Creates a scatter plot matrix by graphing each variable against every other variable.
     *
     * @param variableNames variable names
     * @param variables data to plot
     * @param <T> data type of the {@code variables}
     * @return new {@link Figure} containing the scatter plot matrix
     */
    @SuppressWarnings("unchecked")
    public static <T extends Number> ScatterPlotMatrix scatterPlotMatrix(
        final String[] variableNames, final T[]... variables) {
        final IndexableNumericData[] data =
            Arrays.stream(variables).map(x -> new IndexableNumericDataArrayNumber(x, null))
                .toArray(IndexableNumericData[]::new);
        return scatterPlotMatrix(variableNames, data);
    }

    /**
     * Creates a scatter plot matrix by graphing each variable against every other variable.
     *
     * @param variables data to plot
     * @return new {@link Figure} containing the scatter plot matrix where variable names are
     *         assigned as x1, x2, ... in order.
     */
    public static ScatterPlotMatrix scatterPlotMatrix(final int[]... variables) {
        Require.neqNull(variables, "variables");

        final String[] variableNames =
            IntStream.range(0, variables.length).mapToObj(i -> "x" + i).toArray(String[]::new);
        return scatterPlotMatrix(variableNames, variables);
    }

    /**
     * Creates a scatter plot matrix by graphing each variable against every other variable.
     *
     * @param variableNames variable names
     * @param variables data to plot
     * @return new {@link Figure} containing the scatter plot matrix
     */
    public static ScatterPlotMatrix scatterPlotMatrix(final String[] variableNames,
        final int[]... variables) {
        Require.neqNull(variables, "variables");

        final IndexableNumericData[] data =
            Arrays.stream(variables).map(x -> new IndexableNumericDataArrayInt(x, null))
                .toArray(IndexableNumericData[]::new);
        return scatterPlotMatrix(variableNames, data);
    }

    /**
     * Creates a scatter plot matrix by graphing each variable against every other variable.
     *
     * @param variables data to plot
     * @return new {@link Figure} containing the scatter plot matrix where variable names are
     *         assigned as x1, x2, ... in order.
     */
    public static ScatterPlotMatrix scatterPlotMatrix(final long[]... variables) {
        Require.neqNull(variables, "variables");

        final String[] variableNames =
            IntStream.range(0, variables.length).mapToObj(i -> "x" + i).toArray(String[]::new);
        return scatterPlotMatrix(variableNames, variables);
    }

    /**
     * Creates a scatter plot matrix by graphing each variable against every other variable.
     *
     * @param variableNames variable names
     * @param variables data to plot
     * @return new {@link Figure} containing the scatter plot matrix
     */
    public static ScatterPlotMatrix scatterPlotMatrix(final String[] variableNames,
        final long[]... variables) {
        final IndexableNumericData[] data =
            Arrays.stream(variables).map(x -> new IndexableNumericDataArrayLong(x, null))
                .toArray(IndexableNumericData[]::new);
        return scatterPlotMatrix(variableNames, data);
    }

    /**
     * Creates a scatter plot matrix by graphing each variable against every other variable.
     *
     * @param variables data to plot
     * @return new {@link Figure} containing the scatter plot matrix where variable names are
     *         assigned as x1, x2, ... in order.
     */
    public static ScatterPlotMatrix scatterPlotMatrix(final float[]... variables) {
        Require.neqNull(variables, "variables");

        final String[] variableNames =
            IntStream.range(0, variables.length).mapToObj(i -> "x" + i).toArray(String[]::new);
        return scatterPlotMatrix(variableNames, variables);
    }

    /**
     * Creates a scatter plot matrix by graphing each variable against every other variable.
     *
     * @param variableNames variable names
     * @param variables data to plot
     * @return new {@link Figure} containing the scatter plot matrix
     */
    public static ScatterPlotMatrix scatterPlotMatrix(final String[] variableNames,
        final float[]... variables) {
        final IndexableNumericData[] data =
            Arrays.stream(variables).map(x -> new IndexableNumericDataArrayFloat(x, null))
                .toArray(IndexableNumericData[]::new);
        return scatterPlotMatrix(variableNames, data);
    }

    /**
     * Creates a scatter plot matrix by graphing each variable against every other variable.
     *
     * @param variables data to plot
     * @return new {@link Figure} containing the scatter plot matrix where variable names are
     *         assigned as x1, x2, ... in order.
     */
    public static ScatterPlotMatrix scatterPlotMatrix(final double[]... variables) {
        Require.neqNull(variables, "variables");

        final String[] variableNames =
            IntStream.range(0, variables.length).mapToObj(i -> "x" + i).toArray(String[]::new);
        return scatterPlotMatrix(variableNames, variables);
    }

    /**
     * Creates a scatter plot matrix by graphing each variable against every other variable.
     *
     * @param variableNames variable names
     * @param variables data to plot
     * @return new {@link Figure} containing the scatter plot matrix
     */
    public static ScatterPlotMatrix scatterPlotMatrix(final String[] variableNames,
        final double[]... variables) {
        final IndexableNumericData[] data =
            Arrays.stream(variables).map(x -> new IndexableNumericDataArrayDouble(x, null))
                .toArray(IndexableNumericData[]::new);
        return scatterPlotMatrix(variableNames, data);
    }

    private static ScatterPlotMatrix scatterPlotMatrix(final String[] variableNames,
        final IndexableNumericData[] columns) {
        Require.neqNull(variableNames, "variableNames");
        Require.neqNull(columns, "columns");

        Figure fig = FigureFactory.figure(columns.length, columns.length);

        Require.eqTrue(fig instanceof FigureImpl, "fig instanceof FigureImpl");
        ArgumentValidations.assertSameSize(columns, variableNames,
            ((FigureImpl) fig).getFigure().getPlotInfo());

        int subPlotNum = 0;

        for (int i = 0; i < columns.length; i++) {
            for (int j = 0; j < columns.length; j++) {
                fig = fig.newChart(subPlotNum)
                    .legendVisible(false);
                if (i == 0) {
                    fig = fig.chartTitle(variableNames[j]);
                }

                fig = fig.newAxes().plotStyle(PlotStyle.SCATTER);
                if (j == 0) {
                    fig = fig.yLabel(variableNames[i]);
                }

                fig = fig.plot("" + subPlotNum, columns[i], columns[j], false, false);

                subPlotNum++;
            }
        }

        return new ScatterPlotMatrix((FigureImpl) fig, columns.length);
    }

    /**
     * Creates a scatter plot matrix by graphing each variable against every other variable.
     *
     * @param t table
     * @param columns data to plot
     * @return new {@link Figure} containing the scatter plot matrix
     */
    public static ScatterPlotMatrix scatterPlotMatrix(final Table t, final String... columns) {
        Require.neqNull(t, "table");
        Require.neqNull(columns, "columns");

        Figure fig = FigureFactory.figure(columns.length, columns.length);
        int subPlotNum = 0;
        for (int i = 0; i < columns.length; i++) {
            for (int j = 0; j < columns.length; j++) {
                fig = fig.newChart(subPlotNum)
                    .legendVisible(false);
                if (i == 0) {
                    fig = fig.chartTitle(columns[j]);
                }

                fig = fig.newAxes().plotStyle(PlotStyle.SCATTER);
                if (j == 0) {
                    fig = fig.yLabel(columns[i]);
                }

                fig = fig.plot("" + subPlotNum, t, columns[i], columns[j]);

                subPlotNum++;
            }
        }

        return new ScatterPlotMatrix((FigureImpl) fig, columns.length);
    }

    /**
     * Creates a scatter plot matrix by graphing each variable against every other variable.
     *
     * @param sds selectable data set (e.g. OneClick filterable table)
     * @param columns data to plot
     * @return new {@link Figure} containing the scatter plot matrix
     */
    public static ScatterPlotMatrix scatterPlotMatrix(final SelectableDataSet sds,
        final String... columns) {
        Require.neqNull(sds, "sds");
        Require.neqNull(columns, "columns");

        Figure fig = FigureFactory.figure(columns.length, columns.length);
        int subPlotNum = 0;
        for (int i = 0; i < columns.length; i++) {
            for (int j = 0; j < columns.length; j++) {
                fig = fig.newChart(subPlotNum)
                    .legendVisible(false);
                if (i == 0) {
                    fig = fig.chartTitle(columns[j]);
                }

                fig = fig.newAxes().plotStyle(PlotStyle.SCATTER);
                if (j == 0) {
                    fig = fig.yLabel(columns[i]);
                }

                fig = fig.plot("" + subPlotNum, sds, columns[i], columns[j]);

                subPlotNum++;
            }
        }

        return new ScatterPlotMatrix((FigureImpl) fig, columns.length);
    }

}

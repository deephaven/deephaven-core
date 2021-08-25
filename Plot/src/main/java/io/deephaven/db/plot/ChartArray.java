/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.plot;

import io.deephaven.db.plot.errors.PlotIllegalArgumentException;
import io.deephaven.db.plot.errors.PlotInfo;
import io.deephaven.db.plot.errors.PlotUnsupportedOperationException;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * A grid of charts.
 */
public class ChartArray implements Serializable {
    private static final long serialVersionUID = 4786818836320350714L;
    private final java.util.List<ChartImpl> charts = new ArrayList<>();
    private int[][] takenIndices;
    private final PlotInfo plotInfo;
    private static int EMPTY_INDEX = -1;

    ChartArray(final int width, final int height, final PlotInfo plotInfo) {
        if (height < 1 || width < 1) {
            throw new PlotIllegalArgumentException("Grid must be at least 1x1", plotInfo);
        }

        takenIndices = new int[height][width];
        this.plotInfo = plotInfo;
        markAllNoCheck(takenIndices, 0, height, 0, width, EMPTY_INDEX);
    }

    /**
     * Gets the {@link Chart}s in this ChartArray.
     *
     * @return list of {@link Chart}s in this ChartArray
     */
    public List<ChartImpl> getCharts() {
        return charts;
    }

    void addChart(final ChartImpl chart) {
        int index = charts.size();
        markIndices(chart.row(), chart.column(), chart.rowSpan(), chart.colSpan(), index);
        charts.add(chart);
    }

    /**
     * Gets the index of the {@link Chart} at the specified location in this array.
     *
     * @param row row of the {@link Chart}
     * @param col column of the {@link Chart}
     * @return index of the {@link Chart} at the specified location in this array.
     */
    public int getIndex(final int row, final int col) {
        checkBounds(row, col);

        return takenIndices[row][col];
    }

    /**
     * Whether the index indicates that there is no {@link Chart} in that location.
     *
     * @param index index of the {@link Chart}
     * @return Whether the index indicates that there is no {@link Chart} in that location
     */
    public static boolean isEmpty(int index) {
        return index == EMPTY_INDEX;
    }

    int nextOpenIndex() {
        int index = 0;
        for (int[] takenIndice : takenIndices) {
            for (int aTakenIndice : takenIndice) {
                if (aTakenIndice == EMPTY_INDEX) {
                    return index;
                }
                index++;
            }
        }
        return EMPTY_INDEX;
    }

    void resize(final int width, final int height) {
        if (width < takenIndices[0].length || height < takenIndices.length) {
            throw new PlotUnsupportedOperationException("Can not resize ChartArray to be smaller", plotInfo);
        }

        final int[][] newArray = new int[height][width];
        markAllNoCheck(newArray, 0, height, 0, width, EMPTY_INDEX);
        for (int i = 0; i < takenIndices.length; i++) {
            System.arraycopy(takenIndices[i], 0, newArray[i], 0, takenIndices[i].length);
        }

        takenIndices = newArray;
    }

    /**
     * Gets the {@link Chart} at the specified location in this array.
     *
     * @param row row of the {@link Chart}
     * @param col column of the {@link Chart}
     * @return the {@link Chart} at the specified location in this array.
     */
    public ChartImpl getChart(final int row, final int col) {
        checkBounds(row, col);

        final int index = takenIndices[row][col];
        if (index < 0 || index >= charts.size()) {
            return null;
        }
        return charts.get(index);
    }

    void removeChart(final int row, final int col) {
        checkBounds(row, col);
        final int index = takenIndices[row][col];
        if (index < 0 || index > charts.size() - 1) {
            throw new PlotIllegalArgumentException(
                    "Can not remove chart at (" + row + ", " + col + ")- chart does not exist!", plotInfo);
        }

        final ChartImpl chart = charts.get(index);
        if (chart == null) {
            throw new PlotIllegalArgumentException(
                    "Can not remove chart at (" + row + ", " + col + ")- chart does not exist!", plotInfo);
        }

        markAllNoCheck(takenIndices, chart.row(), chart.row() + chart.rowSpan(), chart.column(),
                chart.column() + chart.colSpan(), EMPTY_INDEX);
    }

    void resizeChart(final int row, final int col, final int rowspan, final int colspan) {
        checkBounds(row + rowspan - 1, col + colspan - 1);
        final int index = takenIndices[row][col];
        markIndices(row, col, rowspan, colspan, index);
    }

    private void markIndices(final int row, final int col, final int rowspan, final int colspan, final int index) {
        // check first
        final int maxRow = row + rowspan;
        final int maxCol = col + colspan;
        checkBounds(maxRow - 1, maxCol - 1);

        for (int i = row; i < maxRow; i++) {
            for (int j = col; j < maxCol; j++) {
                if (takenIndices[i][j] != index && takenIndices[i][j] != EMPTY_INDEX) {
                    removeChart(row, col);
                }
            }
        }
        // if the chart is getting smaller, we fill the previous space with -1's
        if (!(index < 0 || index > charts.size() - 1)) {
            final ChartImpl chart = charts.get(index);
            if (chart != null && (rowspan < chart.rowSpan() || colspan < chart.colSpan())) {
                markAllNoCheck(takenIndices, chart.row(), chart.row() + chart.rowSpan(), chart.column(),
                        chart.column() + chart.colSpan(), EMPTY_INDEX);
            }
        }

        markAllNoCheck(takenIndices, row, maxRow, col, maxCol, index);
    }

    private static void markAllNoCheck(final int[][] matrix, final int rowMin, final int rowMax, final int colMin,
            final int colMax, final int index) {
        for (int i = rowMin; i < rowMax; i++) {
            for (int j = colMin; j < colMax; j++) {
                matrix[i][j] = index;
            }
        }
    }

    private void checkBounds(final int row, final int col) {
        if (row < 0 || col < 0) {
            throw new PlotIllegalArgumentException("Chart indices must be >0. row:" + row + " col:" + col, plotInfo);
        }

        if (row >= takenIndices.length || col >= takenIndices[0].length) {
            throw new PlotIllegalArgumentException(
                    "Chart is not in grid. Trying to access chart at position [" + (row + 1) + "x" + (col + 1)
                            + "], chart grid is [" + takenIndices.length + "x" + takenIndices[0].length + "]",
                    plotInfo);
        }
    }

    boolean isInitialized() {
        boolean initialized = true;
        for (final ChartImpl chart : charts) {
            initialized &= chart.isInitialized();
        }

        return initialized;
    }
}

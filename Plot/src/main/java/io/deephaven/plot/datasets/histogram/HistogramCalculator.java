/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.plot.datasets.histogram;

import io.deephaven.base.verify.Require;
import io.deephaven.function.DoubleFpPrimitives;
import io.deephaven.plot.errors.PlotInfo;
import io.deephaven.plot.errors.PlotUnsupportedOperationException;
import io.deephaven.plot.util.ArgumentValidations;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.util.TableTools;

import java.util.List;

import static io.deephaven.api.agg.Aggregation.*;

/**
 * Utility for calculating histogram plot information: bin locations and data frequencies within these bins.
 */
public class HistogramCalculator {

    private static Table clean(final Table t, final String histogramColumn, final PlotInfo plotInfo,
            final String... byColumns) {
        ArgumentValidations.assertIsNumeric(t, histogramColumn, "Histogram table", plotInfo);

        final String[] view = new String[byColumns == null ? 1 : byColumns.length + 1];
        int i = 0;

        if (ArgumentValidations.isBoxedNumeric(t, histogramColumn, plotInfo)) {
            view[i++] = "X= " + histogramColumn + "==null ? Double.NaN : " + histogramColumn + ".doubleValue()";
        } else if (ArgumentValidations.isPrimitiveNumeric(t, histogramColumn, plotInfo)) {
            view[i++] = "X= isNull(" + histogramColumn + ") ? Double.NaN : (double) " + histogramColumn;
        } else {
            throw new PlotUnsupportedOperationException("Unsupported histogram histogramColumn: histogramColumn="
                    + histogramColumn + " type=" + ArgumentValidations.getColumnType(t, histogramColumn, plotInfo),
                    plotInfo);
        }

        // add byColumns in groupBy columns
        if (byColumns != null && byColumns.length > 0) {
            System.arraycopy(byColumns, 0, view, i, byColumns.length);
        }

        return t
                .view(view)
                .where("io.deephaven.function.DoubleFpPrimitives.isNormal(X)");
    }

    private static Table counts(final Table data, final Table range, final String... byColumns) {

        final String[] groupByColumns = new String[byColumns != null ? byColumns.length + 1 : 1];
        groupByColumns[0] = "RangeIndex";

        if (byColumns != null && byColumns.length > 0) {
            System.arraycopy(byColumns, 0, groupByColumns, 1, byColumns.length);
        }

        return data.join(range)
                .updateView("RangeIndex = Range.index(X)")
                .where("!isNull(RangeIndex)")
                .aggBy(List.of(AggCount("Count"), AggLast("Range")), groupByColumns)
                .updateView("BinMin = Range.binMin(RangeIndex)", "BinMax = Range.binMax(RangeIndex)",
                        "BinMid=0.5*(BinMin+BinMax)");
    }

    private static Table range(final Table t, final int nbins) {
        return t.aggBy(List.of(AggMin("RangeMin=X"), AggMax("RangeMax=X"), AggCount("NSamples")))
                .update("Range = new io.deephaven.plot.datasets.histogram.DiscretizedRangeEqual(RangeMin, RangeMax, "
                        + nbins + ")")
                .view("Range");
    }

    private static Table range(final double rangeMin, final double rangeMax, final int nbins) {
        return TableTools.emptyTable(1)
                .update("Range = new io.deephaven.plot.datasets.histogram.DiscretizedRangeEqual(" + rangeMin
                        + ","
                        + rangeMax + "," + nbins + ")");
    }

    /**
     * Finds the minimum and maximum of the data in the {@code column} of the {@code table} and splits this range into
     * {@code nbins} equally sized bins. Calculates the number of data values in each bin.
     * <p>
     * Data which is not normal as defined in {@link DoubleFpPrimitives#isNormal} is filtered out of the data set.
     *
     * @throws io.deephaven.base.verify.RequirementFailure {@code table} and {@code column} must not be null
     * @throws RuntimeException {@code column} must be numeric
     * @param table table
     * @param column column in {@code table}
     * @param nbins number of bins
     * @param plotInfo plot information
     * @param byColumns other columns needed to calaculate histogram, these columns will be included while grouping
     * @return table holding the calculated bins and their counts
     * @throws io.deephaven.base.verify.RequirementFailure {@code table} and {@code column} must not be null
     * @throws RuntimeException {@code column} must be numeric
     */
    public static Table calc(final Table table, final String column, final int nbins, final PlotInfo plotInfo,
            final String... byColumns) {
        Require.neqNull(table, "table");
        Require.neqNull(column, "column");

        final Table data = clean(table, column, plotInfo, byColumns);
        final Table range = range(data, nbins);
        return counts(data, range, byColumns);
    }

    /**
     * Finds the minimum and maximum of the data in the {@code column} of the {@code table} and splits this range into
     * {@code nbins} equally sized bins. Calculates the number of data values in each bin.
     * <p>
     * Data which is not normal as defined in {@link DoubleFpPrimitives#isNormal} is filtered out of the data set.
     *
     * @param table table
     * @param column column in {@code table}
     * @param nbins number of bins
     * @param plotInfo plot information
     * @param byColumns other columns needed to calaculate histogram, these columns will be included while grouping
     * @return table holding the calculated bins and their counts
     * @throws io.deephaven.base.verify.RequirementFailure {@code table} and {@code column} must not be null
     * @throws RuntimeException {@code column} must be numeric
     */
    public static Table calc(final Table table, final String column, final int nbins, final PlotInfo plotInfo,
            final List<String> byColumns) {
        return calc(table, column, nbins, plotInfo,
                byColumns != null && !byColumns.isEmpty() ? byColumns.toArray(new String[byColumns.size()]) : null);
    }

    /**
     * Splits the specified range into {@code nbins} equally sized bins. Calculates the number of data values in each
     * bin.
     * <p>
     * Data which is not normal as defined in {@link DoubleFpPrimitives#isNormal} is filtered out of the data set.
     *
     * @param table table
     * @param histogramColumn histogramColumn in {@code table}
     * @param rangeMin range minimum
     * @param rangeMax range maximum
     * @param nbins number of bins
     * @param plotInfo plot information
     * @param byColumns other columns needed to calaculate histogram, these columns will be included while grouping
     * @return table holding the calculated bins and their counts
     * @throws io.deephaven.base.verify.RequirementFailure {@code table} and {@code histogramColumn} must not be null
     * @throws RuntimeException {@code histogramColumn} must be numeric
     */
    public static Table calc(final Table table, final String histogramColumn, final double rangeMin,
            final double rangeMax, final int nbins, final PlotInfo plotInfo, final String... byColumns) {
        ArgumentValidations.assertNotNull(table, "table", plotInfo);
        ArgumentValidations.assertNotNull(histogramColumn, "histogramColumn", plotInfo);

        final Table data = clean(table, histogramColumn, plotInfo, byColumns);
        final Table range = range(rangeMin, rangeMax, nbins);
        return counts(data, range, byColumns);
    }

    /**
     * Splits the specified range into {@code nbins} equally sized bins. Calculates the number of data values in each
     * bin.
     * <p>
     * Data which is not normal as defined in {@link DoubleFpPrimitives#isNormal} is filtered out of the data set.
     *
     * @param table table
     * @param histogramColumn histogramColumn in {@code table}
     * @param rangeMin range minimum
     * @param rangeMax range maximum
     * @param nbins number of bins
     * @param plotInfo plot information
     * @param byColumns other columns needed to calaculate histogram, these columns will be included while grouping
     * @return table holding the calculated bins and their counts
     * @throws io.deephaven.base.verify.RequirementFailure {@code table} and {@code histogramColumn} must not be null
     * @throws RuntimeException {@code histogramColumn} must be numeric
     */
    public static Table calc(final Table table, final String histogramColumn, final double rangeMin,
            final double rangeMax, final int nbins, final PlotInfo plotInfo, final List<String> byColumns) {
        return calc(table, histogramColumn, rangeMin, rangeMax, nbins, plotInfo,
                byColumns != null && !byColumns.isEmpty() ? byColumns.toArray(new String[byColumns.size()]) : null);
    }

}

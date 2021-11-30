package io.deephaven.plot;

import io.deephaven.plot.datasets.data.IndexableData;
import io.deephaven.plot.errors.PlotInfo;
import io.deephaven.plot.filters.SelectableDataSet;
import io.deephaven.plot.util.tables.*;
import io.deephaven.engine.table.Table;

import java.util.*;

/**
 * Dynamic chart title created from table columns
 * <p>
 * For each column, by default the title String takes 1 values. This is configurable either via property
 * "Plot.chartTitle.maxRowsInTitle" or {@link io.deephaven.plot.Chart#maxRowsInTitle(int)}
 * <p>
 * Also, the default format for the title is:
 * <p>
 * ${comma separated values}&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;${comma separated values},...
 * </p>
 * <p>
 * In order to customize this format, (a {@link java.text.MessageFormat} instance), please refer <br/>
 * {@link io.deephaven.plot.Chart#chartTitle(String, Table, String...)} and<br/>
 * {@link io.deephaven.plot.Chart#chartTitle(String, SelectableDataSet, String...)}
 */
public abstract class DynamicChartTitle extends ChartTitle {
    /**
     * Dynamic chart title prefix
     */
    String titleFormat;

    /**
     * Dynamic title string
     */
    String dynamicTitleString;

    DynamicChartTitle(final String titleFormat, final PlotInfo plotInfo, final int maxVisibleRowsCount) {
        super(plotInfo, maxVisibleRowsCount);
        this.titleFormat = titleFormat;
    }

    /**
     * Copies given {@link DynamicChartTitle} instance to this
     *
     * @param chartTitle instance to copy
     */
    void copy(final DynamicChartTitle chartTitle) {
        super.copy(chartTitle);
        this.titleFormat = chartTitle.titleFormat;
        this.dynamicTitleString = chartTitle.dynamicTitleString;
    }

    @Override
    public synchronized String getTitle() {
        return dynamicTitleString == null ? "" : dynamicTitleString;
    }


    /**
     * An abstract class for dynamic chart title from table and oneClick dataset
     */
    public static abstract class DynamicChartTitleTable extends DynamicChartTitle {

        private final Set<String> titleColumns;


        DynamicChartTitleTable(final String titleFormat, final Set<String> titleColumns, final PlotInfo plotInfo,
                final int maxVisibleRowsCount) {
            super(titleFormat, plotInfo, maxVisibleRowsCount);
            this.titleColumns = titleColumns;
        }

        /**
         * Copies given {@link DynamicChartTitle} instance to this
         *
         * @param chartTitle instance to copy
         */
        void copy(final DynamicChartTitleTable chartTitle) {
            super.copy(chartTitle);
            this.titleColumns.addAll(chartTitle.titleColumns);
        }

        static String defaultTitleFormatWithColumnNames(final String... titleColumns) {

            if (titleColumns != null && titleColumns.length > 0) {

                final StringBuilder sb = new StringBuilder();

                for (int i = 0; i < titleColumns.length - 1; i++) {
                    final String titleColumn = titleColumns[i];
                    sb.append(titleColumn).append(": {").append(i).append("}        ");
                }

                for (int i = titleColumns.length - 1; i < titleColumns.length; i++) {
                    final String titleColumn = titleColumns[i];
                    sb.append(titleColumn).append(": {").append(i).append("}");
                }

                return sb.toString();
            }
            return "";
        }

        static String defaultTitleFormat(final String... titleColumns) {

            if (titleColumns != null && titleColumns.length > 0) {

                final StringBuilder sb = new StringBuilder();

                for (int i = 0; i < titleColumns.length; i++) {
                    sb.append("{").append(i).append("}").append(i == titleColumns.length - 1 ? "" : "        ");
                }

                return sb.toString();
            }
            return "";
        }
    }


    /**
     * A dynamic chart title from oneClick datasets
     */
    public static class ChartTitleSwappableTable extends DynamicChartTitleTable {

        /**
         * Swappable table for chart title
         */
        private SwappableTable swappableTable;
        private TableMapHandle tableMapHandle;

        /**
         * Table updated on oneClick filter change
         */
        private transient Table localTable;


        ChartTitleSwappableTable(final String titleFormat, final SwappableTable swappableTable, final PlotInfo plotInfo,
                final int maxVisibleRowsCount, final String... titleColumns) {
            super(titleFormat, new LinkedHashSet<>(Arrays.asList(titleColumns)), plotInfo, maxVisibleRowsCount);
            this.swappableTable = swappableTable;

            if (swappableTable instanceof SwappableTableMap) {
                this.tableMapHandle = ((SwappableTableMap) swappableTable).getTableMapHandle();
            }
        }

        /**
         * Copies given {@link ChartTitleSwappableTable} instance to this
         *
         * @param chartTitle instance to copy
         */
        void copy(final ChartTitleSwappableTable chartTitle) {
            super.copy(chartTitle);
            this.swappableTable = chartTitle.swappableTable;
            this.tableMapHandle = chartTitle.tableMapHandle;
            this.localTable = chartTitle.localTable;
        }

        public SwappableTable getSwappableTable() {
            return swappableTable;
        }

        public TableMapHandle getTableMapHandle() {
            return tableMapHandle;
        }
    }


    /**
     * Gets the string value with handling nulls
     *
     * @param indexableData - indexed data source
     * @param index - index
     * @return String value of the indexed item
     */
    private static String getStringValue(final IndexableData indexableData, final int index) {
        return indexableData.get(index) == null ? "" : indexableData.get(index).toString();
    }

    /**
     * A dynamic chart title from {@link Table}
     */
    public static class ChartTitleTable extends DynamicChartTitleTable {

        /**
         * Table handle for chart title
         */
        private TableHandle tableHandle;

        public TableHandle getTableHandle() {
            return tableHandle;
        }

        ChartTitleTable(final String titleFormat, final TableHandle tableHandle, final PlotInfo plotInfo,
                final int maxVisibleRowsCount, final String... titleColumns) {
            super(titleFormat, new LinkedHashSet<>(Arrays.asList(titleColumns)), plotInfo, maxVisibleRowsCount);
            this.tableHandle = tableHandle;
        }


    }
}

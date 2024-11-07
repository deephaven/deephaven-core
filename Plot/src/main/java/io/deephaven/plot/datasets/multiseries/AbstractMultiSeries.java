//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.plot.datasets.multiseries;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.table.PartitionedTable;
import io.deephaven.io.logger.Logger;
import io.deephaven.plot.*;
import io.deephaven.plot.datasets.ColumnNameConstants;
import io.deephaven.plot.datasets.DataSeriesInternal;
import io.deephaven.plot.datasets.DynamicSeriesNamer;
import io.deephaven.plot.errors.PlotIllegalStateException;
import io.deephaven.plot.errors.PlotRuntimeException;
import io.deephaven.plot.errors.PlotUnsupportedOperationException;
import io.deephaven.plot.util.ArgumentValidations;
import io.deephaven.plot.util.functions.ClosureFunction;
import io.deephaven.engine.table.impl.*;
import groovy.lang.Closure;

import io.deephaven.internal.log.LoggerFactory;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;

import static io.deephaven.engine.util.TableTools.emptyTable;

/**
 * Creates and holds a {@link DataSeriesInternal} for every key in a {@link PartitionedTable}.
 */
@SuppressWarnings("SynchronizeOnNonFinalField")
public abstract class AbstractMultiSeries<SERIES extends DataSeriesInternal> extends AbstractSeriesInternal
        implements MultiSeriesInternal<SERIES>, TableSnapshotSeries {
    private static final long serialVersionUID = 3548896765688007362L;
    private static final Logger log = LoggerFactory.getLogger(AbstractMultiSeries.class);

    protected static final PartitionedTable EMPTY_PARTITIONED_TABLE = emptyTable(0).partitionBy();
    protected final String[] byColumns;

    protected transient Object partitionedTableLock;
    protected transient PartitionedTable partitionedTable;

    private transient Object seriesLock;
    private transient List<SERIES> series;
    private transient Set<Object> seriesKeys;
    private transient Map<String, Object> seriesNames;

    private final transient java.util.function.Function<Object, String> DEFAULT_NAMING_FUNCTION = key -> {
        final String keyString;
        if (key instanceof Object[]) {
            final Object[] keyArray = (Object[]) key;
            if (keyArray.length == 1) {
                keyString = Objects.toString(keyArray[0]);
            } else {
                keyString = Arrays.toString(keyArray);
            }
        } else {
            keyString = Objects.toString(key);
        }
        return name() + ": " + keyString;
    };

    transient java.util.function.Function<Object, String> namingFunction;

    private DynamicSeriesNamer seriesNamer;
    private transient Object seriesNamerLock;

    private boolean allowInitialization = false;
    protected boolean initialized;

    private String seriesNameColumnName = null;

    /**
     * Creates a MultiSeries instance.
     *
     * @param axes axes on which this {@link MultiSeries} will be plotted
     * @param id data series id
     * @param name series name
     * @param byColumns columns forming the keys of the partitioned table
     */
    AbstractMultiSeries(final AxesImpl axes, final int id, final Comparable name, final String[] byColumns) {
        super(axes, id, name);
        this.byColumns = byColumns;
        this.namingFunction = DEFAULT_NAMING_FUNCTION;

        initializeTransient();
    }

    /**
     * Creates a copy of a series using a different Axes.
     *
     * @param series series to copy.
     * @param axes new axes to use.
     */
    AbstractMultiSeries(final AbstractMultiSeries series, final AxesImpl axes) {
        super(series, axes);
        this.byColumns = series.byColumns;
        this.namingFunction = series.namingFunction;
        this.seriesNamer = series.seriesNamer;
        this.allowInitialization = series.allowInitialization;
        this.seriesNameColumnName = series.seriesNameColumnName;

        initializeTransient();
    }

    @Override
    public ChartImpl chart() {
        return axes().chart();
    }

    @Override
    public DynamicSeriesNamer getDynamicSeriesNamer() {
        return seriesNamer;
    }

    @Override
    public void setDynamicSeriesNamer(DynamicSeriesNamer seriesNamer) {
        this.seriesNamer = seriesNamer;
    }

    @Override
    public String[] getByColumns() {
        return byColumns;
    }

    String makeSeriesName(final String seriesName, final DynamicSeriesNamer seriesNamer) {
        // this would occur when server side datasets were being updated via OneClick
        // this should no longer happen
        if (seriesNamer == null) {
            throw new PlotIllegalStateException("seriesNamer null " + this, this);
        }

        synchronized (seriesNamerLock) {
            return seriesNamer.makeUnusedName(seriesName, getPlotInfo());
        }
    }

    @Override
    public AbstractMultiSeries<SERIES> seriesNamingFunction(
            final Function<Object, String> namingFunction) {
        if (namingFunction == null) {
            this.namingFunction = DEFAULT_NAMING_FUNCTION;
        } else {
            this.namingFunction = namingFunction;
        }

        applyNamingFunction(namingFunction);
        return this;
    }

    @Override
    public AbstractMultiSeries<SERIES> seriesNamingFunction(final Closure<String> namingFunction) {
        return seriesNamingFunction(namingFunction == null ? null : new ClosureFunction<>(namingFunction));
    }

    /**
     * This is used by super classes so we can call applyNamingFunction during construction without NPEs
     */
    protected void applyNamingFunction() {
        applyNamingFunction(namingFunction);
    }

    private void applyNamingFunction(final java.util.function.Function<Object, String> namingFunction) {
        ArgumentValidations.assertNotNull(namingFunction, "namingFunction", getPlotInfo());
        seriesNameColumnName = seriesNameColumnName == null
                ? ColumnNameConstants.SERIES_NAME + this.hashCode()
                : seriesNameColumnName;
        final String functionInput = byColumns.length > 1
                ? "new Object[] {" + String.join(", ", byColumns) + "}"
                : byColumns[0];
        applyFunction(namingFunction, seriesNameColumnName, functionInput, String.class);
    }

    /**
     * Applies the {@code function} to the {@code byColumns} of the underlying table to create a new column named
     * {@code columnName}.
     *
     * @param function The function to apply
     * @param columnName The column name to create
     * @param functionInput The formula string to use for gathering input to {@code function}
     * @param resultClass The expected result type of {@code function}
     */
    protected <T, R> void applyFunction(final java.util.function.Function<? super T, ? extends R> function,
            final String columnName, final String functionInput, final Class<R> resultClass) {
        ArgumentValidations.assertNotNull(function, "function", getPlotInfo());
        final String queryFunction = columnName + "Function";
        final Map<String, Object> params = new HashMap<>();
        params.put(queryFunction, function);

        final String update = columnName + " = (" + resultClass.getSimpleName() + ") "
                + queryFunction + ".apply(" + functionInput + ")";

        applyTransform(columnName, update, new Class[] {resultClass}, params, true);
    }

    @Override
    public void initializeSeries(SERIES series) {

    }

    private List<SERIES> getSeries() {
        if (series == null) {
            synchronized (seriesLock) {
                if (series != null) {
                    return series;
                }

                series = new CopyOnWriteArrayList<>();
            }
        }

        return series;
    }

    @Override
    // this should be run under a seriesLock
    public void addSeries(SERIES series, Object key) {
        Assert.assertion(Thread.holdsLock(seriesLock), "Thread.holdsLock(seriesLock)");

        if (seriesKeys == null) {
            seriesKeys = new HashSet<>();
        }

        if (seriesNames == null) {
            seriesNames = new HashMap<>();
        }

        if (seriesKeys.contains(key)) {
            log.warn("MultiSeries: attempting to add a series with the same key as an existing series.  key=" + key);
            return;
        }

        if (seriesNames.containsKey(series.name().toString())) {
            throw new PlotRuntimeException(
                    "MultiSeries: attempting to add a series with the same name as an existing series.   name="
                            + series.name() + "oldKey=" + seriesNames.containsKey(series.name()) + " newKey=" + key,
                    this);
        }

        initializeSeries(series);

        seriesKeys.add(key);
        seriesNames.put(series.name().toString(), key);
        getSeries().add(series);
    }

    @Override
    public SERIES get(final int series) {
        final int size = getSeries().size();

        if (series < 0 || series >= size) {
            throw new PlotRuntimeException("Series index is out of range: key=" + series + " range=[0," + size + "]",
                    this);
        }

        return getSeries().get(series);
    }

    @Override
    public int getSeriesCount() {
        return getSeries().size();
    }

    @Override
    public SERIES createSeries(String seriesName, final BaseTable t) {
        return createSeries(seriesName, t, seriesNamer);
    }

    private void initializeTransient() {
        this.partitionedTableLock = new Object();
        this.seriesLock = new Object();
        this.seriesNamerLock = new Object();
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        initializeTransient();
    }

    ////////////////////////////// CODE BELOW HERE IS GENERATED -- DO NOT EDIT BY HAND //////////////////////////////
    ////////////////////////////// TO REGENERATE RUN GenerateMultiSeries //////////////////////////////
    ////////////////////////////// AND THEN RUN GenerateFigureImmutable //////////////////////////////
// @formatter:off

    @Override public <COLOR extends io.deephaven.gui.color.Paint> AbstractMultiSeries<SERIES> pointColor(final groovy.lang.Closure<COLOR> pointColor, final Object... multiSeriesKey) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointColor for arguments [groovy.lang.Closure<COLOR>]. If you think this method should work, try placing your multiSeriesKey into an Object array", this);
    }



    @Override public <COLOR extends io.deephaven.gui.color.Paint> AbstractMultiSeries<SERIES> pointColor(final java.util.function.Function<java.lang.Comparable, COLOR> pointColor, final Object... multiSeriesKey) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointColor for arguments [java.util.function.Function<java.lang.Comparable, COLOR>]. If you think this method should work, try placing your multiSeriesKey into an Object array", this);
    }



    @Override public <COLOR extends java.lang.Integer> AbstractMultiSeries<SERIES> pointColorInteger(final groovy.lang.Closure<COLOR> colors, final Object... multiSeriesKey) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointColorInteger for arguments [groovy.lang.Closure<COLOR>]. If you think this method should work, try placing your multiSeriesKey into an Object array", this);
    }



    @Override public <COLOR extends java.lang.Integer> AbstractMultiSeries<SERIES> pointColorInteger(final java.util.function.Function<java.lang.Comparable, COLOR> colors, final Object... multiSeriesKey) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointColorInteger for arguments [java.util.function.Function<java.lang.Comparable, COLOR>]. If you think this method should work, try placing your multiSeriesKey into an Object array", this);
    }



    @Override public <LABEL> AbstractMultiSeries<SERIES> pointLabel(final groovy.lang.Closure<LABEL> pointLabels, final Object... multiSeriesKey) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointLabel for arguments [groovy.lang.Closure<LABEL>]. If you think this method should work, try placing your multiSeriesKey into an Object array", this);
    }



    @Override public <LABEL> AbstractMultiSeries<SERIES> pointLabel(final java.util.function.Function<java.lang.Comparable, LABEL> pointLabels, final Object... multiSeriesKey) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointLabel for arguments [java.util.function.Function<java.lang.Comparable, LABEL>]. If you think this method should work, try placing your multiSeriesKey into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> pointShape(final groovy.lang.Closure<java.lang.String> pointShapes, final Object... multiSeriesKey) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointShape for arguments [groovy.lang.Closure<java.lang.String>]. If you think this method should work, try placing your multiSeriesKey into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> pointShape(final java.util.function.Function<java.lang.Comparable, java.lang.String> pointShapes, final Object... multiSeriesKey) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointShape for arguments [java.util.function.Function<java.lang.Comparable, java.lang.String>]. If you think this method should work, try placing your multiSeriesKey into an Object array", this);
    }



    @Override public <NUMBER extends java.lang.Number> AbstractMultiSeries<SERIES> pointSize(final groovy.lang.Closure<NUMBER> pointSizes, final Object... multiSeriesKey) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointSize for arguments [groovy.lang.Closure<NUMBER>]. If you think this method should work, try placing your multiSeriesKey into an Object array", this);
    }



    @Override public <NUMBER extends java.lang.Number> AbstractMultiSeries<SERIES> pointSize(final java.util.function.Function<java.lang.Comparable, NUMBER> pointSizes, final Object... multiSeriesKey) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointSize for arguments [java.util.function.Function<java.lang.Comparable, NUMBER>]. If you think this method should work, try placing your multiSeriesKey into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> errorBarColor(final java.lang.String errorBarColor, final Object... multiSeriesKey) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method errorBarColor for arguments [class java.lang.String]. If you think this method should work, try placing your multiSeriesKey into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> errorBarColor(final int errorBarColor, final Object... multiSeriesKey) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method errorBarColor for arguments [int]. If you think this method should work, try placing your multiSeriesKey into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> errorBarColor(final io.deephaven.gui.color.Paint errorBarColor, final Object... multiSeriesKey) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method errorBarColor for arguments [interface io.deephaven.gui.color.Paint]. If you think this method should work, try placing your multiSeriesKey into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> gradientVisible(final boolean gradientVisible, final Object... multiSeriesKey) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method gradientVisible for arguments [boolean]. If you think this method should work, try placing your multiSeriesKey into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> lineColor(final java.lang.String color, final Object... multiSeriesKey) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method lineColor for arguments [class java.lang.String]. If you think this method should work, try placing your multiSeriesKey into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> lineColor(final int color, final Object... multiSeriesKey) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method lineColor for arguments [int]. If you think this method should work, try placing your multiSeriesKey into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> lineColor(final io.deephaven.gui.color.Paint color, final Object... multiSeriesKey) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method lineColor for arguments [interface io.deephaven.gui.color.Paint]. If you think this method should work, try placing your multiSeriesKey into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> lineStyle(final io.deephaven.plot.LineStyle lineStyle, final Object... multiSeriesKey) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method lineStyle for arguments [class io.deephaven.plot.LineStyle]. If you think this method should work, try placing your multiSeriesKey into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> linesVisible(final java.lang.Boolean visible, final Object... multiSeriesKey) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method linesVisible for arguments [class java.lang.Boolean]. If you think this method should work, try placing your multiSeriesKey into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> pointColor(final java.lang.String pointColor, final Object... multiSeriesKey) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointColor for arguments [class java.lang.String]. If you think this method should work, try placing your multiSeriesKey into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> pointColor(final int pointColor, final Object... multiSeriesKey) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointColor for arguments [int]. If you think this method should work, try placing your multiSeriesKey into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> pointColor(final io.deephaven.gui.color.Paint pointColor, final Object... multiSeriesKey) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointColor for arguments [interface io.deephaven.gui.color.Paint]. If you think this method should work, try placing your multiSeriesKey into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> pointLabel(final java.lang.Object pointLabel, final Object... multiSeriesKey) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointLabel for arguments [class java.lang.Object]. If you think this method should work, try placing your multiSeriesKey into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> pointLabelFormat(final java.lang.String pointLabelFormat, final Object... multiSeriesKey) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointLabelFormat for arguments [class java.lang.String]. If you think this method should work, try placing your multiSeriesKey into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> pointShape(final java.lang.String pointShape, final Object... multiSeriesKey) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointShape for arguments [class java.lang.String]. If you think this method should work, try placing your multiSeriesKey into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> pointShape(final io.deephaven.gui.shape.Shape pointShape, final Object... multiSeriesKey) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointShape for arguments [interface io.deephaven.gui.shape.Shape]. If you think this method should work, try placing your multiSeriesKey into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> pointSize(final java.lang.Number pointSize, final Object... multiSeriesKey) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointSize for arguments [class java.lang.Number]. If you think this method should work, try placing your multiSeriesKey into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> pointsVisible(final java.lang.Boolean visible, final Object... multiSeriesKey) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointsVisible for arguments [class java.lang.Boolean]. If you think this method should work, try placing your multiSeriesKey into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> seriesColor(final java.lang.String color, final Object... multiSeriesKey) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method seriesColor for arguments [class java.lang.String]. If you think this method should work, try placing your multiSeriesKey into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> seriesColor(final int color, final Object... multiSeriesKey) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method seriesColor for arguments [int]. If you think this method should work, try placing your multiSeriesKey into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> seriesColor(final io.deephaven.gui.color.Paint color, final Object... multiSeriesKey) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method seriesColor for arguments [interface io.deephaven.gui.color.Paint]. If you think this method should work, try placing your multiSeriesKey into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> toolTipPattern(final java.lang.String toolTipPattern, final Object... multiSeriesKey) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method toolTipPattern for arguments [class java.lang.String]. If you think this method should work, try placing your multiSeriesKey into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> xToolTipPattern(final java.lang.String xToolTipPattern, final Object... multiSeriesKey) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method xToolTipPattern for arguments [class java.lang.String]. If you think this method should work, try placing your multiSeriesKey into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> yToolTipPattern(final java.lang.String yToolTipPattern, final Object... multiSeriesKey) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method yToolTipPattern for arguments [class java.lang.String]. If you think this method should work, try placing your multiSeriesKey into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> group(final int group, final Object... multiSeriesKey) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method group for arguments [int]. If you think this method should work, try placing your multiSeriesKey into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> piePercentLabelFormat(final java.lang.String pieLabelFormat, final Object... multiSeriesKey) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method piePercentLabelFormat for arguments [class java.lang.String]. If you think this method should work, try placing your multiSeriesKey into an Object array", this);
    }



    @Override public <CATEGORY extends java.lang.Comparable, COLOR extends io.deephaven.gui.color.Paint> AbstractMultiSeries<SERIES> pointColor(final java.util.Map<CATEGORY, COLOR> pointColor, final Object... multiSeriesKey) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointColor for arguments [java.util.Map<CATEGORY, COLOR>]. If you think this method should work, try placing your multiSeriesKey into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> pointColor(final java.lang.Comparable category, final java.lang.String pointColor, final Object... multiSeriesKey) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointColor for arguments [interface java.lang.Comparable, class java.lang.String]. If you think this method should work, try placing your multiSeriesKey into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> pointColor(final java.lang.Comparable category, final int pointColor, final Object... multiSeriesKey) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointColor for arguments [interface java.lang.Comparable, int]. If you think this method should work, try placing your multiSeriesKey into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> pointColor(final java.lang.Comparable category, final io.deephaven.gui.color.Paint pointColor, final Object... multiSeriesKey) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointColor for arguments [interface java.lang.Comparable, interface io.deephaven.gui.color.Paint]. If you think this method should work, try placing your multiSeriesKey into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> pointColor(final io.deephaven.engine.table.Table t, final java.lang.String category, final java.lang.String pointColor, final Object... multiSeriesKey) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointColor for arguments [interface io.deephaven.engine.table.Table, class java.lang.String, class java.lang.String]. If you think this method should work, try placing your multiSeriesKey into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> pointColor(final io.deephaven.plot.filters.SelectableDataSet sds, final java.lang.String category, final java.lang.String pointColor, final Object... multiSeriesKey) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointColor for arguments [interface io.deephaven.plot.filters.SelectableDataSet, class java.lang.String, class java.lang.String]. If you think this method should work, try placing your multiSeriesKey into an Object array", this);
    }



    @Override public <CATEGORY extends java.lang.Comparable, COLOR extends java.lang.Integer> AbstractMultiSeries<SERIES> pointColorInteger(final java.util.Map<CATEGORY, COLOR> colors, final Object... multiSeriesKey) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointColorInteger for arguments [java.util.Map<CATEGORY, COLOR>]. If you think this method should work, try placing your multiSeriesKey into an Object array", this);
    }



    @Override public <CATEGORY extends java.lang.Comparable, LABEL> AbstractMultiSeries<SERIES> pointLabel(final java.util.Map<CATEGORY, LABEL> pointLabels, final Object... multiSeriesKey) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointLabel for arguments [java.util.Map<CATEGORY, LABEL>]. If you think this method should work, try placing your multiSeriesKey into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> pointLabel(final java.lang.Comparable category, final java.lang.Object pointLabel, final Object... multiSeriesKey) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointLabel for arguments [interface java.lang.Comparable, class java.lang.Object]. If you think this method should work, try placing your multiSeriesKey into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> pointLabel(final io.deephaven.engine.table.Table t, final java.lang.String category, final java.lang.String pointLabel, final Object... multiSeriesKey) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointLabel for arguments [interface io.deephaven.engine.table.Table, class java.lang.String, class java.lang.String]. If you think this method should work, try placing your multiSeriesKey into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> pointLabel(final io.deephaven.plot.filters.SelectableDataSet sds, final java.lang.String category, final java.lang.String pointLabel, final Object... multiSeriesKey) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointLabel for arguments [interface io.deephaven.plot.filters.SelectableDataSet, class java.lang.String, class java.lang.String]. If you think this method should work, try placing your multiSeriesKey into an Object array", this);
    }



    @Override public <CATEGORY extends java.lang.Comparable> AbstractMultiSeries<SERIES> pointShape(final java.util.Map<CATEGORY, java.lang.String> pointShapes, final Object... multiSeriesKey) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointShape for arguments [java.util.Map<CATEGORY, java.lang.String>]. If you think this method should work, try placing your multiSeriesKey into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> pointShape(final java.lang.Comparable category, final java.lang.String pointShape, final Object... multiSeriesKey) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointShape for arguments [interface java.lang.Comparable, class java.lang.String]. If you think this method should work, try placing your multiSeriesKey into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> pointShape(final java.lang.Comparable category, final io.deephaven.gui.shape.Shape pointShape, final Object... multiSeriesKey) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointShape for arguments [interface java.lang.Comparable, interface io.deephaven.gui.shape.Shape]. If you think this method should work, try placing your multiSeriesKey into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> pointShape(final io.deephaven.engine.table.Table t, final java.lang.String category, final java.lang.String pointShape, final Object... multiSeriesKey) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointShape for arguments [interface io.deephaven.engine.table.Table, class java.lang.String, class java.lang.String]. If you think this method should work, try placing your multiSeriesKey into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> pointShape(final io.deephaven.plot.filters.SelectableDataSet sds, final java.lang.String category, final java.lang.String pointShape, final Object... multiSeriesKey) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointShape for arguments [interface io.deephaven.plot.filters.SelectableDataSet, class java.lang.String, class java.lang.String]. If you think this method should work, try placing your multiSeriesKey into an Object array", this);
    }



    @Override public <CATEGORY extends java.lang.Comparable, NUMBER extends java.lang.Number> AbstractMultiSeries<SERIES> pointSize(final java.util.Map<CATEGORY, NUMBER> pointSizes, final Object... multiSeriesKey) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointSize for arguments [java.util.Map<CATEGORY, NUMBER>]. If you think this method should work, try placing your multiSeriesKey into an Object array", this);
    }



    @Override public <CATEGORY extends java.lang.Comparable, NUMBER extends java.lang.Number> AbstractMultiSeries<SERIES> pointSize(final CATEGORY[] categories, final NUMBER[] pointSizes, final Object... multiSeriesKey) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointSize for arguments [CATEGORY[], NUMBER[]]. If you think this method should work, try placing your multiSeriesKey into an Object array", this);
    }



    @Override public <CATEGORY extends java.lang.Comparable> AbstractMultiSeries<SERIES> pointSize(final CATEGORY[] categories, final double[] pointSizes, final Object... multiSeriesKey) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointSize for arguments [CATEGORY[], class [D]. If you think this method should work, try placing your multiSeriesKey into an Object array", this);
    }



    @Override public <CATEGORY extends java.lang.Comparable> AbstractMultiSeries<SERIES> pointSize(final CATEGORY[] categories, final int[] pointSizes, final Object... multiSeriesKey) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointSize for arguments [CATEGORY[], class [I]. If you think this method should work, try placing your multiSeriesKey into an Object array", this);
    }



    @Override public <CATEGORY extends java.lang.Comparable> AbstractMultiSeries<SERIES> pointSize(final CATEGORY[] categories, final long[] pointSizes, final Object... multiSeriesKey) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointSize for arguments [CATEGORY[], class [J]. If you think this method should work, try placing your multiSeriesKey into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> pointSize(final java.lang.Comparable category, final java.lang.Number pointSize, final Object... multiSeriesKey) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointSize for arguments [interface java.lang.Comparable, class java.lang.Number]. If you think this method should work, try placing your multiSeriesKey into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> pointSize(final java.lang.Comparable category, final double pointSize, final Object... multiSeriesKey) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointSize for arguments [interface java.lang.Comparable, double]. If you think this method should work, try placing your multiSeriesKey into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> pointSize(final java.lang.Comparable category, final int pointSize, final Object... multiSeriesKey) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointSize for arguments [interface java.lang.Comparable, int]. If you think this method should work, try placing your multiSeriesKey into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> pointSize(final java.lang.Comparable category, final long pointSize, final Object... multiSeriesKey) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointSize for arguments [interface java.lang.Comparable, long]. If you think this method should work, try placing your multiSeriesKey into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> pointSize(final io.deephaven.engine.table.Table t, final java.lang.String category, final java.lang.String pointSize, final Object... multiSeriesKey) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointSize for arguments [interface io.deephaven.engine.table.Table, class java.lang.String, class java.lang.String]. If you think this method should work, try placing your multiSeriesKey into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> pointSize(final io.deephaven.plot.filters.SelectableDataSet sds, final java.lang.String category, final java.lang.String pointSize, final Object... multiSeriesKey) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointSize for arguments [interface io.deephaven.plot.filters.SelectableDataSet, class java.lang.String, class java.lang.String]. If you think this method should work, try placing your multiSeriesKey into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> pointColor(final int[] pointColors, final Object... multiSeriesKey) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointColor for arguments [class [I]. If you think this method should work, try placing your multiSeriesKey into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> pointColor(final io.deephaven.gui.color.Paint[] pointColor, final Object... multiSeriesKey) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointColor for arguments [class [Lio.deephaven.gui.color.Paint;]. If you think this method should work, try placing your multiSeriesKey into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> pointColor(final java.lang.Integer[] pointColors, final Object... multiSeriesKey) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointColor for arguments [class [Ljava.lang.Integer;]. If you think this method should work, try placing your multiSeriesKey into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> pointColor(final java.lang.String[] pointColors, final Object... multiSeriesKey) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointColor for arguments [class [Ljava.lang.String;]. If you think this method should work, try placing your multiSeriesKey into an Object array", this);
    }



    @Override public <T extends io.deephaven.gui.color.Paint> AbstractMultiSeries<SERIES> pointColor(final io.deephaven.plot.datasets.data.IndexableData<T> pointColor, final Object... multiSeriesKey) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointColor for arguments [io.deephaven.plot.datasets.data.IndexableData<T>]. If you think this method should work, try placing your multiSeriesKey into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> pointColor(final io.deephaven.engine.table.Table t, final java.lang.String pointColors, final Object... multiSeriesKey) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointColor for arguments [interface io.deephaven.engine.table.Table, class java.lang.String]. If you think this method should work, try placing your multiSeriesKey into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> pointColor(final io.deephaven.plot.filters.SelectableDataSet sds, final java.lang.String pointColors, final Object... multiSeriesKey) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointColor for arguments [interface io.deephaven.plot.filters.SelectableDataSet, class java.lang.String]. If you think this method should work, try placing your multiSeriesKey into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> pointColorInteger(final io.deephaven.plot.datasets.data.IndexableData<java.lang.Integer> colors, final Object... multiSeriesKey) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointColorInteger for arguments [io.deephaven.plot.datasets.data.IndexableData<java.lang.Integer>]. If you think this method should work, try placing your multiSeriesKey into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> pointLabel(final java.lang.Object[] pointLabels, final Object... multiSeriesKey) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointLabel for arguments [class [Ljava.lang.Object;]. If you think this method should work, try placing your multiSeriesKey into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> pointLabel(final io.deephaven.plot.datasets.data.IndexableData<?> pointLabels, final Object... multiSeriesKey) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointLabel for arguments [io.deephaven.plot.datasets.data.IndexableData<?>]. If you think this method should work, try placing your multiSeriesKey into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> pointLabel(final io.deephaven.engine.table.Table t, final java.lang.String pointLabel, final Object... multiSeriesKey) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointLabel for arguments [interface io.deephaven.engine.table.Table, class java.lang.String]. If you think this method should work, try placing your multiSeriesKey into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> pointLabel(final io.deephaven.plot.filters.SelectableDataSet sds, final java.lang.String pointLabel, final Object... multiSeriesKey) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointLabel for arguments [interface io.deephaven.plot.filters.SelectableDataSet, class java.lang.String]. If you think this method should work, try placing your multiSeriesKey into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> pointShape(final io.deephaven.gui.shape.Shape[] pointShapes, final Object... multiSeriesKey) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointShape for arguments [class [Lio.deephaven.gui.shape.Shape;]. If you think this method should work, try placing your multiSeriesKey into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> pointShape(final java.lang.String[] pointShapes, final Object... multiSeriesKey) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointShape for arguments [class [Ljava.lang.String;]. If you think this method should work, try placing your multiSeriesKey into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> pointShape(final io.deephaven.plot.datasets.data.IndexableData<java.lang.String> pointShapes, final Object... multiSeriesKey) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointShape for arguments [io.deephaven.plot.datasets.data.IndexableData<java.lang.String>]. If you think this method should work, try placing your multiSeriesKey into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> pointShape(final io.deephaven.engine.table.Table t, final java.lang.String pointShape, final Object... multiSeriesKey) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointShape for arguments [interface io.deephaven.engine.table.Table, class java.lang.String]. If you think this method should work, try placing your multiSeriesKey into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> pointShape(final io.deephaven.plot.filters.SelectableDataSet sds, final java.lang.String pointShape, final Object... multiSeriesKey) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointShape for arguments [interface io.deephaven.plot.filters.SelectableDataSet, class java.lang.String]. If you think this method should work, try placing your multiSeriesKey into an Object array", this);
    }



    @Override public <T extends java.lang.Number> AbstractMultiSeries<SERIES> pointSize(final T[] pointSizes, final Object... multiSeriesKey) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointSize for arguments [T[]]. If you think this method should work, try placing your multiSeriesKey into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> pointSize(final double[] pointSizes, final Object... multiSeriesKey) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointSize for arguments [class [D]. If you think this method should work, try placing your multiSeriesKey into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> pointSize(final int[] pointSizes, final Object... multiSeriesKey) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointSize for arguments [class [I]. If you think this method should work, try placing your multiSeriesKey into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> pointSize(final long[] pointSizes, final Object... multiSeriesKey) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointSize for arguments [class [J]. If you think this method should work, try placing your multiSeriesKey into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> pointSize(final io.deephaven.plot.datasets.data.IndexableData<java.lang.Double> pointSizes, final Object... multiSeriesKey) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointSize for arguments [io.deephaven.plot.datasets.data.IndexableData<java.lang.Double>]. If you think this method should work, try placing your multiSeriesKey into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> pointSize(final io.deephaven.engine.table.Table t, final java.lang.String pointSizes, final Object... multiSeriesKey) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointSize for arguments [interface io.deephaven.engine.table.Table, class java.lang.String]. If you think this method should work, try placing your multiSeriesKey into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> pointSize(final io.deephaven.plot.filters.SelectableDataSet sds, final java.lang.String pointSize, final Object... multiSeriesKey) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointSize for arguments [interface io.deephaven.plot.filters.SelectableDataSet, class java.lang.String]. If you think this method should work, try placing your multiSeriesKey into an Object array", this);
    }




}
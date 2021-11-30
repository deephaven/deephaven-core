/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.plot.datasets.multiseries;

import io.deephaven.base.verify.Assert;
import io.deephaven.datastructures.util.SmartKey;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableMap;
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
import io.deephaven.engine.table.lang.QueryLibrary;
import io.deephaven.engine.table.impl.*;
import groovy.lang.Closure;

import io.deephaven.internal.log.LoggerFactory;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

import static io.deephaven.engine.util.TableTools.emptyTable;

/**
 * Creates and holds a {@link DataSeriesInternal} for every key in a {@link TableMap}.
 */
@SuppressWarnings("SynchronizeOnNonFinalField")
public abstract class AbstractMultiSeries<SERIES extends DataSeriesInternal> extends AbstractSeriesInternal
        implements MultiSeriesInternal<SERIES>, TableSnapshotSeries {
    private static final long serialVersionUID = 3548896765688007362L;
    private static final Logger log = LoggerFactory.getLogger(AbstractMultiSeries.class);

    protected static final TableMap EMPTY_TABLE_MAP = emptyTable(0).partitionBy();
    protected final String[] byColumns;

    protected transient Object tableMapLock;
    protected transient TableMap tableMap;

    private transient Object seriesLock;
    private transient List<SERIES> series;
    private transient Set<Object> seriesKeys;
    private transient Map<String, Object> seriesNames;

    private final transient java.util.function.Function<Object, String> DEFAULT_NAMING_FUNCTION = key -> {
        final String keyString;
        if (key instanceof SmartKey) {
            keyString = Arrays.toString(((SmartKey) key).values_);
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
     * @param byColumns columns forming the keys of the table map
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
            final java.util.function.Function<Object, String> namingFunction) {
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
        seriesNameColumnName =
                seriesNameColumnName == null ? ColumnNameConstants.SERIES_NAME + this.hashCode() : seriesNameColumnName;
        applyFunction(namingFunction, seriesNameColumnName, String.class);
    }

    /**
     * Applies the {@code function} to the given input of the underlying table to create a new column
     * {@code columnName}.
     */
    protected void applyFunction(final java.util.function.Function function, final String columnName,
            final Class resultClass) {
        final String functionInput;
        if (byColumns.length > 1) {
            QueryLibrary.importClass(SmartKey.class);
            functionInput = "new SmartKey(" + String.join(",", byColumns) + ")";
        } else {
            functionInput = byColumns[0];
        }

        applyFunction(function, columnName, functionInput, resultClass);
    }

    /**
     * Applies the {@code function} to the byColumns of the underlying table to create a new column {@code columnName}.
     */
    protected void applyFunction(final java.util.function.Function function, final String columnName,
            final String functionInput, final Class resultClass) {
        ArgumentValidations.assertNotNull(function, "function", getPlotInfo());
        final String queryFunction = columnName + "Function";
        final Map<String, Object> params = new HashMap<>();
        params.put(queryFunction, function);

        final String update = columnName + " = (" + resultClass.getSimpleName() + ") " + queryFunction + ".apply("
                + functionInput + ")";

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
        Assert.holdsLock(seriesLock, "seriesLock");

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
        this.tableMapLock = new Object();
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

    @Override public <T extends io.deephaven.gui.color.Paint> AbstractMultiSeries<SERIES> pointColorByY(final groovy.lang.Closure<T> colors, final Object... keys) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointColorByY for arguments [groovy.lang.Closure<T>]. If you think this method should work, try placing your keys into an Object array", this);
    }



    @Override public <T extends io.deephaven.gui.color.Paint> AbstractMultiSeries<SERIES> pointColorByY(final java.util.function.Function<java.lang.Double, T> colors, final Object... keys) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointColorByY for arguments [java.util.function.Function<java.lang.Double, T>]. If you think this method should work, try placing your keys into an Object array", this);
    }



    @Override public <COLOR extends io.deephaven.gui.color.Paint> AbstractMultiSeries<SERIES> pointColor(final groovy.lang.Closure<COLOR> colors, final Object... keys) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointColor for arguments [groovy.lang.Closure<COLOR>]. If you think this method should work, try placing your keys into an Object array", this);
    }



    @Override public <COLOR extends io.deephaven.gui.color.Paint> AbstractMultiSeries<SERIES> pointColor(final java.util.function.Function<java.lang.Comparable, COLOR> colors, final Object... keys) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointColor for arguments [java.util.function.Function<java.lang.Comparable, COLOR>]. If you think this method should work, try placing your keys into an Object array", this);
    }



    @Override public <COLOR extends java.lang.Integer> AbstractMultiSeries<SERIES> pointColorInteger(final groovy.lang.Closure<COLOR> colors, final Object... keys) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointColorInteger for arguments [groovy.lang.Closure<COLOR>]. If you think this method should work, try placing your keys into an Object array", this);
    }



    @Override public <COLOR extends java.lang.Integer> AbstractMultiSeries<SERIES> pointColorInteger(final java.util.function.Function<java.lang.Comparable, COLOR> colors, final Object... keys) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointColorInteger for arguments [java.util.function.Function<java.lang.Comparable, COLOR>]. If you think this method should work, try placing your keys into an Object array", this);
    }



    @Override public <LABEL> AbstractMultiSeries<SERIES> pointLabel(final groovy.lang.Closure<LABEL> labels, final Object... keys) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointLabel for arguments [groovy.lang.Closure<LABEL>]. If you think this method should work, try placing your keys into an Object array", this);
    }



    @Override public <LABEL> AbstractMultiSeries<SERIES> pointLabel(final java.util.function.Function<java.lang.Comparable, LABEL> labels, final Object... keys) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointLabel for arguments [java.util.function.Function<java.lang.Comparable, LABEL>]. If you think this method should work, try placing your keys into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> pointShape(final groovy.lang.Closure<java.lang.String> shapes, final Object... keys) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointShape for arguments [groovy.lang.Closure<java.lang.String>]. If you think this method should work, try placing your keys into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> pointShape(final java.util.function.Function<java.lang.Comparable, java.lang.String> shapes, final Object... keys) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointShape for arguments [java.util.function.Function<java.lang.Comparable, java.lang.String>]. If you think this method should work, try placing your keys into an Object array", this);
    }



    @Override public <NUMBER extends java.lang.Number> AbstractMultiSeries<SERIES> pointSize(final groovy.lang.Closure<NUMBER> factors, final Object... keys) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointSize for arguments [groovy.lang.Closure<NUMBER>]. If you think this method should work, try placing your keys into an Object array", this);
    }



    @Override public <NUMBER extends java.lang.Number> AbstractMultiSeries<SERIES> pointSize(final java.util.function.Function<java.lang.Comparable, NUMBER> factors, final Object... keys) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointSize for arguments [java.util.function.Function<java.lang.Comparable, NUMBER>]. If you think this method should work, try placing your keys into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> errorBarColor(final java.lang.String color, final Object... keys) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method errorBarColor for arguments [class java.lang.String]. If you think this method should work, try placing your keys into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> errorBarColor(final int color, final Object... keys) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method errorBarColor for arguments [int]. If you think this method should work, try placing your keys into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> errorBarColor(final io.deephaven.gui.color.Paint color, final Object... keys) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method errorBarColor for arguments [interface io.deephaven.gui.color.Paint]. If you think this method should work, try placing your keys into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> gradientVisible(final boolean visible, final Object... keys) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method gradientVisible for arguments [boolean]. If you think this method should work, try placing your keys into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> lineColor(final java.lang.String color, final Object... keys) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method lineColor for arguments [class java.lang.String]. If you think this method should work, try placing your keys into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> lineColor(final int color, final Object... keys) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method lineColor for arguments [int]. If you think this method should work, try placing your keys into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> lineColor(final io.deephaven.gui.color.Paint color, final Object... keys) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method lineColor for arguments [interface io.deephaven.gui.color.Paint]. If you think this method should work, try placing your keys into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> lineStyle(final io.deephaven.plot.LineStyle style, final Object... keys) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method lineStyle for arguments [class io.deephaven.plot.LineStyle]. If you think this method should work, try placing your keys into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> linesVisible(final java.lang.Boolean visible, final Object... keys) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method linesVisible for arguments [class java.lang.Boolean]. If you think this method should work, try placing your keys into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> pointColor(final java.lang.String color, final Object... keys) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointColor for arguments [class java.lang.String]. If you think this method should work, try placing your keys into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> pointColor(final int color, final Object... keys) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointColor for arguments [int]. If you think this method should work, try placing your keys into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> pointColor(final io.deephaven.gui.color.Paint color, final Object... keys) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointColor for arguments [interface io.deephaven.gui.color.Paint]. If you think this method should work, try placing your keys into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> pointLabel(final java.lang.Object label, final Object... keys) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointLabel for arguments [class java.lang.Object]. If you think this method should work, try placing your keys into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> pointLabelFormat(final java.lang.String format, final Object... keys) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointLabelFormat for arguments [class java.lang.String]. If you think this method should work, try placing your keys into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> pointShape(final java.lang.String shape, final Object... keys) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointShape for arguments [class java.lang.String]. If you think this method should work, try placing your keys into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> pointShape(final io.deephaven.gui.shape.Shape shape, final Object... keys) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointShape for arguments [interface io.deephaven.gui.shape.Shape]. If you think this method should work, try placing your keys into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> pointSize(final java.lang.Number factor, final Object... keys) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointSize for arguments [class java.lang.Number]. If you think this method should work, try placing your keys into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> pointsVisible(final java.lang.Boolean visible, final Object... keys) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointsVisible for arguments [class java.lang.Boolean]. If you think this method should work, try placing your keys into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> seriesColor(final java.lang.String color, final Object... keys) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method seriesColor for arguments [class java.lang.String]. If you think this method should work, try placing your keys into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> seriesColor(final int color, final Object... keys) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method seriesColor for arguments [int]. If you think this method should work, try placing your keys into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> seriesColor(final io.deephaven.gui.color.Paint color, final Object... keys) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method seriesColor for arguments [interface io.deephaven.gui.color.Paint]. If you think this method should work, try placing your keys into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> toolTipPattern(final java.lang.String format, final Object... keys) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method toolTipPattern for arguments [class java.lang.String]. If you think this method should work, try placing your keys into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> xToolTipPattern(final java.lang.String format, final Object... keys) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method xToolTipPattern for arguments [class java.lang.String]. If you think this method should work, try placing your keys into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> yToolTipPattern(final java.lang.String format, final Object... keys) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method yToolTipPattern for arguments [class java.lang.String]. If you think this method should work, try placing your keys into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> group(final int group, final Object... keys) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method group for arguments [int]. If you think this method should work, try placing your keys into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> piePercentLabelFormat(final java.lang.String format, final Object... keys) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method piePercentLabelFormat for arguments [class java.lang.String]. If you think this method should work, try placing your keys into an Object array", this);
    }



    @Override public <CATEGORY extends java.lang.Comparable, COLOR extends io.deephaven.gui.color.Paint> AbstractMultiSeries<SERIES> pointColor(final java.util.Map<CATEGORY, COLOR> colors, final Object... keys) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointColor for arguments [java.util.Map<CATEGORY, COLOR>]. If you think this method should work, try placing your keys into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> pointColor(final java.lang.Comparable category, final java.lang.String color, final Object... keys) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointColor for arguments [interface java.lang.Comparable, class java.lang.String]. If you think this method should work, try placing your keys into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> pointColor(final java.lang.Comparable category, final int color, final Object... keys) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointColor for arguments [interface java.lang.Comparable, int]. If you think this method should work, try placing your keys into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> pointColor(final java.lang.Comparable category, final io.deephaven.gui.color.Paint color, final Object... keys) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointColor for arguments [interface java.lang.Comparable, interface io.deephaven.gui.color.Paint]. If you think this method should work, try placing your keys into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> pointColor(final io.deephaven.engine.table.Table t, final java.lang.String keyColumn, final java.lang.String valueColumn, final Object... keys) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointColor for arguments [interface io.deephaven.engine.table.Table, class java.lang.String, class java.lang.String]. If you think this method should work, try placing your keys into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> pointColor(final io.deephaven.plot.filters.SelectableDataSet sds, final java.lang.String keyColumn, final java.lang.String valueColumn, final Object... keys) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointColor for arguments [interface io.deephaven.plot.filters.SelectableDataSet, class java.lang.String, class java.lang.String]. If you think this method should work, try placing your keys into an Object array", this);
    }



    @Override public <T extends io.deephaven.gui.color.Paint> AbstractMultiSeries<SERIES> pointColorByY(final java.util.Map<java.lang.Double, T> colors, final Object... keys) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointColorByY for arguments [java.util.Map<java.lang.Double, T>]. If you think this method should work, try placing your keys into an Object array", this);
    }



    @Override public <CATEGORY extends java.lang.Comparable, COLOR extends java.lang.Integer> AbstractMultiSeries<SERIES> pointColorInteger(final java.util.Map<CATEGORY, COLOR> colors, final Object... keys) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointColorInteger for arguments [java.util.Map<CATEGORY, COLOR>]. If you think this method should work, try placing your keys into an Object array", this);
    }



    @Override public <CATEGORY extends java.lang.Comparable, LABEL> AbstractMultiSeries<SERIES> pointLabel(final java.util.Map<CATEGORY, LABEL> labels, final Object... keys) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointLabel for arguments [java.util.Map<CATEGORY, LABEL>]. If you think this method should work, try placing your keys into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> pointLabel(final java.lang.Comparable category, final java.lang.Object label, final Object... keys) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointLabel for arguments [interface java.lang.Comparable, class java.lang.Object]. If you think this method should work, try placing your keys into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> pointLabel(final io.deephaven.engine.table.Table t, final java.lang.String keyColumn, final java.lang.String valueColumn, final Object... keys) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointLabel for arguments [interface io.deephaven.engine.table.Table, class java.lang.String, class java.lang.String]. If you think this method should work, try placing your keys into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> pointLabel(final io.deephaven.plot.filters.SelectableDataSet sds, final java.lang.String keyColumn, final java.lang.String valueColumn, final Object... keys) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointLabel for arguments [interface io.deephaven.plot.filters.SelectableDataSet, class java.lang.String, class java.lang.String]. If you think this method should work, try placing your keys into an Object array", this);
    }



    @Override public <CATEGORY extends java.lang.Comparable> AbstractMultiSeries<SERIES> pointShape(final java.util.Map<CATEGORY, java.lang.String> shapes, final Object... keys) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointShape for arguments [java.util.Map<CATEGORY, java.lang.String>]. If you think this method should work, try placing your keys into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> pointShape(final java.lang.Comparable category, final java.lang.String shape, final Object... keys) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointShape for arguments [interface java.lang.Comparable, class java.lang.String]. If you think this method should work, try placing your keys into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> pointShape(final java.lang.Comparable category, final io.deephaven.gui.shape.Shape shape, final Object... keys) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointShape for arguments [interface java.lang.Comparable, interface io.deephaven.gui.shape.Shape]. If you think this method should work, try placing your keys into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> pointShape(final io.deephaven.engine.table.Table t, final java.lang.String keyColumn, final java.lang.String valueColumn, final Object... keys) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointShape for arguments [interface io.deephaven.engine.table.Table, class java.lang.String, class java.lang.String]. If you think this method should work, try placing your keys into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> pointShape(final io.deephaven.plot.filters.SelectableDataSet sds, final java.lang.String keyColumn, final java.lang.String valueColumn, final Object... keys) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointShape for arguments [interface io.deephaven.plot.filters.SelectableDataSet, class java.lang.String, class java.lang.String]. If you think this method should work, try placing your keys into an Object array", this);
    }



    @Override public <CATEGORY extends java.lang.Comparable, NUMBER extends java.lang.Number> AbstractMultiSeries<SERIES> pointSize(final java.util.Map<CATEGORY, NUMBER> factors, final Object... keys) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointSize for arguments [java.util.Map<CATEGORY, NUMBER>]. If you think this method should work, try placing your keys into an Object array", this);
    }



    @Override public <CATEGORY extends java.lang.Comparable, NUMBER extends java.lang.Number> AbstractMultiSeries<SERIES> pointSize(final CATEGORY[] categories, final NUMBER[] factors, final Object... keys) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointSize for arguments [CATEGORY[], NUMBER[]]. If you think this method should work, try placing your keys into an Object array", this);
    }



    @Override public <CATEGORY extends java.lang.Comparable> AbstractMultiSeries<SERIES> pointSize(final CATEGORY[] categories, final double[] factors, final Object... keys) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointSize for arguments [CATEGORY[], class [D]. If you think this method should work, try placing your keys into an Object array", this);
    }



    @Override public <CATEGORY extends java.lang.Comparable> AbstractMultiSeries<SERIES> pointSize(final CATEGORY[] categories, final int[] factors, final Object... keys) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointSize for arguments [CATEGORY[], class [I]. If you think this method should work, try placing your keys into an Object array", this);
    }



    @Override public <CATEGORY extends java.lang.Comparable> AbstractMultiSeries<SERIES> pointSize(final CATEGORY[] categories, final long[] factors, final Object... keys) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointSize for arguments [CATEGORY[], class [J]. If you think this method should work, try placing your keys into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> pointSize(final java.lang.Comparable category, final java.lang.Number factor, final Object... keys) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointSize for arguments [interface java.lang.Comparable, class java.lang.Number]. If you think this method should work, try placing your keys into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> pointSize(final java.lang.Comparable category, final double factor, final Object... keys) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointSize for arguments [interface java.lang.Comparable, double]. If you think this method should work, try placing your keys into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> pointSize(final java.lang.Comparable category, final int factor, final Object... keys) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointSize for arguments [interface java.lang.Comparable, int]. If you think this method should work, try placing your keys into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> pointSize(final java.lang.Comparable category, final long factor, final Object... keys) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointSize for arguments [interface java.lang.Comparable, long]. If you think this method should work, try placing your keys into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> pointSize(final io.deephaven.engine.table.Table t, final java.lang.String keyColumn, final java.lang.String valueColumn, final Object... keys) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointSize for arguments [interface io.deephaven.engine.table.Table, class java.lang.String, class java.lang.String]. If you think this method should work, try placing your keys into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> pointSize(final io.deephaven.plot.filters.SelectableDataSet sds, final java.lang.String keyColumn, final java.lang.String valueColumn, final Object... keys) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointSize for arguments [interface io.deephaven.plot.filters.SelectableDataSet, class java.lang.String, class java.lang.String]. If you think this method should work, try placing your keys into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> pointColor(final int[] colors, final Object... keys) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointColor for arguments [class [I]. If you think this method should work, try placing your keys into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> pointColor(final io.deephaven.gui.color.Paint[] colors, final Object... keys) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointColor for arguments [class [Lio.deephaven.gui.color.Paint;]. If you think this method should work, try placing your keys into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> pointColor(final java.lang.Integer[] colors, final Object... keys) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointColor for arguments [class [Ljava.lang.Integer;]. If you think this method should work, try placing your keys into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> pointColor(final java.lang.String[] colors, final Object... keys) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointColor for arguments [class [Ljava.lang.String;]. If you think this method should work, try placing your keys into an Object array", this);
    }



    @Override public <T extends io.deephaven.gui.color.Paint> AbstractMultiSeries<SERIES> pointColor(final io.deephaven.plot.datasets.data.IndexableData<T> colors, final Object... keys) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointColor for arguments [io.deephaven.plot.datasets.data.IndexableData<T>]. If you think this method should work, try placing your keys into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> pointColor(final io.deephaven.engine.table.Table t, final java.lang.String columnName, final Object... keys) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointColor for arguments [interface io.deephaven.engine.table.Table, class java.lang.String]. If you think this method should work, try placing your keys into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> pointColor(final io.deephaven.plot.filters.SelectableDataSet sds, final java.lang.String columnName, final Object... keys) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointColor for arguments [interface io.deephaven.plot.filters.SelectableDataSet, class java.lang.String]. If you think this method should work, try placing your keys into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> pointColorInteger(final io.deephaven.plot.datasets.data.IndexableData<java.lang.Integer> colors, final Object... keys) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointColorInteger for arguments [io.deephaven.plot.datasets.data.IndexableData<java.lang.Integer>]. If you think this method should work, try placing your keys into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> pointLabel(final java.lang.Object[] labels, final Object... keys) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointLabel for arguments [class [Ljava.lang.Object;]. If you think this method should work, try placing your keys into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> pointLabel(final io.deephaven.plot.datasets.data.IndexableData<?> labels, final Object... keys) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointLabel for arguments [io.deephaven.plot.datasets.data.IndexableData<?>]. If you think this method should work, try placing your keys into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> pointLabel(final io.deephaven.engine.table.Table t, final java.lang.String columnName, final Object... keys) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointLabel for arguments [interface io.deephaven.engine.table.Table, class java.lang.String]. If you think this method should work, try placing your keys into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> pointLabel(final io.deephaven.plot.filters.SelectableDataSet sds, final java.lang.String columnName, final Object... keys) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointLabel for arguments [interface io.deephaven.plot.filters.SelectableDataSet, class java.lang.String]. If you think this method should work, try placing your keys into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> pointShape(final io.deephaven.gui.shape.Shape[] shapes, final Object... keys) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointShape for arguments [class [Lio.deephaven.gui.shape.Shape;]. If you think this method should work, try placing your keys into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> pointShape(final java.lang.String[] shapes, final Object... keys) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointShape for arguments [class [Ljava.lang.String;]. If you think this method should work, try placing your keys into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> pointShape(final io.deephaven.plot.datasets.data.IndexableData<java.lang.String> shapes, final Object... keys) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointShape for arguments [io.deephaven.plot.datasets.data.IndexableData<java.lang.String>]. If you think this method should work, try placing your keys into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> pointShape(final io.deephaven.engine.table.Table t, final java.lang.String columnName, final Object... keys) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointShape for arguments [interface io.deephaven.engine.table.Table, class java.lang.String]. If you think this method should work, try placing your keys into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> pointShape(final io.deephaven.plot.filters.SelectableDataSet sds, final java.lang.String columnName, final Object... keys) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointShape for arguments [interface io.deephaven.plot.filters.SelectableDataSet, class java.lang.String]. If you think this method should work, try placing your keys into an Object array", this);
    }



    @Override public <T extends java.lang.Number> AbstractMultiSeries<SERIES> pointSize(final T[] factors, final Object... keys) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointSize for arguments [T[]]. If you think this method should work, try placing your keys into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> pointSize(final double[] factors, final Object... keys) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointSize for arguments [class [D]. If you think this method should work, try placing your keys into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> pointSize(final int[] factors, final Object... keys) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointSize for arguments [class [I]. If you think this method should work, try placing your keys into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> pointSize(final long[] factors, final Object... keys) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointSize for arguments [class [J]. If you think this method should work, try placing your keys into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> pointSize(final io.deephaven.plot.datasets.data.IndexableData<java.lang.Double> factors, final Object... keys) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointSize for arguments [io.deephaven.plot.datasets.data.IndexableData<java.lang.Double>]. If you think this method should work, try placing your keys into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> pointSize(final io.deephaven.engine.table.Table t, final java.lang.String columnName, final Object... keys) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointSize for arguments [interface io.deephaven.engine.table.Table, class java.lang.String]. If you think this method should work, try placing your keys into an Object array", this);
    }



    @Override public AbstractMultiSeries<SERIES> pointSize(final io.deephaven.plot.filters.SelectableDataSet sds, final java.lang.String columnName, final Object... keys) {
        throw new PlotUnsupportedOperationException("DataSeries " + this.getClass() + " does not support method pointSize for arguments [interface io.deephaven.plot.filters.SelectableDataSet, class java.lang.String]. If you think this method should work, try placing your keys into an Object array", this);
    }




}
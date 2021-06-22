package io.deephaven.web.client.api.widget.plot;

import elemental2.core.JsArray;
import elemental2.core.JsObject;
import elemental2.dom.CustomEventInit;
import elemental2.promise.IThenable;
import elemental2.promise.Promise;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.FetchFigureResponse;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.FigureDescriptor;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.figuredescriptor.AxisDescriptor;
import io.deephaven.web.client.api.Callback;
import io.deephaven.web.client.api.Callbacks;
import io.deephaven.web.client.api.HasEventHandling;
import io.deephaven.web.client.api.JsTable;
import io.deephaven.web.client.api.TableMap;
import io.deephaven.web.client.api.WorkerConnection;
import io.deephaven.web.client.fu.JsLog;
import io.deephaven.web.client.fu.JsPromise;
import io.deephaven.web.client.fu.LazyPromise;
import io.deephaven.web.client.state.ClientTableState;
import io.deephaven.web.shared.data.InitialTableDefinition;
import io.deephaven.web.shared.data.TableHandle;
import io.deephaven.web.shared.fu.JsBiConsumer;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsOptional;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@JsType(name = "Figure", namespace = "dh.plot")
public class JsFigure extends HasEventHandling {
    private static native Throwable ofObject(Object obj) /*-{
      return @java.lang.Throwable::of(*)(obj);
    }-*/;

    @JsProperty(namespace = "dh.plot.Figure")
    public static final String EVENT_UPDATED = "updated",
            EVENT_SERIES_ADDED = "seriesadded",
            EVENT_DISCONNECT = JsTable.EVENT_DISCONNECT,
            EVENT_RECONNECT = JsTable.EVENT_RECONNECT,
            EVENT_RECONNECTFAILED = JsTable.EVENT_RECONNECTFAILED,
            EVENT_DOWNSAMPLESTARTED = "downsamplestarted",
            EVENT_DOWNSAMPLEFINISHED = "downsamplefinished",
            EVENT_DOWNSAMPLEFAILED = "downsamplefailed",
            EVENT_DOWNSAMPLENEEDED = "downsampleneeded";

    public interface FigureFetch {
        void fetch(JsBiConsumer<Object, FetchFigureResponse> callback);
    }

    public interface FigureTableFetch {
        Promise<FigureTableFetchData> fetch(JsFigure figure, FigureDescriptor descriptor);
    }

    public interface FigureClose {
        void close(JsFigure figure);
    }

    public class FigureSourceException extends RuntimeException {
        @JsProperty
        transient JsTable table;

        @JsProperty
        transient SeriesDataSource source;

        FigureSourceException(JsTable table, SeriesDataSource source, String message) {
            super(message);

            this.table = table;
            this.source = source;
        }
    }

    public class FigureFetchError {
        @JsProperty
        Object error;

        @JsProperty
        JsArray<? extends Object> errors;

        FigureFetchError(Object error, JsArray<? extends Object> errors) {
            this.error = error;
            this.errors = errors;
        }

        public String toString() {
            return error.toString();
        }
    }

    private final FigureFetch fetch;
    private final FigureTableFetch tableFetch;
    private FigureClose onClose;

    private FigureDescriptor descriptor;

    private JsChart[] charts;

    private JsTable[] tables;
    private Map<Integer, JsTable> plotHandlesToTables;

    private TableMap[] tableMaps;
    private Map<Integer, TableMap> plotHandlesToTableMaps;

    private final Map<AxisDescriptor, DownsampledAxisDetails> downsampled = new HashMap<>();

    private final Map<FigureSubscription, FigureSubscription> activeFigureSubscriptions = new HashMap<>();

    private boolean subCheckEnqueued = false;

    @JsIgnore
    public JsFigure(WorkerConnection connection, FigureFetch fetch) {
        this(fetch, new DefaultFigureTableFetch(connection));
    }

    @JsIgnore
    public JsFigure(FigureFetch fetch, FigureTableFetch tableFetch) {
        this.fetch = fetch;
        this.tableFetch = tableFetch;
    }

    @JsIgnore
    public Promise<JsFigure> refetch() {
        plotHandlesToTables = new HashMap<>();

        return Callbacks.grpcUnaryPromise(fetch::fetch).then(response -> {
            this.descriptor = response.getFigureDescriptor();

            charts = descriptor.getChartsList().asList().stream().map(chartDescriptor -> new JsChart(chartDescriptor, this)).toArray(JsChart[]::new);
            JsObject.freeze(charts);

            return this.tableFetch.fetch(this, descriptor);
        }).then(tableFetchData -> {
            // all tables are wired up, need to map them to the series instances
            tables = tableFetchData.tables;
            tableMaps = tableFetchData.tableMaps;
            plotHandlesToTableMaps = tableFetchData.plotHandlesToTableMaps;
            onClose = tableFetchData.onClose;

            for (int i = 0; i < descriptor.getTablesList().length; i++) {
                JsTable table = tables[i];
                registerTableWithId(table, Js.cast(JsArray.of((double)i)));
            }
            Arrays.stream(charts)
                    .flatMap(c -> Arrays.stream(c.getSeries()))
                    .forEach(s -> s.initSources(plotHandlesToTables, plotHandlesToTableMaps));
            Arrays.stream(charts)
                    .flatMap(c -> Arrays.stream(c.getMultiSeries()))
                    .forEach(s -> s.initSources(plotHandlesToTableMaps));

            return null;
        }).then(ignore -> {
            unsuppressEvents();
            fireEvent(EVENT_RECONNECT);
            return Promise.resolve(this);
        }, err -> {
            final FigureFetchError fetchError = new FigureFetchError(ofObject(err), this.descriptor != null ? this.descriptor.getErrorsList() : new JsArray<>());
            final CustomEventInit init = CustomEventInit.create();
            init.setDetail(fetchError);
            unsuppressEvents();
            fireEvent(EVENT_RECONNECTFAILED, init);
            suppressEvents();

            //noinspection unchecked,rawtypes
            return (Promise<JsFigure>) (Promise) Promise.reject(fetchError);
        });
    }

    @JsProperty
    public String getTitle() {
        if (descriptor.hasTitle()) {
            return descriptor.getTitle();
        }
        return null;
    }

    @JsProperty
    public String getTitleFont() {
        return descriptor.getTitleFont();
    }

    @JsProperty
    public String getTitleColor() {
        return descriptor.getTitleColor();
    }

    @JsProperty
    public double getUpdateInterval() {
        return descriptor.getUpdateInterval();
    }

    @JsProperty
    public int getCols() {
        return descriptor.getCols();
    }

    @JsProperty
    public int getRows() {
        return descriptor.getRows();
    }

    @JsProperty
    public JsChart[] getCharts() {
        return charts;
    }

    public String[] getErrors() {
        return Js.uncheckedCast(descriptor.getErrorsList().slice());
    }

    @JsIgnore
    public void subscribe() {
        subscribe(null);
    }

    public void subscribe(@JsOptional DownsampleOptions forceDisableDownsample) {
        //iterate all series, mark all as subscribed, will enqueue a check automatically
        Arrays.stream(charts).flatMap(c -> Arrays.stream(c.getSeries()))
                .forEach(s -> s.subscribe(forceDisableDownsample));
    }

    public void unsubscribe() {
        //iterate all series, mark all as unsubscribed
        Arrays.stream(charts).flatMap(c -> Arrays.stream(c.getSeries()))
                .forEach(JsSeries::markUnsubscribed);

        // clear all subscriptions, no need to do a real check
        activeFigureSubscriptions.keySet().forEach(FigureSubscription::unsubscribe);
        activeFigureSubscriptions.clear();
    }

    @JsIgnore
    public void downsampleNeeded(String message, Set<JsSeries> series, double tableSize) {
        CustomEventInit failInit = CustomEventInit.create();
        failInit.setDetail(JsPropertyMap.of("series", series, "message", message, "size", tableSize));
        fireEvent(EVENT_DOWNSAMPLENEEDED, failInit);
    }

    @JsIgnore
    public void downsampleFailed(String message, Set<JsSeries> series, double tableSize) {
        CustomEventInit failInit = CustomEventInit.create();
        failInit.setDetail(JsPropertyMap.of("series", series, "message", message, "size", tableSize));
        fireEvent(EVENT_DOWNSAMPLEFAILED, failInit);
    }

    private void updateSubscriptions() {
        // mark that we're performing the subscription check, any future changes will need to re-enqueue this step
        subCheckEnqueued = false;

        // Collect the subscriptions that we will need for the current series and their configurations
        final Map<JsTable, Map<AxisRange, DownsampleParams>> downsampleMappings = Arrays.stream(charts)
                .flatMap(c -> Arrays.stream(c.getSeries()))
                .filter(JsSeries::isSubscribed)
                .filter(series -> series.getOneClick() == null || (series.getOneClick().allRequiredValuesSet() && series.getOneClick().getTable() != null))
                .collect(
                        Collectors.groupingBy(
                                this::tableForSeries,
                                Collectors.groupingBy(
                                        this::groupByAxisRange,
                                        Collectors.reducing(DownsampleParams.EMPTY, this::makeParamsForSeries, DownsampleParams::merge)
                                )
                        )
                );

        final Set<FigureSubscription> newSubscriptions = downsampleMappings.entrySet().stream().flatMap(outerEntry -> {
            JsTable table = outerEntry.getKey();
            Map<AxisRange, DownsampleParams> mapping = outerEntry.getValue();
            return mapping.entrySet().stream().map(innerEntry -> {
                AxisRange range = innerEntry.getKey();
                DownsampleParams params = innerEntry.getValue();
                return new FigureSubscription(this, table, range, range == null ? null : params, new HashSet<>(Arrays.asList(params.series)));
            });
        }).collect(Collectors.toSet());

        // Given those subscriptions, check our existing subscriptions to determine which new subscriptions
        // need to be created, and which existing ones are no longer needed.
        // Note that when we compare these, we only check the original table and the mutations applied to that table
        // (filters, downsample), we don't include the series instances themselves, as there is no need to re-subscribe
        // just because a series is now being drawn which shares the same data as other visible series.

        // Both unsubscribing and creating a subscription will delegate to the FigureSubscription class to let it
        // get things started.
        final Set<FigureSubscription> unseen = new HashSet<>(activeFigureSubscriptions.values());
        for (final FigureSubscription newSubscription : newSubscriptions) {
            if (activeFigureSubscriptions.containsKey(newSubscription)) {
                // already present, update series (if needed), and let it fire events
                activeFigureSubscriptions.get(newSubscription).replaceSeries(newSubscription.getSeries());
                JsLog.info("Saw same subscription again", activeFigureSubscriptions.get(newSubscription));

                // mark as seen
                unseen.remove(newSubscription);
            } else {
                // new subscription, not present yet
                activeFigureSubscriptions.put(newSubscription, newSubscription);
                JsLog.info("Adding new subscription", newSubscription);
                newSubscription.subscribe();
            }
        }
        // remove all now-unused subscriptions
        for (final FigureSubscription unseenSub : unseen) {
            JsLog.info("Removing unused subscription", unseenSub);
            unseenSub.unsubscribe();
            activeFigureSubscriptions.remove(unseenSub);
        }
    }

    private JsTable tableForSeries(JsSeries s) {
        // if we have a oneclick, then grab the loaded table
        if (s.getOneClick() != null) {
            return s.getOneClick().getTable();
        }

        // otherwise grab the first table we can find
        //TODO loop, assert all match
        return plotHandlesToTables.get(s.getDescriptor().getDataSourcesList().getAt(0).getTableId());
    }

    // First, break down the ranges so we can tell when they are entirely incompatible. They
    // won't forever be incompatible by max/min, but today they are.
    public static class AxisRange {
        final String xCol;
        final Long min;
        final Long max;

        AxisRange(final String xCol, final Long min, final Long max) {
            this.xCol = xCol;
            this.min = min;
            this.max = max;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            final AxisRange axisRange = (AxisRange) o;

            if (!xCol.equals(axisRange.xCol)) return false;
            if (min != null ? !min.equals(axisRange.min) : axisRange.min != null) return false;
            return max != null ? max.equals(axisRange.max) : axisRange.max == null;
        }

        @Override
        public int hashCode() {
            int result = xCol.hashCode();
            result = 31 * result + (min != null ? min.hashCode() : 0);
            result = 31 * result + (max != null ? max.hashCode() : 0);
            return result;
        }

        public String getxCol() {
            return xCol;
        }

        public Long getMin() {
            return min;
        }

        public Long getMax() {
            return max;
        }
    }

    private AxisRange groupByAxisRange(JsSeries s) {
        if (s.getDownsampleOptions() == DownsampleOptions.DISABLE) {
            return null;
        }
        if (!canDownsampleSeries(s)) {
            return null;
        }
        for (int i = 0; i < s.getSources().length; i++) {
            SeriesDataSource source = s.getSources()[i];
            if (!source.getColumnType().equals("io.deephaven.db.tables.utils.DBDateTime")) {
                continue;
            }
            DownsampledAxisDetails downsampledAxisDetails = downsampled.get(source.getAxis().getDescriptor());
            if (downsampledAxisDetails == null) {
                continue;
            }
            return new AxisRange(source.getDescriptor().getColumnName(), downsampledAxisDetails.min, downsampledAxisDetails.max);
        }
        return null;
    }

    private boolean canDownsampleSeries(JsSeries series) {
        if (series.getShapesVisible() == Boolean.TRUE) {
            return false;
        }
        // this was formerly a switch/case, but since we're referencing JS we need to use expressions that look like non-constants to java
        int plotStyle = series.getPlotStyle();
        if (plotStyle == FigureDescriptor.SeriesPlotStyle.getBAR() || plotStyle == FigureDescriptor.SeriesPlotStyle.getSTACKED_BAR() || plotStyle == FigureDescriptor.SeriesPlotStyle.getPIE()) {
            // category charts, can't remove categories
            return false;
        } else if (plotStyle == FigureDescriptor.SeriesPlotStyle.getSCATTER()) {
            // pointless without shapes visible, this ensures we aren't somehow trying to draw it
            return false;
        } else if (plotStyle == FigureDescriptor.SeriesPlotStyle.getLINE() || plotStyle == FigureDescriptor.SeriesPlotStyle.getAREA() || plotStyle == FigureDescriptor.SeriesPlotStyle.getSTACKED_AREA() || plotStyle == FigureDescriptor.SeriesPlotStyle.getHISTOGRAM() || plotStyle == FigureDescriptor.SeriesPlotStyle.getOHLC() || plotStyle == FigureDescriptor.SeriesPlotStyle.getSTEP() || plotStyle == FigureDescriptor.SeriesPlotStyle.getERROR_BAR()) {
            //allowed, fall through (listed so we can default to not downsample)
            return true;
        }
        //unsupported
        return false;
    }

    private DownsampleParams makeParamsForSeries(JsSeries s) {
        String[] yCols = new String[0];
        int pixels = 0;
        //... again, loop and find x axis, this time also y cols
        for (int i = 0; i < s.getSources().length; i++) {
            SeriesDataSource source = s.getSources()[i];
            DownsampledAxisDetails downsampledAxisDetails = downsampled.get(source.getAxis().getDescriptor());
            if (downsampledAxisDetails == null) {
                yCols[yCols.length] = source.getDescriptor().getColumnName();
            } else {
                pixels = downsampledAxisDetails.pixels;
            }
        }
        return new DownsampleParams(new JsSeries[]{s}, yCols, pixels);
    }

    // Then, aggregate the series instances and find the max pixel count, all the value columns to use
    public static class DownsampleParams {
        static DownsampleParams EMPTY = new DownsampleParams(new JsSeries[0] , new String[0], 0);

        private final JsSeries[] series;
        private final String[] yCols;
        private final int pixelCount;

        DownsampleParams(final JsSeries[] series, final String[] yCols, final int pixelCount) {
            this.series = series;
            this.yCols = yCols;
            this.pixelCount = pixelCount;
        }
        public DownsampleParams merge(DownsampleParams other) {
            return new DownsampleParams(
                    Stream.of(series, other.series)
                            .flatMap(Arrays::stream)
                            .distinct()
                            .toArray(JsSeries[]::new),
                    Stream.of(yCols, other.yCols)
                            .flatMap(Arrays::stream)
                            .distinct()
                            .toArray(String[]::new),
                    Math.max(pixelCount, other.pixelCount)
            );
        }
        public JsSeries[] getSeries() {
            return series;
        }

        public String[] getyCols() {
            return yCols;
        }

        public int getPixelCount() {
            return pixelCount;
        }
    }

    @JsIgnore
    public void enqueueSubscriptionCheck() {
        if (!subCheckEnqueued) {
            for (JsTable table : tables) {
                if (table.isClosed()) {
                    throw new IllegalStateException("Cannot subscribe, at least one table is disconnected");
                }
            }
            subCheckEnqueued = true;
            LazyPromise.runLater(this::updateSubscriptions);
        }
    }

    /**
     * Verifies that the underlying tables have the columns the series are expected.
     * Throws an FigureSourceException if not found
     */
    @JsIgnore
    public void verifyTables() {
        Arrays.stream(charts)
            .flatMap(c -> Arrays.stream(c.getSeries()))
            .forEach(s -> {
                JsTable table = tableForSeries(s);
                Arrays.stream(s.getSources())
                    .forEach(source -> {
                        try {
                            table.findColumn(source.getDescriptor().getColumnName());
                        } catch (NoSuchElementException e) {
                            throw new FigureSourceException(table, source, e.toString());
                        }
                    });
            });
    }

    public void close() {
        //explicit unsubscribe first, since those are handled separately from the table obj itself
        unsubscribe();

        if (onClose != null) {
            onClose.close(this);
        }

        if (tables != null) {
            Arrays.stream(tables).filter(jsTable -> !jsTable.isClosed()).forEach(JsTable::close);
        }
        if (tableMaps != null) {
            Arrays.stream(tableMaps).forEach(TableMap::close);
        }
    }

    @JsIgnore
    public int registerTable(JsTable table) {
        int id = plotHandlesToTables.size();
        registerTableWithId(table, Js.uncheckedCast(new double[] { id }));
        return id;
    }

    private void registerTableWithId(JsTable table, JsArray<Double> plotTableHandles) {
        for (int j = 0; j < plotTableHandles.length; j++) {
            plotHandlesToTables.put((int) (double) plotTableHandles.getAt(j), table);
        }
    }

    @JsIgnore
    public void updateDownsampleRange(AxisDescriptor axis, Integer pixels, Long min, Long max) {
        if (pixels == null) {
            downsampled.remove(axis);
        } else {
            if (axis.getLog() || axis.getType() != AxisDescriptor.AxisType.getX() || axis.getInvert()) {
                return;
            }
            downsampled.put(axis, new DownsampledAxisDetails(pixels, min, max));
        }
        enqueueSubscriptionCheck();
    }

    /**
     * Tracks ranges that an axis has registered for.
     */
    public static class DownsampledAxisDetails {
        private final int pixels;
        private final Long min;
        private final Long max;

        public DownsampledAxisDetails(final int pixels, final Long min, final Long max) {
            this.pixels = pixels;
            this.min = min;
            this.max = max;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            final DownsampledAxisDetails that = (DownsampledAxisDetails) o;

            if (pixels != that.pixels) return false;
            if (min != null ? !min.equals(that.min) : that.min != null) return false;
            return max != null ? max.equals(that.max) : that.max == null;
        }

        @Override
        public int hashCode() {
            int result = pixels;
            result = 31 * result + (min != null ? min.hashCode() : 0);
            result = 31 * result + (max != null ? max.hashCode() : 0);
            return result;
        }
    }

    public static class FigureTableFetchData {
        private JsTable[] tables;

        private TableMap[] tableMaps;
        private Map<Integer, TableMap> plotHandlesToTableMaps;
        private FigureClose onClose;

        public FigureTableFetchData(
                JsTable[] tables,
                TableMap[] tableMaps,
                Map<Integer, TableMap> plotHandlesToTableMaps
        ) {
            this(tables, tableMaps, plotHandlesToTableMaps, null);
        }

        public FigureTableFetchData(
                JsTable[] tables,
                TableMap[] tableMaps,
                Map<Integer, TableMap> plotHandlesToTableMaps,
                FigureClose onClose
        ) {
            this.tables = tables;
            this.tableMaps = tableMaps;
            this.plotHandlesToTableMaps = plotHandlesToTableMaps;

            // Called when the figure is being closed
            this.onClose = onClose;
        }
    }


    private static class DefaultFigureTableFetch implements FigureTableFetch {
        private WorkerConnection connection;
        DefaultFigureTableFetch(WorkerConnection connection) {
            this.connection = connection;
        }

        @Override
        public Promise fetch(JsFigure figure, FigureDescriptor descriptor) {
            JsTable[] tables;

            // TODO (deephaven-core#62) implement fetch for tablemaps
//            // iterate through the tablemaps we're supposed to have, fetch keys for them, and construct them
            TableMap[] tableMaps = new TableMap[0];//new TableMap[descriptor.getTableMapIdsList().length];
//            Promise<?>[] tableMapPromises = new Promise[descriptor.getTablemapsList().length];
            Map<Integer, TableMap> plotHandlesToTableMaps = new HashMap<>();
//            for (int i = 0; i < descriptor.getTablemapsList().length; i++) {
//                final int index = i;
//                tableMapPromises[i] = Callbacks
//                        .<ColumnData, String>promise(null, c -> {
////                            connection.getServer().getTableMapKeys(descriptor.getTableMaps()[index], c);
//                            throw new UnsupportedOperationException("getTableMapKeys");
//                        })
//                        .then(keys -> {
//                            TableMapDeclaration decl = new TableMapDeclaration();
//                            decl.setKeys(keys);
//                            decl.setHandle(descriptor.getTablemapsList().getAt(index));
//                            TableMap tableMap = new TableMap(connection, c -> c.onSuccess(decl));
//
//                            // never attempt a reconnect, we'll get a new tablemap with the figure when it reconnects
//                            tableMap.addEventListener(TableMap.EVENT_DISCONNECT, ignore -> tableMap.close());
//
//                            JsArray<Double> plotIds = descriptor.getTablemapidsList().getAt(index).getIdsList();
//                            for (int j = 0; j < plotIds.length; j++) {
//                                plotHandlesToTableMaps.put((int) (double) plotIds.getAt(j), tableMap);
//                            }
//                            tableMaps[index] = tableMap;
//                            return tableMap.refetch();
//                        });
//            }

            // iterate through the table handles we're supposed to have and prep TableHandles for them
            tables = new JsTable[descriptor.getTablesList().length];

            for (int i = 0; i < descriptor.getTablesList().length; i++) {
                ClientTableState clientTableState = connection.newStateFromUnsolicitedTable(descriptor.getTablesList().getAt(i), "table " + i + " for plot");
                JsTable table = new JsTable(connection, clientTableState);
                // never attempt a reconnect, since we might have a different figure schema entirely
                table.addEventListener(JsTable.EVENT_DISCONNECT, ignore -> table.close());
                tables[i] = table;
            }

            connection.registerFigure(figure);

            return Promise.resolve(
                    new FigureTableFetchData(tables, tableMaps, plotHandlesToTableMaps, f -> this.connection.releaseFigure(f))
            );
        }
    }
}

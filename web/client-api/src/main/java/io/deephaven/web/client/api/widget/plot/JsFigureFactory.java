package io.deephaven.web.client.api.widget.plot;

import elemental2.core.JsArray;
import elemental2.dom.CustomEventInit;
import elemental2.promise.Promise;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.FetchFigureResponse;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.FigureDescriptor;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.figuredescriptor.*;
import io.deephaven.web.client.api.JsTable;
import io.deephaven.web.client.api.TableMap;
import io.deephaven.web.client.fu.JsPromise;
import io.deephaven.web.shared.fu.RemoverFn;
import jsinterop.annotations.JsMethod;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class JsFigureFactory {
    @JsMethod(namespace = "dh.plot.Figure", name = "create")
    public static Promise<JsFigure> create(Object config) {
        if (config instanceof JsFigureDescriptor) {
            return create((JsFigureDescriptor) config);
        } else {
            JsFigureDescriptor descriptor = new JsFigureDescriptor((JsPropertyMap<Object>) config);
            return create(descriptor);
        }
    }

    private static Promise<JsFigure> create(JsFigureDescriptor descriptor) {
        JsArray<JsTable> tables = descriptor.getTables();

        if (tables == null || tables.length == 0) {
            return (Promise<JsFigure>)(Promise)Promise.reject("No tables provided for Figure creation");
        }

        FigureDescriptor figureDescriptor = convertJsFigureDescriptor(descriptor);
        FetchFigureResponse response = new FetchFigureResponse();
        response.setFiguredescriptor(figureDescriptor);
        return JsPromise.all(tables.map((table, index, all) -> table.copy(false)))
            .then(tableCopies -> new JsFigure(
                c -> c.apply(null, response),
                (figure, descriptor1) -> {
                    // We need to listen for disconnects and reconnects
                    boolean[] isTableDisconnected = new boolean[tableCopies.length];
                    ArrayList<RemoverFn> removerFns = new ArrayList<>(tableCopies.length * 3);

                    for (int i = 0; i < tableCopies.length; i++) {
                        final int tableIndex = i;
                        // Tables are closed when the figure is closed, no need to remove listeners later
                        removerFns.add(tableCopies[i].addEventListener(JsTable.EVENT_DISCONNECT, ignore -> {
                            isTableDisconnected[tableIndex] = true;
                            for (int j = 0; j < isTableDisconnected.length; j++) {
                                if (isTableDisconnected[j] && j != tableIndex) {
                                    return;
                                }
                            }

                            figure.fireEvent(JsFigure.EVENT_DISCONNECT);
                            figure.unsubscribe();
                        }));
                        removerFns.add(tableCopies[i].addEventListener(JsTable.EVENT_RECONNECT, ignore -> {
                            isTableDisconnected[tableIndex] = false;
                            for (int j = 0; j < isTableDisconnected.length; j++) {
                                if (isTableDisconnected[j]) {
                                    return;
                                }
                            }

                            try {
                                figure.verifyTables();
                                figure.fireEvent(JsFigure.EVENT_RECONNECT);
                                figure.enqueueSubscriptionCheck();
                            } catch (JsFigure.FigureSourceException e) {
                                final CustomEventInit init = CustomEventInit.create();
                                init.setDetail(e);
                                figure.fireEvent(JsFigure.EVENT_RECONNECTFAILED, init);
                            }
                        }));
                        removerFns.add(tableCopies[i].addEventListener(JsTable.EVENT_RECONNECTFAILED, err -> {
                            for (RemoverFn removerFn : removerFns) {
                                removerFn.remove();
                            }
                            figure.unsubscribe();

                            final CustomEventInit init = CustomEventInit.create();
                            init.setDetail(err);
                            figure.fireEvent(JsFigure.EVENT_RECONNECTFAILED, init);
                        }));
                    }

                    return Promise.resolve(new JsFigure.FigureTableFetchData(
                        tableCopies,
                        new TableMap[0],
                        Collections.emptyMap()
                    ));
                }
            ).refetch());
    }

    private static FigureDescriptor convertJsFigureDescriptor(JsFigureDescriptor jsDescriptor) {
        FigureDescriptor descriptor = new FigureDescriptor();
        descriptor.setTitle(jsDescriptor.title);
        descriptor.setTitlefont(jsDescriptor.titleFont);
        descriptor.setTitlecolor(jsDescriptor.titleColor);
//        descriptor.setResizable(jsDescriptor.isResizable);
//        descriptor.setDefaultTheme(jsDescriptor.isDefaultTheme);
        descriptor.setUpdateinterval(jsDescriptor.updateInterval);
        descriptor.setCols(jsDescriptor.cols);
        descriptor.setRows(jsDescriptor.rows);

        JsArray<JsTable> tables = jsDescriptor.getTables();
        // The only thing used by the Figure with the tableIds (outside of the default fetchTables function) is the
        // length of these tableIds.
        descriptor.setTableidsList(new JsArray<>());
        descriptor.getTableidsList().length = tables.length;

        // There's just a straight mapping of plot handles to table ids
        JsArray<RepeatedInt32> plotHandleIds = new JsArray<>();
        for (int i = 0; i < tables.length; i++) {
            RepeatedInt32 repeatedInt32 = new RepeatedInt32();
            repeatedInt32.addIds(i);
            plotHandleIds.push(repeatedInt32);
        }
        descriptor.setPlothandleidsList(plotHandleIds);

        JsArray<JsChartDescriptor> charts = jsDescriptor.charts;
        ChartDescriptor[] chartDescriptors = new ChartDescriptor[charts.length];
        for (int i = 0; i < charts.length; i++) {
            chartDescriptors[i] = convertJsChartDescriptor(charts.getAt(i), tables);
        }
        descriptor.setChartsList(chartDescriptors);

        return descriptor;
    }

    private static ChartDescriptor convertJsChartDescriptor(JsChartDescriptor jsDescriptor, JsArray<JsTable> tables) {
        ChartDescriptor descriptor = new ChartDescriptor();

        descriptor.setColspan(jsDescriptor.colspan);
        descriptor.setRowspan(jsDescriptor.rowspan);

        if (jsDescriptor.chartType != null) {
            descriptor.setCharttype(Js.coerceToInt(jsDescriptor.chartType));
        }

        descriptor.setTitle(jsDescriptor.title);
        descriptor.setTitlefont(jsDescriptor.titleFont);
        descriptor.setTitlecolor(jsDescriptor.titleColor);

        descriptor.setShowlegend(jsDescriptor.showLegend);
        descriptor.setLegendfont(jsDescriptor.legendFont);
        descriptor.setLegendcolor(jsDescriptor.legendColor);

        descriptor.setIs3d(jsDescriptor.is3d);

        JsArray<JsAxisDescriptor> jsAxes = jsDescriptor.axes;
        AxisDescriptor[] axes = new AxisDescriptor[jsAxes.length];
        Map<JsAxisDescriptor, AxisDescriptor> axisMap = new HashMap<>();
        for (int i = 0; i < jsAxes.length; i++) {
            JsAxisDescriptor jsAxis = jsAxes.getAt(i);
            axes[i] = convertJsAxisDescriptor(jsAxis);
            axes[i].setId(Integer.toString(i));
            axisMap.put(jsAxis, axes[i]);
        }
        descriptor.setAxesList(axes);

        JsArray<JsSeriesDescriptor> jsSeries = jsDescriptor.series;
        SeriesDescriptor[] seriesDescriptors = new SeriesDescriptor[jsSeries.length];
        for (int i = 0; i < jsSeries.length; i++) {
            seriesDescriptors[i] = convertJsSeriesDescriptor(jsSeries.getAt(i), tables, axisMap);
        }
        descriptor.setSeriesList(seriesDescriptors);

        // TODO: IDS-5767 Add support for byExternal and multiseries descriptors
        descriptor.setMultiseriesList(new MultiSeriesDescriptor[0]);

        return descriptor;
    }

    private static AxisDescriptor convertJsAxisDescriptor(JsAxisDescriptor jsDescriptor) {
        AxisDescriptor descriptor = new AxisDescriptor();

        descriptor.setFormattype(Js.coerceToInt(jsDescriptor.formatType));
        descriptor.setType(Js.coerceToInt(jsDescriptor.type));
        descriptor.setPosition(Js.coerceToInt(jsDescriptor.position));
        descriptor.setLog(jsDescriptor.log);
        descriptor.setLabel(jsDescriptor.label);
        descriptor.setLabelfont(jsDescriptor.labelFont);
        descriptor.setTicksfont(jsDescriptor.ticksFont);
        descriptor.setFormatpattern(jsDescriptor.formatPattern);
        descriptor.setColor(jsDescriptor.color);
        descriptor.setMinrange(jsDescriptor.minRange);
        descriptor.setMaxrange(jsDescriptor.maxRange);
        descriptor.setMinorticksvisible(jsDescriptor.minorTicksVisible);
        descriptor.setMajorticksvisible(jsDescriptor.majorTicksVisible);
        descriptor.setMinortickcount(jsDescriptor.minorTickCount);
        descriptor.setGapbetweenmajorticks(jsDescriptor.gapBetweenMajorTicks);
        descriptor.setMajorticklocationsList(Js.<JsArray<Double>>uncheckedCast(jsDescriptor.majorTickLocations));
        descriptor.setTicklabelangle(jsDescriptor.tickLabelAngle);
        descriptor.setInvert(jsDescriptor.invert);
        descriptor.setIstimeaxis(jsDescriptor.isTimeAxis);

        return descriptor;
    }

    private static SeriesDescriptor convertJsSeriesDescriptor(JsSeriesDescriptor jsDescriptor, JsArray<JsTable> tables, Map<JsAxisDescriptor, AxisDescriptor> axisMap) {
        SeriesDescriptor descriptor = new SeriesDescriptor();

        descriptor.setPlotstyle(Js.coerceToInt(jsDescriptor.plotStyle));
        descriptor.setName(jsDescriptor.name);
        if (jsDescriptor.linesVisible != null) {
            descriptor.setLinesvisible(jsDescriptor.linesVisible);
        }
        if (jsDescriptor.shapesVisible != null) {
            descriptor.setShapesvisible(jsDescriptor.shapesVisible);
        }
        descriptor.setGradientvisible(jsDescriptor.gradientVisible != null ? jsDescriptor.gradientVisible : false);
        descriptor.setLinecolor(jsDescriptor.lineColor);
        descriptor.setPointlabelformat(jsDescriptor.pointLabelFormat);
        descriptor.setXtooltippattern(jsDescriptor.xToolTipPattern);
        descriptor.setYtooltippattern(jsDescriptor.yToolTipPattern);

        descriptor.setShapelabel(jsDescriptor.shapeLabel);
        if (jsDescriptor.shapeSize != null) {
            descriptor.setShapesize(jsDescriptor.shapeSize);
        }
        descriptor.setShapecolor(jsDescriptor.shapeColor);
        descriptor.setShape(jsDescriptor.shape);

        JsArray<JsSourceDescriptor> jsDataSources = jsDescriptor.dataSources;
        SourceDescriptor[] dataSources = new SourceDescriptor[jsDataSources.length];
        for (int i = 0; i < jsDataSources.length; i++) {
            dataSources[i] = convertJsSourceDescriptor(jsDataSources.getAt(i), tables, axisMap);
        }
        descriptor.setDatasourcesList(dataSources);

        return descriptor;
    }

    private static SourceDescriptor convertJsSourceDescriptor(JsSourceDescriptor jsDescriptor, JsArray<JsTable> tables, Map<JsAxisDescriptor, AxisDescriptor> axisMap) {
        SourceDescriptor descriptor = new SourceDescriptor();

        descriptor.setAxis(axisMap.get(jsDescriptor.axis));
        descriptor.setTableid(tables.indexOf(jsDescriptor.table));
        descriptor.setColumnname(jsDescriptor.columnName);
        descriptor.setType(Js.coerceToInt(jsDescriptor.type));

        return descriptor;
    }
}

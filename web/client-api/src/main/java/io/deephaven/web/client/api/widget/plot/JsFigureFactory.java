//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.widget.plot;

import com.vertispan.tsdefs.annotations.TsTypeRef;
import elemental2.core.JsArray;
import elemental2.promise.Promise;
import io.deephaven.proto.backplane.grpc.FetchObjectResponse;
import io.deephaven.proto.backplane.script.grpc.FigureDescriptor;
import io.deephaven.web.client.api.JsPartitionedTable;
import io.deephaven.web.client.api.JsTable;
import io.deephaven.web.shared.fu.RemoverFn;
import jsinterop.annotations.JsMethod;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class JsFigureFactory {
    @JsMethod(namespace = "dh.plot.Figure", name = "create")
    public static Promise<JsFigure> create(@TsTypeRef(JsFigureDescriptor.class) Object config) {
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
            return (Promise<JsFigure>) (Promise) Promise.reject("No tables provided for Figure creation");
        }

        FigureDescriptor figureDescriptor = convertJsFigureDescriptor(descriptor);
        FetchObjectResponse.Builder response = FetchObjectResponse.newBuilder();
        response.setData(figureDescriptor.toByteString());
        Promise<?>[] tableCopyPromises =
                tables.map((table, index) -> table.copy(false)).asArray(new Promise[0]);
        return Promise.all(tableCopyPromises)
                .then(unknownTableCopies -> {
                    JsArray<JsTable> jsTableCopies = Js.cast(unknownTableCopies);
                    JsTable[] tableCopies = jsTableCopies.asArray(new JsTable[0]);
                    return new JsFigure(
                            () -> Promise.resolve(response.build()),
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
                                            figure.fireEvent(JsFigure.EVENT_RECONNECTFAILED, e);
                                        }
                                    }));
                                    removerFns
                                            .add(tableCopies[i].addEventListener(JsTable.EVENT_RECONNECTFAILED, err -> {
                                                for (RemoverFn removerFn : removerFns) {
                                                    removerFn.remove();
                                                }
                                                figure.unsubscribe();

                                                figure.fireEvent(JsFigure.EVENT_RECONNECTFAILED, err);
                                            }));
                                }

                                return Promise.resolve(new JsFigure.FigureTableFetchData(
                                        tableCopies,
                                        new JsPartitionedTable[0]));
                            }).refetch();
                });
    }

    private static FigureDescriptor convertJsFigureDescriptor(JsFigureDescriptor jsDescriptor) {
        FigureDescriptor.Builder descriptor = FigureDescriptor.newBuilder();
        descriptor.setTitle(jsDescriptor.title);
        descriptor.setTitleFont(jsDescriptor.titleFont);
        descriptor.setTitleColor(jsDescriptor.titleColor);
        descriptor.setUpdateInterval((long) jsDescriptor.updateInterval);
        descriptor.setCols(jsDescriptor.cols);
        descriptor.setRows(jsDescriptor.rows);

        JsArray<JsTable> tables = jsDescriptor.getTables();

        JsArray<JsChartDescriptor> charts = jsDescriptor.charts;
        for (int i = 0; i < charts.length; i++) {
            descriptor.addCharts(convertJsChartDescriptor(charts.getAt(i), tables));
        }

        return descriptor.build();
    }

    private static FigureDescriptor.ChartDescriptor convertJsChartDescriptor(JsChartDescriptor jsDescriptor,
            JsArray<JsTable> tables) {
        FigureDescriptor.ChartDescriptor.Builder descriptor = FigureDescriptor.ChartDescriptor.newBuilder();

        descriptor.setColspan(jsDescriptor.colspan);
        descriptor.setRowspan(jsDescriptor.rowspan);

        if (jsDescriptor.chartType != null) {
            descriptor.setChartType(
                    FigureDescriptor.ChartDescriptor.ChartType.forNumber(jsDescriptor.chartType.coerceToInt()));
        }

        descriptor.setTitle(jsDescriptor.title);
        descriptor.setTitleFont(jsDescriptor.titleFont);
        descriptor.setTitleColor(jsDescriptor.titleColor);

        descriptor.setShowLegend(jsDescriptor.showLegend);
        descriptor.setLegendFont(jsDescriptor.legendFont);
        descriptor.setLegendColor(jsDescriptor.legendColor);

        descriptor.setIs3D(jsDescriptor.is3d);

        JsArray<JsAxisDescriptor> jsAxes = jsDescriptor.axes;
        Map<JsAxisDescriptor, FigureDescriptor.AxisDescriptor> axisMap = new HashMap<>();
        for (int i = 0; i < jsAxes.length; i++) {
            JsAxisDescriptor jsAxis = jsAxes.getAt(i);
            FigureDescriptor.AxisDescriptor converted = convertJsAxisDescriptor(jsAxis)
                    .setId(Integer.toString(i))
                    .build();
            axisMap.put(jsAxis, converted);
            descriptor.addAxes(converted);
        }

        JsArray<JsSeriesDescriptor> jsSeries = jsDescriptor.series;
        for (int i = 0; i < jsSeries.length; i++) {
            descriptor.addSeries(convertJsSeriesDescriptor(jsSeries.getAt(i), tables, axisMap));
        }

        // TODO: IDS-5767 Add support for partitionBy and multiseries descriptors

        return descriptor.build();
    }

    private static FigureDescriptor.AxisDescriptor.Builder convertJsAxisDescriptor(JsAxisDescriptor jsDescriptor) {
        FigureDescriptor.AxisDescriptor.Builder descriptor = FigureDescriptor.AxisDescriptor.newBuilder();

        descriptor.setFormatType(
                FigureDescriptor.AxisDescriptor.AxisFormatType.forNumber(jsDescriptor.formatType.coerceToInt()));
        descriptor.setType(FigureDescriptor.AxisDescriptor.AxisType.forNumber(jsDescriptor.type.coerceToInt()));
        descriptor.setPosition(
                FigureDescriptor.AxisDescriptor.AxisPosition.forNumber(jsDescriptor.position.coerceToInt()));
        descriptor.setLog(jsDescriptor.log);
        descriptor.setLabel(jsDescriptor.label);
        descriptor.setLabelFont(jsDescriptor.labelFont);
        descriptor.setTicksFont(jsDescriptor.ticksFont);
        descriptor.setFormatPattern(jsDescriptor.formatPattern);
        descriptor.setColor(jsDescriptor.color);
        descriptor.setMinRange(jsDescriptor.minRange);
        descriptor.setMaxRange(jsDescriptor.maxRange);
        descriptor.setMinorTicksVisible(jsDescriptor.minorTicksVisible);
        descriptor.setMajorTicksVisible(jsDescriptor.majorTicksVisible);
        descriptor.setMinorTickCount(jsDescriptor.minorTickCount);
        descriptor.setGapBetweenMajorTicks(jsDescriptor.gapBetweenMajorTicks);
        descriptor.addAllMajorTickLocations(jsDescriptor.majorTickLocations.asList());
        descriptor.setTickLabelAngle(jsDescriptor.tickLabelAngle);
        descriptor.setInvert(jsDescriptor.invert);
        descriptor.setIsTimeAxis(jsDescriptor.isTimeAxis);

        return descriptor;
    }

    private static FigureDescriptor.SeriesDescriptor convertJsSeriesDescriptor(JsSeriesDescriptor jsDescriptor,
            JsArray<JsTable> tables,
            Map<JsAxisDescriptor, FigureDescriptor.AxisDescriptor> axisMap) {
        FigureDescriptor.SeriesDescriptor.Builder descriptor = FigureDescriptor.SeriesDescriptor.newBuilder();

        descriptor.setPlotStyle(FigureDescriptor.SeriesPlotStyle.forNumber(jsDescriptor.plotStyle.coerceToInt()));
        descriptor.setName(jsDescriptor.name);
        if (jsDescriptor.linesVisible != null) {
            descriptor.setLinesVisible(jsDescriptor.linesVisible);
        }
        if (jsDescriptor.shapesVisible != null) {
            descriptor.setShapesVisible(jsDescriptor.shapesVisible);
        }
        descriptor.setGradientVisible(jsDescriptor.gradientVisible != null ? jsDescriptor.gradientVisible : false);
        descriptor.setLineColor(jsDescriptor.lineColor);
        descriptor.setPointLabelFormat(jsDescriptor.pointLabelFormat);
        descriptor.setXToolTipPattern(jsDescriptor.xToolTipPattern);
        descriptor.setYToolTipPattern(jsDescriptor.yToolTipPattern);

        descriptor.setShapeLabel(jsDescriptor.shapeLabel);
        if (jsDescriptor.shapeSize != null) {
            descriptor.setShapeSize(jsDescriptor.shapeSize);
        }
        descriptor.setShapeColor(jsDescriptor.shapeColor);
        descriptor.setShape(jsDescriptor.shape);

        JsArray<JsSourceDescriptor> jsDataSources = jsDescriptor.dataSources;
        for (int i = 0; i < jsDataSources.length; i++) {
            descriptor.addDataSources(convertJsSourceDescriptor(jsDataSources.getAt(i), tables, axisMap));
        }

        return descriptor.build();
    }

    private static FigureDescriptor.SourceDescriptor convertJsSourceDescriptor(JsSourceDescriptor jsDescriptor,
            JsArray<JsTable> tables,
            Map<JsAxisDescriptor, FigureDescriptor.AxisDescriptor> axisMap) {
        FigureDescriptor.SourceDescriptor.Builder descriptor = FigureDescriptor.SourceDescriptor.newBuilder();

        descriptor.setAxisId(axisMap.get(jsDescriptor.axis).getId());
        descriptor.setTableId(tables.indexOf(jsDescriptor.table));
        descriptor.setColumnName(jsDescriptor.columnName);
        descriptor.setType(FigureDescriptor.SourceType.forNumber(jsDescriptor.type.coerceToInt()));

        return descriptor.build();
    }
}

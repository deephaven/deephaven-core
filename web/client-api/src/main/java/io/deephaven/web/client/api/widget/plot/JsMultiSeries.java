//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.widget.plot;

import com.vertispan.tsdefs.annotations.TsInterface;
import com.vertispan.tsdefs.annotations.TsName;
import com.vertispan.tsdefs.annotations.TsTypeRef;
import io.deephaven.proto.backplane.script.grpc.FigureDescriptor;
import io.deephaven.web.client.api.JsPartitionedTable;
import io.deephaven.web.client.api.widget.plot.enums.JsSeriesPlotStyle;
import jsinterop.annotations.JsProperty;

import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Describes a template that will be used to make new series instances when a new table added to a {@code plotBy}.
 */
@TsInterface
@TsName(name = "MultiSeries", namespace = "dh.plot")
public class JsMultiSeries {
    private final FigureDescriptor.MultiSeriesDescriptor descriptor;
    private final JsFigure figure;
    private final Map<String, JsAxis> axes;
    private final JsChart parent;

    public JsMultiSeries(FigureDescriptor.MultiSeriesDescriptor descriptor, JsFigure figure, Map<String, JsAxis> axes,
            JsChart parent) {

        this.descriptor = descriptor;
        this.figure = figure;
        this.axes = axes;
        this.parent = parent;
    }

    public void initSources(Map<Integer, JsPartitionedTable> plotHandlesToPartitionedTables) {
        descriptor.getDataSourcesList().stream()
                .mapToInt(FigureDescriptor.MultiSeriesSourceDescriptor::getPartitionedTableId)
                .distinct()
                // TODO assert only one at this stage
                .forEach(plotHandle -> {
                    JsPartitionedTable partitionedTable = plotHandlesToPartitionedTables.get(plotHandle);
                    partitionedTable.getKeys().forEach((p0, p1, p2) -> {
                        requestTable(partitionedTable, p0);
                        return null;
                    });
                    partitionedTable.addEventListener(JsPartitionedTable.EVENT_KEYADDED, event -> {
                        requestTable(partitionedTable, event.getDetail());
                    });

                });
    }

    private void requestTable(JsPartitionedTable partitionedTable, Object key) {
        // TODO ask the server in parallel for the series name
        String seriesName = descriptor.getName() + ": " + key;
        partitionedTable.getTable(key).then(table -> {
            FigureDescriptor.SeriesDescriptor.Builder seriesInstance = FigureDescriptor.SeriesDescriptor.newBuilder();

            seriesInstance.setName(seriesName);
            seriesInstance.setPlotStyle(FigureDescriptor.SeriesPlotStyle.forNumber(getPlotStyle()));

            seriesInstance.setLineColor(getOrDefault(seriesName, descriptor.getLineColor()));
            seriesInstance.setShapeColor(getOrDefault(seriesName, descriptor.getPointColor()));
            seriesInstance.setLinesVisible(getOrDefault(seriesName, descriptor.getLinesVisible()));
            seriesInstance.setShapesVisible(getOrDefault(seriesName, descriptor.getPointsVisible()));
            Boolean gradientVisible = getOrDefault(seriesName, descriptor.getGradientVisible());
            if (gradientVisible != null) {
                seriesInstance.setGradientVisible(gradientVisible);
            }

            seriesInstance.setYToolTipPattern(getOrDefault(seriesName, descriptor.getYToolTipPattern()));
            seriesInstance.setXToolTipPattern(getOrDefault(seriesName, descriptor.getXToolTipPattern()));

            seriesInstance.setShapeLabel(getOrDefault(seriesName, descriptor.getPointLabel()));
            seriesInstance.setShapeSize(getOrDefault(seriesName, descriptor.getPointSize()));
            seriesInstance.setShape(getOrDefault(seriesName, descriptor.getPointShape()));

            seriesInstance.setPointLabelFormat(getOrDefault(seriesName, descriptor.getPointLabelFormat()));

            int tableId = figure.registerTable(table);

            seriesInstance.addAllDataSources(
                    descriptor.getDataSourcesList()
                            .stream()
                            .map((multiSeriesSource) -> {
                                return FigureDescriptor.SourceDescriptor.newBuilder()
                                        .setColumnName(multiSeriesSource.getColumnName())
                                        .setAxisId(multiSeriesSource.getAxisId())
                                        .setTableId(tableId)
                                        .setType(multiSeriesSource.getType())
                                        .build();
                            })
                            .collect(Collectors.toList())

            );

            JsSeries series = new JsSeries(seriesInstance.build(), figure, axes);
            series.setMultiSeries(this);
            series.initSources(Collections.singletonMap(tableId, table), Collections.emptyMap());

            parent.addSeriesFromMultiSeries(series);

            figure.fireEvent(JsFigure.EVENT_SERIES_ADDED, series);
            parent.fireEvent(JsChart.EVENT_SERIES_ADDED, series);
            return null;
        });
    }

    private boolean getOrDefault(String name, FigureDescriptor.BoolMapWithDefault map) {
        int index = map.getKeysList().indexOf(name);
        if (index == -1) {
            return map.getDefaultBool();
        }
        return map.getValuesList().get(index);
    }

    private String getOrDefault(String name, FigureDescriptor.StringMapWithDefault map) {
        int index = map.getKeysList().indexOf(name);
        if (index == -1) {
            return map.getDefaultString();
        }
        return map.getValuesList().get(index);
    }

    private double getOrDefault(String name, FigureDescriptor.DoubleMapWithDefault map) {
        int index = map.getKeysList().indexOf(name);
        if (index == -1) {
            return map.getDefaultDouble();
        }
        return map.getValuesList().get(index);
    }

    /**
     * The plotting style to use for the series that will be created. See {@code SeriesPlotStyle} enum for more details.
     * 
     * @return int
     *
     */
    @JsProperty
    @TsTypeRef(JsSeriesPlotStyle.class)
    public int getPlotStyle() {
        return descriptor.getPlotStyle().getNumber();
    }

    /**
     * The name for this multi-series.
     * 
     * @return String
     */
    @JsProperty
    public String getName() {
        return descriptor.getName();
    }
}

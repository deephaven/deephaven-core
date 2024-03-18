//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.widget.plot;

import com.vertispan.tsdefs.annotations.TsInterface;
import com.vertispan.tsdefs.annotations.TsName;
import com.vertispan.tsdefs.annotations.TsTypeRef;
import elemental2.dom.CustomEvent;
import elemental2.dom.CustomEventInit;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.figuredescriptor.*;
import io.deephaven.web.client.api.JsPartitionedTable;
import io.deephaven.web.client.api.widget.plot.enums.JsSeriesPlotStyle;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsProperty;

import java.util.Collections;
import java.util.Map;

/**
 * Describes a template that will be used to make new series instances when a new table added to a plotBy.
 */
@TsInterface
@TsName(name = "MultiSeries", namespace = "dh.plot")
public class JsMultiSeries {
    private final MultiSeriesDescriptor descriptor;
    private final JsFigure figure;
    private final Map<String, JsAxis> axes;
    private final JsChart parent;

    public JsMultiSeries(MultiSeriesDescriptor descriptor, JsFigure figure, Map<String, JsAxis> axes, JsChart parent) {

        this.descriptor = descriptor;
        this.figure = figure;
        this.axes = axes;
        this.parent = parent;
    }

    public void initSources(Map<Integer, JsPartitionedTable> plotHandlesToPartitionedTables) {
        descriptor.getDataSourcesList().asList().stream().mapToInt(MultiSeriesSourceDescriptor::getPartitionedTableId)
                .distinct()
                // TODO assert only one at this stage
                .forEach(plotHandle -> {
                    JsPartitionedTable partitionedTable = plotHandlesToPartitionedTables.get(plotHandle);
                    partitionedTable.getKeys().forEach((p0, p1, p2) -> {
                        requestTable(partitionedTable, p0);
                        return null;
                    });
                    partitionedTable.addEventListener(JsPartitionedTable.EVENT_KEYADDED, event -> {
                        requestTable(partitionedTable, ((CustomEvent) event).detail);
                    });

                });
    }

    private void requestTable(JsPartitionedTable partitionedTable, Object key) {
        // TODO ask the server in parallel for the series name
        String seriesName = descriptor.getName() + ": " + key;
        partitionedTable.getTable(key).then(table -> {
            SeriesDescriptor seriesInstance = new SeriesDescriptor();

            seriesInstance.setName(seriesName);
            seriesInstance.setPlotStyle(getPlotStyle());

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

            seriesInstance.setDataSourcesList(
                    descriptor.getDataSourcesList()
                            .map((multiSeriesSource, p1, p2) -> {
                                SourceDescriptor sourceDescriptor = new SourceDescriptor();
                                sourceDescriptor.setColumnName(multiSeriesSource.getColumnName());
                                sourceDescriptor.setAxisId(multiSeriesSource.getAxisId());
                                sourceDescriptor.setTableId(tableId);
                                sourceDescriptor.setType(multiSeriesSource.getType());
                                return sourceDescriptor;
                            })

            );

            JsSeries series = new JsSeries(seriesInstance, figure, axes);
            series.setMultiSeries(this);
            series.initSources(Collections.singletonMap(tableId, table), Collections.emptyMap());

            CustomEventInit init = CustomEventInit.create();
            init.setDetail(series);

            parent.addSeriesFromMultiSeries(series);

            figure.fireEvent(JsFigure.EVENT_SERIES_ADDED, init);
            parent.fireEvent(JsChart.EVENT_SERIES_ADDED, init);
            return null;
        });
    }

    private boolean getOrDefault(String name, BoolMapWithDefault map) {
        int index = map.getKeysList().findIndex((p0, p1, p2) -> name.equals(p0));
        if (index == -1) {
            return map.getDefaultBool();
        }
        return map.getValuesList().getAt(index);
    }

    private String getOrDefault(String name, StringMapWithDefault map) {
        int index = map.getKeysList().findIndex((p0, p1, p2) -> name.equals(p0));
        if (index == -1) {
            return map.getDefaultString();
        }
        return map.getValuesList().getAt(index);
    }

    private double getOrDefault(String name, DoubleMapWithDefault map) {
        int index = map.getKeysList().findIndex((p0, p1, p2) -> name.equals(p0));
        if (index == -1) {
            return map.getDefaultDouble();
        }
        return map.getValuesList().getAt(index);
    }

    /**
     * The plotting style to use for the series that will be created. See <b>SeriesPlotStyle</b> enum for more details.
     * 
     * @return int
     *
     */
    @JsProperty
    @TsTypeRef(JsSeriesPlotStyle.class)
    public int getPlotStyle() {
        return descriptor.getPlotStyle();
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

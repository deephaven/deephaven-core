package io.deephaven.web.client.api.widget.plot;

import elemental2.dom.CustomEvent;
import elemental2.dom.CustomEventInit;
import io.deephaven.web.client.api.TableMap;
import io.deephaven.web.shared.data.plot.*;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsProperty;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

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

    @JsIgnore
    public void initSources(Map<Integer, TableMap> plotHandlesToTableMaps) {
        Arrays.stream(descriptor.getDataSources()).mapToInt(MultiSeriesSourceDescriptor::getTableMapId).distinct()
                //TODO assert only one at this stage
                .forEach(plotHandle -> {
            TableMap tableMap = plotHandlesToTableMaps.get(plotHandle);
            tableMap.getKeys().forEach((p0, p1, p2) -> {
                requestTable(tableMap, p0);
                return null;
            });
            tableMap.addEventListener(TableMap.EVENT_KEYADDED, event -> {
                requestTable(tableMap, ((CustomEvent)event).detail);
            });

        });
    }

    private void requestTable(TableMap tableMap, Object key) {
        //TODO ask the server in parallel for the series name
        String seriesName = descriptor.getName() + ": " + key;
        tableMap.getTable(key).then(table -> {
            SeriesDescriptor seriesInstance = new SeriesDescriptor();

            seriesInstance.setName(seriesName);
            seriesInstance.setPlotStyle(getPlotStyle());

            seriesInstance.setLineColor(getOrDefault(seriesName, descriptor.getLineColorDefault(), descriptor.getLineColorKeys(), descriptor.getLineColorValues()));
            seriesInstance.setShapeColor(getOrDefault(seriesName, descriptor.getPointColorDefault(), descriptor.getPointColorKeys(), descriptor.getPointColorValues()));
            seriesInstance.setLinesVisible(getOrDefault(seriesName, descriptor.getLinesVisibleDefault(), descriptor.getLinesVisibleKeys(), descriptor.getLinesVisibleValues()));
            seriesInstance.setShapesVisible(getOrDefault(seriesName, descriptor.getPointsVisibleDefault(), descriptor.getPointsVisibleKeys(), descriptor.getPointsVisibleValues()));
            Boolean gradientVisible = getOrDefault(seriesName, descriptor.getGradientVisibleDefault(), descriptor.getGradientVisibleKeys(), descriptor.getGradientVisibleValues());
            if (gradientVisible != null) {
                seriesInstance.setGradientVisible(gradientVisible);
            }

            seriesInstance.setYToolTipPattern(getOrDefault(seriesName, descriptor.getYToolTipPatternDefault(), descriptor.getYToolTipPatternKeys(), descriptor.getYToolTipPatternValues()));
            seriesInstance.setXToolTipPattern(getOrDefault(seriesName, descriptor.getXToolTipPatternDefault(), descriptor.getXToolTipPatternKeys(), descriptor.getXToolTipPatternValues()));

            seriesInstance.setShapeLabel(getOrDefault(seriesName, descriptor.getPointLabelDefault(), descriptor.getPointLabelKeys(), descriptor.getPointLabelValues()));
            seriesInstance.setShapeSize(getOrDefault(seriesName, descriptor.getPointSizeDefault(), descriptor.getPointSizeKeys(), descriptor.getPointSizeValues()));
            seriesInstance.setShape(getOrDefault(seriesName, descriptor.getPointShapeDefault(), descriptor.getPointShapeKeys(), descriptor.getPointShapeValues()));

            seriesInstance.setPointLabelFormat(getOrDefault(seriesName, descriptor.getPointLabelFormatDefault(), descriptor.getPointLabelFormatKeys(), descriptor.getPointLabelFormatValues()));

            int tableId = figure.registerTable(table);

            seriesInstance.setDataSources(
                    Arrays.stream(descriptor.getDataSources())
                            .map(multiSeriesSource -> {
                                SourceDescriptor sourceDescriptor = new SourceDescriptor();
                                sourceDescriptor.setColumnName(multiSeriesSource.getColumnName());
                                sourceDescriptor.setAxis(multiSeriesSource.getAxis());
                                sourceDescriptor.setTableId(tableId);
                                sourceDescriptor.setType(multiSeriesSource.getType());
                                return sourceDescriptor;
                            })
                            .toArray(SourceDescriptor[]::new)
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

    private <T> T getOrDefault(String name, T defaultValue, String[] seriesNames, T[] values) {
        for (int i = 0; i < seriesNames.length; i++) {
            if (name.equals(seriesNames[i])) {
                return values[i];
            }
        }

        return defaultValue;
    }

    @JsProperty
    public SeriesPlotStyle getPlotStyle() {
        return descriptor.getPlotStyle();
    }

    @JsProperty
    public String getName() {
        return descriptor.getName();
    }
}

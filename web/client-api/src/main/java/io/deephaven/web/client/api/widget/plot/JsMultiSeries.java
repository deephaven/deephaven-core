package io.deephaven.web.client.api.widget.plot;

import elemental2.dom.CustomEvent;
import elemental2.dom.CustomEventInit;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.figuredescriptor.*;
import io.deephaven.web.client.api.TableMap;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsProperty;

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
        descriptor.getDatasourcesList().asList().stream().mapToInt(MultiSeriesSourceDescriptor::getTablemapid).distinct()
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
            seriesInstance.setPlotstyle(getPlotStyle());

            seriesInstance.setLinecolor(getOrDefault(seriesName, descriptor.getLinecolor()));
            seriesInstance.setShapecolor(getOrDefault(seriesName, descriptor.getPointcolor()));
            seriesInstance.setLinesvisible(getOrDefault(seriesName, descriptor.getLinesvisible()));
            seriesInstance.setShapesvisible(getOrDefault(seriesName, descriptor.getPointsvisible()));
            Boolean gradientVisible = getOrDefault(seriesName, descriptor.getGradientvisible());
            if (gradientVisible != null) {
                seriesInstance.setGradientvisible(gradientVisible);
            }

            seriesInstance.setYtooltippattern(getOrDefault(seriesName, descriptor.getYtooltippattern()));
            seriesInstance.setXtooltippattern(getOrDefault(seriesName, descriptor.getXtooltippattern()));

            seriesInstance.setShapelabel(getOrDefault(seriesName, descriptor.getPointlabel()));
            seriesInstance.setShapesize(getOrDefault(seriesName, descriptor.getPointsize()));
            seriesInstance.setShape(getOrDefault(seriesName, descriptor.getPointshape()));

            seriesInstance.setPointlabelformat(getOrDefault(seriesName, descriptor.getPointlabelformat()));

            int tableId = figure.registerTable(table);

            seriesInstance.setDatasourcesList(
                    descriptor.getDatasourcesList()
                            .map((multiSeriesSource, p1, p2) -> {
                                SourceDescriptor sourceDescriptor = new SourceDescriptor();
                                sourceDescriptor.setColumnname(multiSeriesSource.getColumnname());
                                sourceDescriptor.setAxis(multiSeriesSource.getAxis());
                                sourceDescriptor.setTableid(tableId);
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
            return map.getDefaultbool();
        }
        return map.getValuesList().getAt(index);
    }
    private String getOrDefault(String name, StringMapWithDefault map) {
        int index = map.getKeysList().findIndex((p0, p1, p2) -> name.equals(p0));
        if (index == -1) {
            return map.getDefaultstring();
        }
        return map.getValuesList().getAt(index);
    }
    private double getOrDefault(String name, FloatMapWithDefault map) {
        int index = map.getKeysList().findIndex((p0, p1, p2) -> name.equals(p0));
        if (index == -1) {
            return map.getDefaultfloat();
        }
        return map.getValuesList().getAt(index);
    }

    @JsProperty
    public int getPlotStyle() {
        return descriptor.getPlotstyle();
    }

    @JsProperty
    public String getName() {
        return descriptor.getName();
    }
}

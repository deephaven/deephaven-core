package io.deephaven.web.client.api.widget.plot;

import elemental2.core.JsArray;
import io.deephaven.web.client.fu.JsData;
import jsinterop.annotations.JsConstructor;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

import java.util.HashMap;
import java.util.Map;

@JsType(name = "ChartDescriptor", namespace = "dh.plot")
public class JsChartDescriptor {
    public int colspan;
    public int rowspan;

    public JsArray<JsSeriesDescriptor> series = new JsArray<>();
    public JsArray<JsAxisDescriptor> axes = new JsArray<>();

    public String chartType;

    public String title;
    public String titleFont;
    public String titleColor;

    public boolean showLegend;
    public String legendFont;
    public String legendColor;

    public boolean is3d;

    @JsConstructor
    public JsChartDescriptor() {}

    @JsIgnore
    public JsChartDescriptor(JsPropertyMap<Object> source) {
        this();

        Map<Object, JsAxisDescriptor> axisMap = new HashMap<>();
        if (source.has("axes")) {
            JsArray<Object> axes = source.getAny("axes").cast();
            this.axes = Js.uncheckedCast(axes.map((axisSource, index, all) -> {
                if (axisSource instanceof JsAxisDescriptor) {
                    return (JsAxisDescriptor) axisSource;
                } else {
                    return new JsAxisDescriptor((JsPropertyMap<Object>) axisSource);
                }
            }));
            this.axes.forEach((axis, i, all) -> axisMap.put(axes.getAt(i), axis));
        } else {
            throw new IllegalArgumentException("'axes' property must be set");
        }

        if (source.has("series")) {
            JsArray<Object> series = source.getAny("series").cast();
            this.series = Js.uncheckedCast(series.map((seriesSource, index, all) -> {
                if (seriesSource instanceof JsSeriesDescriptor) {
                    return (JsSeriesDescriptor) seriesSource;
                } else {
                    return new JsSeriesDescriptor((JsPropertyMap<Object>) seriesSource, axisMap);
                }
            }));
        } else {
            throw new IllegalArgumentException("'series' property must be set");
        }

        colspan = JsData.getIntProperty(source, "colspan", 1);
        rowspan = JsData.getIntProperty(source, "rowspan", 1);
        chartType = JsData.getRequiredStringProperty(source, "chartType");
        title = JsData.getStringProperty(source, "title");
        titleFont = JsData.getStringProperty(source, "titleFont");
        titleColor = JsData.getStringProperty(source, "titleColor");
        showLegend = JsData.getBooleanProperty(source, "showLegend");
        legendFont = JsData.getStringProperty(source, "legendFont");
        legendColor = JsData.getStringProperty(source, "legendColor");
        is3d = JsData.getBooleanProperty(source, "is3d");
    }
}

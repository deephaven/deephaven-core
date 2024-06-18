//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.widget.plot;

import elemental2.core.JsArray;
import io.deephaven.web.client.fu.JsData;
import jsinterop.annotations.JsConstructor;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsNullable;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

import java.util.HashMap;
import java.util.Map;

@JsType(name = "ChartDescriptor", namespace = "dh.plot")
public class JsChartDescriptor {
    @JsNullable
    public int colspan;
    @JsNullable
    public int rowspan;

    public JsArray<JsSeriesDescriptor> series = new JsArray<>();
    public JsArray<JsAxisDescriptor> axes = new JsArray<>();

    // TODO (deephaven-core#3442) change to some kind of String+int union type
    public String chartType;

    @JsNullable
    public String title;
    @JsNullable
    public String titleFont;
    @JsNullable
    public String titleColor;

    @JsNullable
    public boolean showLegend;
    @JsNullable
    public String legendFont;
    @JsNullable
    public String legendColor;

    @JsNullable
    public boolean is3d;

    @JsConstructor
    public JsChartDescriptor() {}

    @JsIgnore
    public JsChartDescriptor(JsPropertyMap<Object> source) {
        this();

        Map<Object, JsAxisDescriptor> axisMap = new HashMap<>();
        if (source.has("axes")) {
            JsArray<Object> axes = source.getAsAny("axes").cast();
            this.axes = Js.uncheckedCast(axes.map((axisSource, index) -> {
                if (axisSource instanceof JsAxisDescriptor) {
                    return (JsAxisDescriptor) axisSource;
                } else {
                    return new JsAxisDescriptor((JsPropertyMap<Object>) axisSource);
                }
            }));
            this.axes.forEach((axis, i) -> axisMap.put(axes.getAt(i), axis));
        } else {
            throw new IllegalArgumentException("'axes' property must be set");
        }

        if (source.has("series")) {
            JsArray<Object> series = source.getAsAny("series").cast();
            this.series = Js.uncheckedCast(series.map((seriesSource, index) -> {
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

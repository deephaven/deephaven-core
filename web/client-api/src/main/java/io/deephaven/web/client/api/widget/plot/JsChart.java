package io.deephaven.web.client.api.widget.plot;

import elemental2.core.JsArray;
import elemental2.core.JsObject;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.figuredescriptor.ChartDescriptor;
import io.deephaven.web.client.api.HasEventHandling;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

@JsType(name = "Chart", namespace = "dh.plot")
public class JsChart extends HasEventHandling {
    public static final String EVENT_SERIES_ADDED = "seriesadded";

    private final ChartDescriptor descriptor;
    private final JsSeries[] series;
    private final JsMultiSeries[] multiSeries;
    private final JsAxis[] axes;

    @JsIgnore
    public JsChart(ChartDescriptor descriptor, JsFigure jsFigure) {
        this.descriptor = descriptor;
        //build axes first, key them in a map for easy reuse when constructing series instances
        axes = Arrays.stream(descriptor.getAxes()).map(axis -> new JsAxis(axis, jsFigure)).toArray(JsAxis[]::new);
        JsObject.freeze(axes);
        Map<String, JsAxis> indexed = new HashMap<>();
        for (int i = 0; i < axes.length; i++) {
            indexed.put(axes[i].getId(), axes[i]);
        }
        series = Arrays.stream(descriptor.getSeries()).map(series -> new JsSeries(series, jsFigure, indexed)).toArray(JsSeries[]::new);
        multiSeries = Arrays.stream(descriptor.getMultiSeries()).map(multiSeries -> new JsMultiSeries(multiSeries, jsFigure, indexed, this)).toArray(JsMultiSeries[]::new);
        JsObject.freeze(multiSeries);
    }

    @JsProperty
    public int getColspan() {
        return descriptor.getColspan();
    }

    @JsProperty
    public int getRowspan() {
        return descriptor.getRowspan();
    }

    @JsProperty
    @SuppressWarnings("unusable-by-js")
    public ChartDescriptor.ChartType getChartType() {
        return descriptor.getChartType();
    }

    @JsProperty
    public String getTitle() {
        return descriptor.getTitle();
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
    public boolean isShowLegend() {
        return descriptor.isShowLegend();
    }

    @JsProperty
    public String getLegendFont() {
        return descriptor.getLegendFont();
    }

    @JsProperty
    public String getLegendColor() {
        return descriptor.getLegendColor();
    }

    @JsProperty(name = "is3d")
    public boolean isIs3d() {
        return descriptor.isIs3d();
    }

    //exposed for JS, do not use this from java methods
    @JsProperty(name = "series")
    public JsSeries[] getExportedSeriesArray() {
        return Js.uncheckedCast(Js.<JsArray<JsSeries>>uncheckedCast(series).slice());
    }
    //exposed for JS, do not use this from java methods
    @JsProperty(name = "multiSeries")
    public JsMultiSeries[] getExportedMultiSeriesArray() {
        return multiSeries;
    }

    @JsIgnore
    public JsSeries[] getSeries() {
        return series;
    }

    @JsIgnore
    public JsMultiSeries[] getMultiSeries() {
        return multiSeries;
    }

    @JsProperty
    public JsAxis[] getAxes() {
        return axes;
    }

    @JsIgnore
    public void addSeriesFromMultiSeries(JsSeries series) {
        this.series[this.series.length] = series;
    }
}

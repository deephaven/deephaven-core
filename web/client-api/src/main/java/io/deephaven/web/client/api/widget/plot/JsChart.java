package io.deephaven.web.client.api.widget.plot;

import elemental2.core.JsArray;
import elemental2.core.JsObject;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.figuredescriptor.ChartDescriptor;
import io.deephaven.web.client.api.HasEventHandling;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;

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
        axes = descriptor.getAxesList().map((p0, p1, p2) -> new JsAxis(p0, jsFigure));
        JsObject.freeze(axes);
        Map<String, JsAxis> indexed = new HashMap<>();
        for (int i = 0; i < axes.length; i++) {
            indexed.put(axes[i].getId(), axes[i]);
        }
        series = descriptor.getSeriesList().map((p0, p1, p2) -> new JsSeries(p0, jsFigure, indexed));
        multiSeries = descriptor.getMultiseriesList().map((p0, p1, p2) -> new JsMultiSeries(p0, jsFigure, indexed, this));
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
    public int getChartType() {
        return descriptor.getCharttype();
    }

    @JsProperty
    public String getTitle() {
        return descriptor.getTitle();
    }

    @JsProperty
    public String getTitleFont() {
        return descriptor.getTitlefont();
    }

    @JsProperty
    public String getTitleColor() {
        return descriptor.getTitlecolor();
    }

    @JsProperty
    public boolean isShowLegend() {
        return descriptor.getShowlegend();
    }

    @JsProperty
    public String getLegendFont() {
        return descriptor.getLegendfont();
    }

    @JsProperty
    public String getLegendColor() {
        return descriptor.getLegendcolor();
    }

    @JsProperty(name = "is3d")
    public boolean isIs3d() {
        return descriptor.getIs3d();
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

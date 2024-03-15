//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.widget.plot;

import com.vertispan.tsdefs.annotations.TsTypeRef;
import elemental2.core.JsArray;
import elemental2.core.JsObject;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.figuredescriptor.ChartDescriptor;
import io.deephaven.web.client.api.HasEventHandling;
import io.deephaven.web.client.api.widget.plot.enums.JsChartType;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsNullable;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;

import java.util.HashMap;
import java.util.Map;

/**
 * Provide the details for a chart.
 */
@JsType(name = "Chart", namespace = "dh.plot")
public class JsChart extends HasEventHandling {
    /**
     * a new series was added to this chart as part of a multi-series. The series instance is the detail for this event.
     */
    public static final String EVENT_SERIES_ADDED = "seriesadded";

    private final ChartDescriptor descriptor;
    private final JsSeries[] series;
    private final JsMultiSeries[] multiSeries;
    private final JsAxis[] axes;

    @JsIgnore
    public JsChart(ChartDescriptor descriptor, JsFigure jsFigure) {
        this.descriptor = descriptor;
        // build axes first, key them in a map for easy reuse when constructing series instances
        axes = descriptor.getAxesList().asList().stream().map((axisDescriptor) -> new JsAxis(axisDescriptor, jsFigure))
                .toArray(JsAxis[]::new);
        JsObject.freeze(axes);
        Map<String, JsAxis> indexed = new HashMap<>();
        for (int i = 0; i < axes.length; i++) {
            indexed.put(axes[i].getId(), axes[i]);
        }
        series = descriptor.getSeriesList().asList().stream()
                .map((seriesDescriptor) -> new JsSeries(seriesDescriptor, jsFigure, indexed)).toArray(JsSeries[]::new);
        multiSeries = descriptor.getMultiSeriesList().asList().stream()
                .map((multiSeriesDescriptor) -> new JsMultiSeries(multiSeriesDescriptor, jsFigure, indexed, this))
                .toArray(JsMultiSeries[]::new);
        JsObject.freeze(multiSeries);
    }

    @JsProperty
    public int getColumn() {
        return descriptor.getColumn();
    }

    @JsProperty
    public int getRow() {
        return descriptor.getRow();
    }

    @JsProperty
    public int getColspan() {
        return descriptor.getColspan();
    }

    @JsProperty
    public int getRowspan() {
        return descriptor.getRowspan();
    }

    /**
     * The type of this chart, see <b>ChartType</b> enum for more details.
     * 
     * @return int
     */
    @JsProperty
    @TsTypeRef(JsChartType.class)
    public int getChartType() {
        return descriptor.getChartType();
    }

    /**
     * The title of the chart.
     * 
     * @return String
     */
    @JsProperty
    @JsNullable
    public String getTitle() {
        if (descriptor.hasTitle()) {
            return descriptor.getTitle();
        }
        return null;
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
        return descriptor.getShowLegend();
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
        return descriptor.getIs3d();
    }

    // exposed for JS, do not use this from java methods
    @JsProperty(name = "series")
    public JsSeries[] getExportedSeriesArray() {
        return Js.uncheckedCast(Js.<JsArray<JsSeries>>uncheckedCast(series).slice());
    }

    // exposed for JS, do not use this from java methods
    @JsProperty(name = "multiSeries")
    public JsMultiSeries[] getExportedMultiSeriesArray() {
        return multiSeries;
    }

    /**
     * The series data for display in this chart.
     * 
     * @return dh.plot.Series
     */
    @JsIgnore
    public JsSeries[] getSeries() {
        return series;
    }

    /**
     * The multi-series data for display in this chart
     * 
     * @return dh.plot.MultiSeries
     */
    @JsIgnore
    public JsMultiSeries[] getMultiSeries() {
        return multiSeries;
    }

    /**
     * The axes used in this chart.
     * 
     * @return dh.plot.Axis
     */
    @JsProperty
    public JsAxis[] getAxes() {
        return axes;
    }

    @JsIgnore
    public void addSeriesFromMultiSeries(JsSeries series) {
        this.series[this.series.length] = series;
    }
}

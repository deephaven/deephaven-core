//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.widget.plot;

import com.vertispan.tsdefs.annotations.TsTypeRef;
import elemental2.core.JsArray;
import elemental2.core.JsObject;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.console_pb.figuredescriptor.ChartDescriptor;
import io.deephaven.web.client.api.event.HasEventHandling;
import io.deephaven.web.client.api.widget.plot.enums.JsChartType;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsNullable;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;

import java.util.HashMap;
import java.util.Map;

/**
 * Provides the details for a chart.
 */
@JsType(name = "Chart", namespace = "dh.plot")
public class JsChart extends HasEventHandling {
    /**
     * Fired when a new series is added to this chart as part of a multi-series chart. 
     * The event detail is the added series instance.
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

    /**
     * The column position of this chart in the figure layout.
     *
     * @return int
     */
    @JsProperty
    public int getColumn() {
        return descriptor.getColumn();
    }

    /**
     * The row position of this chart in the figure layout.
     *
     * @return int
     */
    @JsProperty
    public int getRow() {
        return descriptor.getRow();
    }

    /**
     * The number of columns this chart spans in the figure layout.
     *
     * @return int
     */
    @JsProperty
    public int getColspan() {
        return descriptor.getColspan();
    }

    /**
     * The number of rows this chart spans in the figure layout.
     *
     * @return int
     */
    @JsProperty
    public int getRowspan() {
        return descriptor.getRowspan();
    }

    /**
     * The type of this chart. See {@link JsChartType} for more details.
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

    /**
     * The font used to render the chart title.
     *
     * @return String
     */
    @JsProperty
    public String getTitleFont() {
        return descriptor.getTitleFont();
    }

    /**
     * The color used to render the chart title.
     *
     * @return String
     */
    @JsProperty
    public String getTitleColor() {
        return descriptor.getTitleColor();
    }

    /**
     * Whether the chart legend is shown.
     *
     * @return boolean
     */
    @JsProperty
    public boolean isShowLegend() {
        return descriptor.getShowLegend();
    }

    /**
     * The font used to render the chart legend.
     *
     * @return String
     */
    @JsProperty
    public String getLegendFont() {
        return descriptor.getLegendFont();
    }

    /**
     * The color used to render the chart legend.
     *
     * @return String
     */
    @JsProperty
    public String getLegendColor() {
        return descriptor.getLegendColor();
    }

    /**
     * Whether this chart is rendered in 3D.
     *
     * @return boolean
     */
    @JsProperty(name = "is3d")
    public boolean isIs3d() {
        return descriptor.getIs3d();
    }

    /**
     * Gets a copy of the chart series array.
     *
     * <p>
     * Exposed for JS; do not use this from Java methods.
     *
     * @return {@link JsSeries dh.plot.Series}[]
     */
    // exposed for JS, do not use this from java methods
    @JsProperty(name = "series")
    public JsSeries[] getExportedSeriesArray() {
        return Js.uncheckedCast(Js.<JsArray<JsSeries>>uncheckedCast(series).slice());
    }

    /**
     * Gets the chart multi-series array.
     *
     * <p>
     * Exposed for JS; do not use this from Java methods.
     *
     * @return {@link JsMultiSeries dh.plot.MultiSeries}[]
     */
    // exposed for JS, do not use this from java methods
    @JsProperty(name = "multiSeries")
    public JsMultiSeries[] getExportedMultiSeriesArray() {
        return multiSeries;
    }

    /**
     * The series data for display in this chart.
     * 
     * @return {@link JsSeries dh.plot.Series}[]
     */
    @JsIgnore
    public JsSeries[] getSeries() {
        return series;
    }

    /**
     * The multi-series data for display in this chart.
     * 
     * @return {@link JsMultiSeries dh.plot.MultiSeries}
     */
    @JsIgnore
    public JsMultiSeries[] getMultiSeries() {
        return multiSeries;
    }

    /**
     * The axes used in this chart.
     * 
     * @return {@link JsAxis dh.plot.Axis}[]
     */
    @JsProperty
    public JsAxis[] getAxes() {
        return axes;
    }

    /**
     * Adds a series generated from a multi-series into the series array.
     *
     * @param series The series instance to add.
     */
    @JsIgnore
    public void addSeriesFromMultiSeries(JsSeries series) {
        this.series[this.series.length] = series;
    }
}

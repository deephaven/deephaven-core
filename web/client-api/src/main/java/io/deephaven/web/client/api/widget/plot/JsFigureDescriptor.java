package io.deephaven.web.client.api.widget.plot;

import elemental2.core.JsArray;
import io.deephaven.web.client.api.JsTable;
import io.deephaven.web.client.fu.JsData;
import jsinterop.annotations.JsConstructor;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

import java.util.HashSet;
import java.util.Set;

/**
 * A descriptor used with JsFigureFactory.create to create a figure from JS.
 */
@JsType(name = "FigureDescriptor", namespace = "dh.plot")
public class JsFigureDescriptor {
    public String title;
    public String titleFont;
    public String titleColor;
    public boolean isResizable;
    public boolean isDefaultTheme;

    public double updateInterval;

    public int cols;
    public int rows;

    public JsArray<JsChartDescriptor> charts;

    @JsConstructor
    public JsFigureDescriptor() {}

    @JsIgnore
    public JsFigureDescriptor(JsPropertyMap<Object> source) {
        this();

        JsArray<Object> charts = JsData.getRequiredProperty(source, "charts").cast();
        this.charts = Js.uncheckedCast(charts.map((chartSource, index, all) -> {
            if (chartSource instanceof JsChartDescriptor) {
                return (JsChartDescriptor) chartSource;
            } else {
                return new JsChartDescriptor((JsPropertyMap<Object>) chartSource);
            }
        }));
        title = JsData.getStringProperty(source, "title");
        titleFont = JsData.getStringProperty(source, "titleFont");
        titleColor = JsData.getStringProperty(source, "titleColor");
        isResizable = JsData.getBooleanProperty(source, "isResizable");
        isDefaultTheme = JsData.getBooleanProperty(source, "isDefaultTheme");
        updateInterval = JsData.getDoubleProperty(source, "updateInterval");
        cols = JsData.getIntProperty(source, "cols", 1);
        rows = JsData.getIntProperty(source, "rows", 1);
    }

    @JsIgnore
    public JsArray<JsTable> getTables() {
        Set<JsTable> tableSet = new HashSet<>();
        charts.forEach((chart, i1, a1) -> {
            chart.series.forEach((series, i2, a2) -> {
                series.dataSources.forEach((source, i3, a3) -> {
                    tableSet.add(source.table);
                    return null;
                });
                return null;
            });
            return null;
        });

        return Js.cast(JsArray.from(tableSet.toArray(new JsTable[0])));
    }
}

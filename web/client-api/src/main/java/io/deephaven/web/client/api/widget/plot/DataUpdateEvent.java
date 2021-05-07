package io.deephaven.web.client.api.widget.plot;

import elemental2.core.JsArray;
import io.deephaven.web.client.api.TableData;
import io.deephaven.web.shared.data.plot.SourceDescriptor;
import io.deephaven.web.shared.data.plot.SourceType;
import io.deephaven.web.shared.fu.JsFunction;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsOptional;
import jsinterop.annotations.JsProperty;
import jsinterop.base.Any;

import java.util.Arrays;

public class DataUpdateEvent {

    public static final DataUpdateEvent empty(JsSeries... series) {
        return new DataUpdateEvent(series, null, null) {
            @Override
            public JsArray<Any> getArray(JsSeries series, SourceType sourceType, @JsOptional JsFunction<Any, Any> mappingFunc) {
                return new JsArray<>();
            }
        };
    }

    private final JsSeries[] series;
    private final ChartData data;
    private final TableData currentUpdate;

    public DataUpdateEvent(JsSeries[] relatedSeries, ChartData data, TableData currentUpdate) {
        this.series = relatedSeries;
        this.data = data;
        this.currentUpdate = currentUpdate;
    }

    @JsProperty
    public JsSeries[] getSeries() {
        return series;
    }

    public JsArray<Any> getArray(JsSeries series, SourceType sourceName) {
        return getArray(series, sourceName, null);
    }

    @JsMethod
    public JsArray<Any> getArray(JsSeries series, SourceType sourceType, @JsOptional JsFunction<Any, Any> mappingFunc) {
        String columnName = getColumnName(series, sourceType);

        return data.getColumn(columnName, mappingFunc, currentUpdate);
    }

    private String getColumnName(JsSeries series, SourceType sourceType) {
        return Arrays.stream(series.getDescriptor().getDataSources())
                .filter(sd -> sd.getType().equals(sourceType))
                .findFirst().map(SourceDescriptor::getColumnName)
                .orElseThrow(() -> new IllegalArgumentException("No sourceType " + sourceType + " in provided series"));
    }
}

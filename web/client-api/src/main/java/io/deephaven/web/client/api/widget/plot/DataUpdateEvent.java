//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.widget.plot;

import com.vertispan.tsdefs.annotations.TsInterface;
import com.vertispan.tsdefs.annotations.TsName;
import elemental2.core.JsArray;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.console_pb.figuredescriptor.SourceDescriptor;
import io.deephaven.web.client.api.TableData;
import io.deephaven.web.shared.fu.JsFunction;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsOptional;
import jsinterop.annotations.JsProperty;
import jsinterop.base.Any;

/**
 * Event detail for a figure data update.
 *
 * <p>
 * Provides access to the updated data for one or more related series.
 */
@TsInterface
@TsName(name = "FigureDataUpdatedEvent", namespace = "dh.plot")
public class DataUpdateEvent {

    public static final DataUpdateEvent empty(JsSeries... series) {
        return new DataUpdateEvent(series, null, null) {
            @Override
            public JsArray<Any> getArray(JsSeries series, int sourceType,
                    @JsOptional JsFunction<Any, Any> mappingFunc) {
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

    /**
     * The series related to this update.
     */
    @JsProperty
    public JsSeries[] getSeries() {
        return series;
    }

    /**
     * Gets a contiguous JS array of values for the given series and data source type.
     */
    public JsArray<Any> getArray(JsSeries series, int sourceName) {
        return getArray(series, sourceName, null);
    }

    /**
     * Gets a contiguous JS array of values for the given series and data source type.
     *
     * <p>
     * If provided, {@code mappingFunc} is applied to each value. To re-use cached data across calls, use the same
     * {@code mappingFunc} instance each time.
     *
     * @param series the series to read data for
     * @param sourceType the data source type, as defined by the series descriptor
     * @param mappingFunc an optional mapping function applied to each value; {@code null} returns the raw values
     * @return a contiguous JS array of values
     */
    @JsMethod
    public JsArray<Any> getArray(JsSeries series, int sourceType, @JsOptional JsFunction<Any, Any> mappingFunc) {
        String columnName = getColumnName(series, sourceType);

        return data.getColumn(columnName, mappingFunc, currentUpdate);
    }

    private String getColumnName(JsSeries series, int sourceType) {
        return series.getDescriptor().getDataSourcesList().asList().stream()
                .filter(sd -> sd.getType() == sourceType)
                .findFirst().map(SourceDescriptor::getColumnName)
                .orElseThrow(() -> new IllegalArgumentException("No sourceType " + sourceType + " in provided series"));
    }
}

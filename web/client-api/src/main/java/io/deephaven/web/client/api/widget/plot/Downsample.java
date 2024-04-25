//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.widget.plot;

import elemental2.promise.Promise;
import io.deephaven.web.client.api.*;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsNullable;
import jsinterop.annotations.JsOptional;
import jsinterop.annotations.JsType;

/**
 * Helper class for plot downsampling methods.
 */
@JsType(namespace = "dh.plot")
public class Downsample {
    /**
     * Downsamples a table so that the data can be used for a time-series line plot. The downsampled table should have
     * the same visual fidelity as the original table, but with fewer rows.
     *
     * @param table The table to downsample.
     * @param xCol The name of the X column to downsample. Must be an Instant or long.
     * @param yCols The names of the Y columns to downsample.
     * @param width The width of the visible area in pixels.
     * @param xRange The visible range as `[start, end]` or null to always use all data.
     * 
     * @return A promise that resolves to the downsampled table.
     */
    public static Promise<JsTable> runChartDownsample(JsTable table, String xCol, String[] yCols, int width,
            @JsOptional @JsNullable LongWrapper[] xRange) {
        return table.downsample(xRange, width, xCol, yCols);
    }

    @JsIgnore
    private Downsample() {}
}

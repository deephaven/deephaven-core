//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.updateby;

import com.vertispan.tsdefs.annotations.TsLiteral;
import jsinterop.annotations.JsNullable;
import jsinterop.annotations.JsType;

/**
 * Computes the exponential moving standard deviation of each input column, within each group:
 *
 * <pre>
 * variance = a * (variancePrev + (1 - a) * (value - emaPrev)^2)
 * emStd = sqrt(variance)
 * </pre>
 *
 * where {@code emaPrev} is the previous row's exponential moving average, as {@link UpdateByEma} computes it. The
 * output for the first value in each group is NaN, or null for BigDecimal and BigInteger input. The variance starts at
 * zero from that row. {@link #windowScale} sets {@code a}.
 */
@JsType(namespace = "dh.updateby")
public class UpdateByEmStd {
    @TsLiteral
    public final String type = "UpdateByEmStd";
    /**
     * How to handle null and NaN values, and other options. If null, the server's defaults apply.
     */
    @JsNullable
    public UpdateByEmOptions emOptions;
    /**
     * The decay rate. In rows, {@code a = e^(-1 / ticks)}. In time, {@code a = e^(-dt / duration)}, where {@code dt} is
     * the time since the previous row. A larger value decays more slowly.
     */
    public UpdateByWindowScale windowScale;
}

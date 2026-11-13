//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.updateby;

import com.vertispan.tsdefs.annotations.TsLiteral;
import jsinterop.annotations.JsNullable;
import jsinterop.annotations.JsType;

/**
 * Computes the exponential moving maximum of each input column, within each group:
 *
 * <pre>
 * emMax = max(a * emMaxPrev, value)
 * </pre>
 *
 * The first value in each group is the initial maximum. {@link #windowScale} sets {@code a}.
 */
@JsType(namespace = "dh.updateby")
public class UpdateByEmMax {
    @TsLiteral
    public final String type = "UpdateByEmMax";
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

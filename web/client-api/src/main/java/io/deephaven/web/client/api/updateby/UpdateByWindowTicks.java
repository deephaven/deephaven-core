//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.updateby;

import com.vertispan.tsdefs.annotations.TsLiteral;
import jsinterop.annotations.JsType;

/**
 * A window size or decay rate measured in rows.
 */
@JsType(namespace = "dh.updateby")
public class UpdateByWindowTicks {
    @TsLiteral
    public final String type = "ticks";
    /**
     * The number of rows. Rolling operations drop any fractional part. The exponential moving operations use it as
     * given.
     */
    public double ticks;
}

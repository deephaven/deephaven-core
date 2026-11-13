//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.updateby;

import com.vertispan.tsdefs.annotations.TsLiteral;
import jsinterop.annotations.JsType;

/**
 * Computes the maximum of each input column over the current row and every row before it in its group. Skips null
 * values.
 */
@JsType(namespace = "dh.updateby")
public class UpdateByCumulativeMax {
    @TsLiteral
    public final String type = "UpdateByCumulativeMax";
}

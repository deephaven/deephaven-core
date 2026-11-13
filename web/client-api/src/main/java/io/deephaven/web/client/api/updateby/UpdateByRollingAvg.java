//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.updateby;

import com.vertispan.tsdefs.annotations.TsLiteral;
import jsinterop.annotations.JsType;

/**
 * Computes the average of each input column over each row's window. Skips null values.
 */
@JsType(namespace = "dh.updateby")
public final class UpdateByRollingAvg extends UpdateByRollingSpec {
    @TsLiteral
    public final String type = "UpdateByRollingAvg";
}

//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.updateby;

import com.vertispan.tsdefs.annotations.TsLiteral;
import io.deephaven.web.client.api.Column;
import jsinterop.annotations.JsType;

/**
 * Computes the weighted average of each input column over each row's window: the sum of each value times its weight,
 * divided by the sum of the weights. Skips rows where the value or the weight is null.
 */
@JsType(namespace = "dh.updateby")
public final class UpdateByRollingWAvg extends UpdateByRollingSpec {
    @TsLiteral
    public final String type = "UpdateByRollingWAvg";
    /** The column that holds each row's weight. */
    public Column.ColumnOrName weightColumn;
}

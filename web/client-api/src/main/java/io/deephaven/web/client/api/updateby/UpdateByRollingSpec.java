//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.updateby;

import jsinterop.annotations.JsNullable;
import jsinterop.annotations.JsType;

/**
 * Base type for the rolling operations, which compute each row's output over a window around that row: the reverse
 * window ends with the row itself, and the forward window covers the rows after it.
 */
@JsType(namespace = "dh.updateby")
public sealed class UpdateByRollingSpec
        permits UpdateByRollingAvg, UpdateByRollingCount, UpdateByRollingCountWhere, UpdateByRollingFormula,
        UpdateByRollingGroup, UpdateByRollingMax, UpdateByRollingMin, UpdateByRollingProduct, UpdateByRollingStd,
        UpdateByRollingSum, UpdateByRollingWAvg {

    /**
     * The scale of the forward window, after the current row. If null, zero ticks are used, so the window ends at the
     * current row.
     */
    @JsNullable
    public UpdateByWindowScale forwardWindowScale;

    /**
     * The scale of the reverse window, which includes the current row: one tick is the current row alone.
     */
    public UpdateByWindowScale reverseWindowScale;
}

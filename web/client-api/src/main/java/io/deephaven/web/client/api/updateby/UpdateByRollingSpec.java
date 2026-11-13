//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.updateby;

import jsinterop.annotations.JsNullable;
import jsinterop.annotations.JsType;

/**
 * Base type for the rolling operations, which compute each row's output over a window of rows around it, within its
 * group. A reverse and a forward scale define the window. Both scales are measured in rows, as
 * {@link UpdateByWindowTicks}, or both in time, as {@link UpdateByWindowTime}.
 * <p>
 * In rows, the reverse window ends with the current row, and the forward window covers the rows after it. A negative
 * forward value ends the window before the current row, and a negative reverse value starts it after the current row:
 * <ul>
 * <li>reverse 1, forward 0 — the current row only</li>
 * <li>reverse 10, forward 0 — from 9 rows before through the current row</li>
 * <li>reverse 0, forward 10 — from 1 row after through 10 rows after</li>
 * <li>reverse 10, forward 10 — from 9 rows before through 10 rows after</li>
 * <li>reverse 10, forward -5 — from 9 rows before through 5 rows before</li>
 * <li>reverse -5, forward 10 — from 6 rows after through 10 rows after</li>
 * </ul>
 * <p>
 * In time, the window holds the rows whose timestamp is from the reverse duration before the current row's timestamp
 * through the forward duration after it, inclusive. A negative forward duration ends the window before the current
 * row's timestamp, and a negative reverse duration starts it after. A row with a null timestamp is in no window, and
 * its output is null.
 * <p>
 * The window may not have a negative total size.
 */
@JsType(namespace = "dh.updateby")
public sealed class UpdateByRollingSpec
        permits UpdateByRollingAvg, UpdateByRollingCount, UpdateByRollingCountWhere, UpdateByRollingFormula,
        UpdateByRollingGroup, UpdateByRollingMax, UpdateByRollingMin, UpdateByRollingProduct, UpdateByRollingStd,
        UpdateByRollingSum, UpdateByRollingWAvg {

    /**
     * The scale of the forward window, after the current row. Must use the same kind of scale as
     * {@link #reverseWindowScale}. A time scale must name the same column. If null, the window ends at the current row
     * or its timestamp.
     */
    @JsNullable
    public UpdateByWindowScale forwardWindowScale;

    /**
     * The scale of the reverse window, which ends with the current row.
     */
    public UpdateByWindowScale reverseWindowScale;
}

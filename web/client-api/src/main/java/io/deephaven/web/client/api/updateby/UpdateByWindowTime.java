//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.updateby;

import com.vertispan.tsdefs.annotations.TsLiteral;
import io.deephaven.web.client.api.Column;
import jsinterop.annotations.JsType;

/**
 * A window size or decay rate measured in time, using the timestamps in a column of the table.
 */
@JsType(namespace = "dh.updateby")
public class UpdateByWindowTime {
    @TsLiteral
    public final String type = "time";
    /**
     * The timestamp column, which holds date-times or long epoch nanoseconds. All time-based operations in one
     * {@link io.deephaven.web.client.api.JsTableOperations#updateBy(UpdateByOptions)} call must use the same column.
     */
    public Column.ColumnOrName column;
    /**
     * The length of time: a number of nanoseconds, a {@link io.deephaven.web.client.api.LongWrapper} of nanoseconds, or
     * a duration string such as {@code "PT5M"} or {@code "PT00:10:00"}.
     */
    public WindowDurationUnion duration;
}

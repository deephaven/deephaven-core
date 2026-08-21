//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.agg;

import com.vertispan.tsdefs.annotations.TsInterface;
import jsinterop.annotations.JsNullable;
import jsinterop.annotations.JsType;

/**
 * Partitions the table into sub-tables, one per group. Not supported in aggAllBy.
 */
@JsType
@TsInterface
public final class Partition extends Aggregation {
    public final String type = "Partition";
    /** The output column name to hold the sub-tables. */
    public String col;
    /** Whether to include the group-by columns in the sub-tables. Defaults to true if omitted. */
    @JsNullable
    public Boolean includeGroupByColumns;
}


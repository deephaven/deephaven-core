//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.agg;

import jsinterop.annotations.JsNullable;
import jsinterop.annotations.JsType;

/**
 * Partitions the source table into sub-tables, one per aggregation group. Each sub-table is stored in a new column with
 * the specified name.
 *
 * <p>
 * This aggregation is not supported in {@code aggAllBy} — use it only with {@code aggBy}.
 */
@JsType
public final class Partition extends Aggregation {
    public final String type = "Partition";

    /** The output column name to hold the sub-table for each group. */
    public String col;

    /**
     * Whether to include the group-by columns in each output sub-table. Defaults to {@code true} if not specified.
     */
    @JsNullable
    public Boolean includeGroupByColumns;
}


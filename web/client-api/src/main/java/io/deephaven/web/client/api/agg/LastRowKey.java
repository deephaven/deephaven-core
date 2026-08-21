//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.agg;

/**
 * Returns the row key of the last row in each group. Not supported in aggAllBy.
 */
public final class LastRowKey extends Aggregation {
    public final String type = "LastRowKey";
    /** The output column name to hold the last row key. */
    public String col;
}


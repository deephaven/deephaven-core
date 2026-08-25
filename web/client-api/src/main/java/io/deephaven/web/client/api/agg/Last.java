//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.agg;

import jsinterop.annotations.JsType;

/**
 * Returns the last value within each aggregation group.
 */
@JsType
public final class Last extends ColumnAggregation {
    public final String type = "Last";
}

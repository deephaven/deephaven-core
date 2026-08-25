//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.agg;

import jsinterop.annotations.JsNullable;
import jsinterop.annotations.JsType;

/**
 * Computes the single unique value within each aggregation group. If all values in a group are null, or if there is
 * more than one distinct value, the result is null (or a non-unique sentinel value, if supported).
 */
@JsType
public final class Unique extends ColumnAggregation {
    public final String type = "Unique";

    /**
     * Whether {@code null} is treated as a value for the purpose of determining if the values in the aggregation group
     * are unique. When {@code true}, a group containing both null and a single non-null value is considered non-unique.
     * Defaults to {@code false}.
     */
    @JsNullable
    public Boolean includeNulls;
    // TODO non-unique sentinel values
}

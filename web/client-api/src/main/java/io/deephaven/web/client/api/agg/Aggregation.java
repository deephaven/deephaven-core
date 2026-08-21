//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.agg;

/**
 * Base sealed type for all aggregation definitions.
 *
 * <p>
 * Column-based aggregations (those backed by an {@code AggSpec} and optional input/output column pairs) extend
 * {@link ColumnAggregation}. Non-column aggregations that produce a single output column or have special semantics
 * ({@link Count}, {@link CountWhere}, {@link Partition}, {@link FirstRowKey}, {@link LastRowKey}) extend this class
 * directly.
 *
 * <p>
 * Each concrete subtype carries a {@code type} field whose compile-time constant value acts as a discriminant, enabling
 * TypeScript consumers to narrow the union via {@code switch (agg.type)}.
 *
 * @see ColumnAggregation
 * @see AggregationUnion
 */
public sealed class Aggregation
        permits ColumnAggregation, Count, CountWhere, Partition, FirstRowKey, LastRowKey {

}

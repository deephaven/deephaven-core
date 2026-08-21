//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.agg;

/**
 * Base type for all aggregation definitions used by {@code aggBy} and {@code aggAllBy}.
 *
 * <p>
 * Column-based aggregations (those using an {@link io.deephaven.proto.backplane.grpc.AggSpec AggSpec}) extend
 * {@link ColumnAggregation}. Non-column aggregations ({@link Count}, {@link CountWhere}, {@link Partition},
 * {@link FirstRowKey}, {@link LastRowKey}) extend this class directly.
 */
public sealed class Aggregation
        permits ColumnAggregation, Count, CountWhere, Partition, FirstRowKey, LastRowKey {

}


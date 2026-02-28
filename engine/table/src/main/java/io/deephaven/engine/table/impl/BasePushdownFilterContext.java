//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.select.*;
import io.deephaven.util.SafeCloseable;

import java.util.List;

/**
 * Base class for {@link PushdownFilterContext} to help with execution cost tracking.
 */
public interface BasePushdownFilterContext extends PushdownFilterContext {

    /**
     * Enum for the behavior of a filter when applied to null values.
     */
    enum FilterNullBehavior {
        /**
         * The filter includes nulls in its results, like {@code x == null}.
         */
        INCLUDES_NULLS,

        /**
         * The filter does not include nulls in its results, like {@code x > 5}.
         */
        EXCLUDES_NULLS,

        /**
         * The filter throws an exception when applied to nulls, like {@code x.beginsWith("A")}.
         */
        FAILS_ON_NULLS
    }

    /**
     * Interface for a unified chunk filter that can be used to apply a filter to a chunk of data, whether the
     * underlying filter is a {@link ExposesChunkFilter} or a {@link ConditionFilter}.
     */
    interface UnifiedChunkFilter extends SafeCloseable {
        LongChunk<OrderedRowKeys> filter(Chunk<? extends Values> values, LongChunk<OrderedRowKeys> keys);
    }

    /**
     * Gets the filter associated with this context.
     */
    WhereFilter filter();

    /**
     * Get the column sources this filter will use.
     */
    List<ColumnSource<?>> columnSources();

    /**
     * Whether this filter supports direct chunk filtering, i.e., it can be applied to a chunk of data rather than a
     * table. This includes any filter that implements {@link ExposesChunkFilter} or {@link ConditionFilter} with
     * exactly one column.
     */
    boolean supportsChunkFiltering();

    /**
     * Whether this filter supports filtering based on parquet metadata.
     */
    boolean supportsMetadataFiltering();

    boolean supportsInMemoryDataIndexFiltering();

    boolean supportsDeferredDataIndexFiltering();

    /**
     * The filter to use for parquet metadata filtering. Can only call when {@link #supportsMetadataFiltering()} is
     * {@code true}.
     */
    WhereFilter filterForMetadataFiltering();

    /**
     * Get the behavior of this filter when applied to null values. This is lazily computed on first access.
     */
    FilterNullBehavior filterNullBehavior();

    /**
     * Create a {@link UnifiedChunkFilter} for the {@link WhereFilter} that efficiently filters chunks of data. Every
     * thread that uses this should create its own instance and must close it after use. Can only call when
     * {@link #supportsChunkFiltering()} is {@code true}
     *
     * @param maxChunkSize the maximum size of the chunk that will be filtered
     * @return the initialized {@link UnifiedChunkFilter}
     */
    UnifiedChunkFilter createChunkFilter(final int maxChunkSize);
}

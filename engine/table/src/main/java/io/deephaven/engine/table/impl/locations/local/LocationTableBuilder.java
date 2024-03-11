package io.deephaven.engine.table.impl.locations.local;

import io.deephaven.engine.table.Table;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;

/**
 * Interface for implementations to perform type coercion and specify a table of partition values for observed table
 * locations. Used by {@link KeyValuePartitionLayout}.
 */
public interface LocationTableBuilder {

    /**
     * Register an ordered collection of {@link String strings} representing partition keys. This should be called
     * exactly once, and before any calls to {@link #acceptLocation(Collection) acceptLocation}.
     *
     * @param partitionKeys The partition keys to register
     */
    void registerPartitionKeys(@NotNull Collection<String> partitionKeys);

    /**
     * Accept an ordered collection of {@link String strings} representing partition values for a particular table
     * location, parallel to a previously-registered collection of partition keys. Should be called after a single call
     * to {@link #registerPartitionKeys(Collection) registerPartitionKeys}.
     *
     * @param partitionValueStrings The partition values to accept. Must have the same length as the
     *        previously-registered partition keys.
     */
    void acceptLocation(@NotNull Collection<String> partitionValueStrings);

    /**
     * Build a {@link Table} with one column per partition key specified in {@link #registerPartitionKeys(Collection)
     * registerPartitionKeys}, and one row per location provided via {@link #acceptLocation(Collection) acceptLocation},
     * with cell values parallel to that location's partition values after any appropriate conversion has been applied.
     * The implementation is responsible for determining the appropriate column types.
     *
     * @return The {@link Table}
     */
    Table build();
}

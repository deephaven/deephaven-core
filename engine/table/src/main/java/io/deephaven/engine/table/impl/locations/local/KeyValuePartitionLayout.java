//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.locations.local;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.locations.TableLocationKey;
import io.deephaven.engine.table.impl.locations.impl.TableLocationKeyFinder;
import io.deephaven.engine.table.ColumnSource;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.net.URI;
import java.nio.file.Path;
import java.util.function.BiFunction;
import java.util.function.Consumer;

/**
 * Base class for {@link TableLocationKeyFinder location finders} that perform file system traversal to infer
 * partitions.
 *
 * @param <TLK> The type of {@link TableLocationKey} to be generated
 * @param <TARGET_FILE_TYPE> The type of files used to generate location keys, like a {@link URI} or a {@link Path}
 */
public abstract class KeyValuePartitionLayout<TLK extends TableLocationKey, TARGET_FILE_TYPE>
        implements TableLocationKeyFinder<TLK> {

    /**
     * Interface for implementations to perform type coercion and specify a table of partition values for observed table
     * locations.
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
         * location, parallel to a previously registered collection of partition keys. Should be called after a single
         * call to {@link #registerPartitionKeys(Collection) registerPartitionKeys}.
         *
         * @param partitionValueStrings The partition values to accept. Must have the same length as the previously
         *        registered partition keys.
         */
        void acceptLocation(@NotNull Collection<String> partitionValueStrings);

        /**
         * Build a {@link Table} with one column per partition key specified in
         * {@link #registerPartitionKeys(Collection) registerPartitionKeys}, and one row per location provided via
         * {@link #acceptLocation(Collection) acceptLocation}, with cell values parallel to that location's partition
         * values after any appropriate conversion has been applied. The implementation is responsible for determining
         * the appropriate column types.
         *
         * @return The {@link Table}
         */
        Table build();
    }

    private final BiFunction<TARGET_FILE_TYPE, Map<String, Comparable<?>>, TLK> keyFactory;

    /**
     * @param keyFactory Factory function used to generate table location keys from target files and partition values
     */
    public KeyValuePartitionLayout(
            @NotNull final BiFunction<TARGET_FILE_TYPE, Map<String, Comparable<?>>, TLK> keyFactory) {
        this.keyFactory = keyFactory;
    }

    public String toString() {
        return KeyValuePartitionLayout.class.getSimpleName();
    }

    /**
     * Build location keys from a location table and a collection of target files.
     *
     * @param locationTable The location table
     * @param targetFiles The target files
     * @param locationKeyObserver A consumer which will receive the location keys
     */
    final void buildLocationKeys(
            @NotNull final Table locationTable,
            @NotNull final Deque<TARGET_FILE_TYPE> targetFiles,
            @NotNull final Consumer<TLK> locationKeyObserver) {
        final Map<String, Comparable<?>> partitions = new LinkedHashMap<>();
        // Note that we allow the location table to define partition priority order.
        final String[] partitionKeys = locationTable.getDefinition().getColumnNamesArray();
        // noinspection unchecked
        final ColumnSource<? extends Comparable<?>>[] partitionValueSources =
                locationTable.getColumnSources().toArray(ColumnSource.ZERO_LENGTH_COLUMN_SOURCE_ARRAY);
        final int numColumns = partitionValueSources.length;
        locationTable.getRowSet().forAllRowKeys((final long rowKey) -> {
            for (int ci = 0; ci < numColumns; ++ci) {
                partitions.put(partitionKeys[ci], partitionValueSources[ci].get(rowKey));
            }
            locationKeyObserver.accept(keyFactory.apply(targetFiles.remove(), partitions));
        });
    }
}

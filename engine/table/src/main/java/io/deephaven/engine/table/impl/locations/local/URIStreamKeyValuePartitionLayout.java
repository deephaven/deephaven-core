//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.locations.local;

import io.deephaven.api.util.NameValidator;
import io.deephaven.base.verify.Require;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.engine.table.impl.locations.TableLocationKey;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.net.URI;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * Extracts a key-value partitioned table layout from a stream of URIs.
 */
public abstract class URIStreamKeyValuePartitionLayout<TLK extends TableLocationKey>
        extends KeyValuePartitionLayout<TLK, URI> {

    protected final URI tableRootDirectory;
    private final Supplier<KeyValuePartitionLayout.LocationTableBuilder> locationTableBuilderFactory;
    private final int maxPartitioningLevels;
    private final String fileSeparator;

    /**
     * @param tableRootDirectory The directory to traverse from
     * @param locationTableBuilderFactory Factory for {@link LocationTableBuilder builders} used to organize partition
     *        information; as builders are typically stateful, a new builder is created each time this
     *        {@link KeyValuePartitionLayout} is used to {@link #findKeys(Consumer) find keys}
     * @param keyFactory Factory function used to generate table location keys from target files and partition values
     * @param maxPartitioningLevels Maximum partitioning levels to traverse. Must be {@code >= 0}. {@code 0} means only
     *        look at files in {@code tableRootDirectory} and find no partitions.
     */
    public URIStreamKeyValuePartitionLayout(
            @NotNull final URI tableRootDirectory,
            @NotNull final Supplier<KeyValuePartitionLayout.LocationTableBuilder> locationTableBuilderFactory,
            @NotNull final BiFunction<URI, Map<String, Comparable<?>>, TLK> keyFactory,
            final int maxPartitioningLevels,
            @NotNull final String fileSeparator) {
        super(keyFactory);
        this.tableRootDirectory = tableRootDirectory;
        this.locationTableBuilderFactory = locationTableBuilderFactory;
        this.maxPartitioningLevels = Require.geqZero(maxPartitioningLevels, "maxPartitioningLevels");
        this.fileSeparator = fileSeparator;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + '[' + tableRootDirectory + ']';
    }

    /**
     * Find the keys in the given URI stream and notify the observer.
     */
    protected final void findKeys(@NotNull final Stream<URI> uriStream,
            @NotNull final Consumer<TLK> locationKeyObserver) {
        final KeyValuePartitionLayout.LocationTableBuilder locationTableBuilder = locationTableBuilderFactory.get();
        final Deque<URI> targetURIs = new ArrayDeque<>();
        final Set<String> takenNames = new HashSet<>();
        final List<String> partitionKeys = new ArrayList<>();
        final boolean[] registered = {false}; // Hack to make the variable final
        uriStream.forEachOrdered(uri -> {
            final Collection<String> partitionValues = new ArrayList<>();
            final URI relativePath = tableRootDirectory.relativize(uri);
            getPartitions(relativePath, partitionKeys, partitionValues, takenNames, registered[0]);
            if (!registered[0]) {
                // Use the first path to find the partition keys and then use the same partition keys for the rest
                locationTableBuilder.registerPartitionKeys(partitionKeys);
                registered[0] = true;
            }
            // Use the partition values from each path to build the location table
            locationTableBuilder.acceptLocation(partitionValues);
            targetURIs.add(uri);
        });
        final Table locationTable = locationTableBuilder.build();
        buildLocationKeys(locationTable, targetURIs, locationKeyObserver);
    }

    private void getPartitions(@NotNull final URI relativePath,
            @NotNull final List<String> partitionKeys,
            @NotNull final Collection<String> partitionValues,
            @NotNull final Set<String> takenNames,
            final boolean registered) {
        final String relativePathString = relativePath.getPath();
        int partitioningColumnIndex = 0;
        // Split the path to get the subdirectory names
        final String[] subDirs = relativePathString.split(fileSeparator);
        for (int i = 0; i < subDirs.length - 1; i++) {
            final String dirName = subDirs[i];
            if (dirName.isEmpty()) {
                // Ignore empty directory names
                continue;
            }
            final String[] components = dirName.split("=", 2);
            if (components.length != 2) {
                throw new TableDataException("Unexpected directory name format (not key=value) at "
                        + new File(tableRootDirectory.getPath(), relativePathString));
            }
            if (partitioningColumnIndex == maxPartitioningLevels) {
                throw new TableDataException("Too many partitioning levels at " + relativePathString + ", maximum " +
                        "expected partitioning levels are " + maxPartitioningLevels);
            }
            // We use an empty set to allow duplicate partition keys across files
            final String columnKey = NameValidator.legalizeColumnName(components[0], Collections.emptySet());
            if (registered) {
                // We have already seen another parquet file in the tree, so compare the
                // partitioning levels against the previous ones
                if (partitioningColumnIndex >= partitionKeys.size()) {
                    throw new TableDataException("Too many partitioning levels at " + relativePathString + " (expected "
                            + partitionKeys.size() + ") based on earlier parquet files in the tree.");
                }
                if (!partitionKeys.get(partitioningColumnIndex).equals(columnKey)) {
                    throw new TableDataException(String.format(
                            "Column name mismatch at column index %d: expected %s found %s at %s",
                            partitioningColumnIndex, partitionKeys.get(partitioningColumnIndex), columnKey,
                            relativePathString));
                }
            } else {
                // This is the first parquet file in the tree, so accumulate the partitioning levels
                partitionKeys.add(columnKey);
            }
            final String columnValue = components[1];
            partitionValues.add(columnValue);
            partitioningColumnIndex++;
        }
    }
}


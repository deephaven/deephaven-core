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
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static io.deephaven.engine.table.impl.locations.local.KeyValuePartitionLayout.buildLocationKeys;

/**
 * Extracts a Key-Value partitioned layout from a stream of URIs. This class is inspired from
 * {@link KeyValuePartitionLayout}, but for URIs instead of files.
 */
public class URIStreamKeyValuePartitionLayout<TLK extends TableLocationKey> {

    protected final URI tableRootDirectory;
    private final Supplier<KeyValuePartitionLayout.LocationTableBuilder> locationTableBuilderFactory;
    private final BiFunction<URI, Map<String, Comparable<?>>, TLK> keyFactory;
    private final int maxPartitioningLevels;
    private final String fileSeparator;

    public URIStreamKeyValuePartitionLayout(
            @NotNull final URI tableRootDirectory,
            @NotNull final Supplier<KeyValuePartitionLayout.LocationTableBuilder> locationTableBuilderFactory,
            @NotNull final BiFunction<URI, Map<String, Comparable<?>>, TLK> keyFactory,
            final int maxPartitioningLevels,
            @NotNull final String fileSeparator) {
        this.tableRootDirectory = tableRootDirectory;
        this.locationTableBuilderFactory = locationTableBuilderFactory;
        this.keyFactory = keyFactory;
        this.maxPartitioningLevels = Require.geqZero(maxPartitioningLevels, "maxPartitioningLevels");
        this.fileSeparator = fileSeparator;
    }

    public String toString() {
        return URIStreamKeyValuePartitionLayout.class.getSimpleName() + '[' + tableRootDirectory + ']';
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
            final String fileRelativePath = uri.getPath().substring(tableRootDirectory.getPath().length());
            getPartitions(fileRelativePath, partitionKeys, partitionValues, takenNames, registered[0]);
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
        buildLocationKeys(locationTable, targetURIs, locationKeyObserver, keyFactory);
    }

    private void getPartitions(@NotNull final String path,
            @NotNull final List<String> partitionKeys,
            @NotNull final Collection<String> partitionValues,
            @NotNull final Set<String> takenNames,
            final boolean registered) {
        int partitioningColumnIndex = 0;
        // Split the path to get the subdirectory names
        final String[] subDirs = path.split(fileSeparator);
        for (int i = 0; i < subDirs.length - 1; i++) {
            final String dirName = subDirs[i];
            if (dirName.isEmpty()) {
                // Ignore empty directory names
                continue;
            }
            final String[] components = dirName.split("=", 2);
            if (components.length != 2) {
                throw new TableDataException("Unexpected directory name format (not key=value) at "
                        + new File(tableRootDirectory.getPath(), path));
            }
            if (partitioningColumnIndex == maxPartitioningLevels) {
                throw new TableDataException("Too many partitioning levels at " + path + ", maximum " +
                        "expected partitioning levels are " + maxPartitioningLevels);
            }
            // TODO Is this legalizing adding any value?
            final String columnKey = NameValidator.legalizeColumnName(components[0], takenNames);
            if (registered) {
                // We have already seen another parquet file in the tree, so compare the
                // partitioning levels against the previous ones
                if (partitioningColumnIndex >= partitionKeys.size()) {
                    throw new TableDataException("Too many partitioning levels at " + path + " (expected "
                            + partitionKeys.size() + ") based on earlier parquet files in the tree.");
                }
                if (!partitionKeys.get(partitioningColumnIndex).equals(columnKey)) {
                    throw new TableDataException(String.format(
                            "Column name mismatch at column index %d: expected %s found %s at %s",
                            partitioningColumnIndex, partitionKeys.get(partitioningColumnIndex), columnKey,
                            path));
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


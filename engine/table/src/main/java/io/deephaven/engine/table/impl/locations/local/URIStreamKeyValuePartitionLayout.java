//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.locations.local;

import io.deephaven.api.util.NameValidator;
import io.deephaven.base.verify.Require;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.engine.table.impl.locations.TableLocationKey;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.net.URI;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
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

    private static final String URI_SEPARATOR = "/";

    protected final URI tableRootDirectory;
    private final Supplier<KeyValuePartitionLayout.LocationTableBuilder> locationTableBuilderFactory;
    private final int maxPartitioningLevels;

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
            @NotNull final Supplier<LocationTableBuilder> locationTableBuilderFactory,
            @NotNull final BiFunction<URI, Map<String, Comparable<?>>, TLK> keyFactory,
            final int maxPartitioningLevels) {
        super(keyFactory);
        this.tableRootDirectory = tableRootDirectory;
        this.locationTableBuilderFactory = locationTableBuilderFactory;
        this.maxPartitioningLevels = Require.geqZero(maxPartitioningLevels, "maxPartitioningLevels");
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
        final LocationTableBuilder locationTableBuilder = locationTableBuilderFactory.get();
        final Queue<URI> targetURIs = new ArrayDeque<>();
        final List<String> partitionKeys = new ArrayList<>();
        final MutableBoolean registered = new MutableBoolean(false);
        uriStream.forEachOrdered(uri -> {
            final Collection<String> partitionValues = new ArrayList<>();
            final URI relativePath = tableRootDirectory.relativize(uri);
            getPartitions(relativePath, partitionKeys, partitionValues, registered.booleanValue());
            if (registered.isFalse()) {
                // Use the first path to find the partition keys and use the same for the rest
                if (partitionKeys.size() > maxPartitioningLevels) {
                    throw new TableDataException("Too many partitioning levels at " + uri + ", count = " +
                            partitionKeys.size() + ", maximum expected are " + maxPartitioningLevels);
                }
                locationTableBuilder.registerPartitionKeys(partitionKeys);
                registered.setTrue();
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
            final boolean registered) {
        final Set<String> takenNames = new HashSet<>();
        final String relativePathString = relativePath.getPath();
        int partitioningColumnIndex = 0;
        // Split the path to get the subdirectory names
        final String[] subDirs = relativePathString.split(URI_SEPARATOR);
        for (int sdi = 0; sdi < subDirs.length - 1; sdi++) {
            final String dirName = subDirs[sdi];
            if (dirName.isEmpty()) {
                // Ignore empty directory names
                continue;
            }
            final String[] components = dirName.split("=", 2);
            if (components.length != 2) {
                throw new TableDataException("Unexpected directory name format (not key=value) at "
                        + new File(tableRootDirectory.getPath(), relativePathString));
            }
            // We use an empty set to allow duplicate partition keys across files
            final String columnKey = NameValidator.legalizeColumnName(components[0], takenNames);
            takenNames.add(columnKey);
            if (registered) {
                // We have already seen another leaf node in the tree, so compare the partitioning levels against the
                // previous ones
                if (partitioningColumnIndex >= partitionKeys.size()) {
                    throw new TableDataException("Too many partitioning levels at " + relativePathString + " (expected "
                            + partitionKeys.size() + ") based on earlier leaf nodes in the tree.");
                }
                if (!partitionKeys.get(partitioningColumnIndex).equals(columnKey)) {
                    throw new TableDataException(String.format(
                            "Column name mismatch at column index %d: expected %s found %s at %s",
                            partitioningColumnIndex, partitionKeys.get(partitioningColumnIndex), columnKey,
                            relativePathString));
                }
            } else {
                // This is the first leaf node in the tree, so accumulate the partitioning levels
                partitionKeys.add(columnKey);
            }
            final String columnValue = components[1];
            partitionValues.add(columnValue);
            partitioningColumnIndex++;
        }
    }
}


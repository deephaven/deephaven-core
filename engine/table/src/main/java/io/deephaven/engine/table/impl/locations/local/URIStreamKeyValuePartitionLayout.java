//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.locations.local;

import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import io.deephaven.base.verify.Require;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.engine.table.impl.locations.TableLocationKey;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.jetbrains.annotations.NotNull;

import java.net.URI;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static io.deephaven.base.FileUtils.URI_SEPARATOR;

/**
 * Extracts a key-value partitioned table layout from a stream of URIs.
 */
public abstract class URIStreamKeyValuePartitionLayout<TLK extends TableLocationKey>
        extends KeyValuePartitionLayout<TLK, URI> {

    protected final URI tableRootDirectory;
    private final Supplier<LocationTableBuilder> locationTableBuilderFactory;
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
    protected URIStreamKeyValuePartitionLayout(
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
     * Find the keys in the given URI stream and notify the observer. Note that the URIs are not expected to have any
     * extra slashes or other path separators. For example, "/a//b/c" in the path is not expected.
     */
    protected final void findKeys(@NotNull final Stream<URI> uriStream,
            @NotNull final Consumer<TLK> locationKeyObserver) {
        final LocationTableBuilder locationTableBuilder = locationTableBuilderFactory.get();
        final Queue<URI> targetURIs = new ArrayDeque<>();
        final Set<String> partitionKeys = new LinkedHashSet<>(); // Preserve order of insertion
        final TIntObjectMap<ColumnNameInfo> partitionColInfo = new TIntObjectHashMap<>();
        final MutableBoolean registered = new MutableBoolean(false);
        uriStream.forEachOrdered(uri -> {
            final Collection<String> partitionValues = new ArrayList<>();
            final URI relativePath = tableRootDirectory.relativize(uri);
            getPartitions(relativePath, partitionKeys, partitionValues, partitionColInfo, registered.booleanValue());
            if (registered.isFalse()) {
                // Use the first path to find the partition keys and use the same for the rest
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

    private void getPartitions(
            @NotNull final URI relativePath,
            @NotNull final Set<String> partitionKeys,
            @NotNull final Collection<String> partitionValues,
            @NotNull final TIntObjectMap<ColumnNameInfo> partitionColInfo,
            final boolean registered) {
        final String relativePathString = relativePath.getPath();
        // The following assumes that there is exactly one separator between each subdirectory in the path
        final String[] subDirs = relativePathString.split(URI_SEPARATOR);
        final int numPartitioningCol = subDirs.length - 1;
        if (registered) {
            if (numPartitioningCol > partitionKeys.size()) {
                throw new TableDataException("Too many partitioning levels at " + relativePathString + " (expected "
                        + partitionKeys.size() + ") based on earlier leaf nodes in the tree.");
            }
        } else {
            if (numPartitioningCol > maxPartitioningLevels) {
                throw new TableDataException("Too many partitioning levels at " + relativePathString + ", count = " +
                        numPartitioningCol + ", maximum expected are " + maxPartitioningLevels);
            }
        }
        for (int partitioningColIndex = 0; partitioningColIndex < numPartitioningCol; partitioningColIndex++) {
            processSubdirectoryInternal(subDirs[partitioningColIndex], relativePathString, partitioningColIndex,
                    partitionKeys, partitionValues, partitionColInfo);
        }
    }
}


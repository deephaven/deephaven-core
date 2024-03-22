//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.locations.local;

import io.deephaven.api.util.NameValidator;
import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.engine.table.impl.locations.TableLocationKey;
import io.deephaven.engine.table.impl.locations.impl.TableLocationKeyFinder;
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
import java.util.function.Predicate;
import java.util.function.Supplier;

import static io.deephaven.engine.table.impl.locations.local.KeyValuePartitionLayout.buildLocationKeys;

/**
 * Extracts a Key-Value partitioned layout from a list of URIs.
 */
public class URIListKeyValuePartitionLayout<TLK extends TableLocationKey> implements TableLocationKeyFinder<TLK> {

    protected final URI tableRootDirectory;
    private final Predicate<URI> pathFilter;
    private final Supplier<LocationTableBuilder> locationTableBuilderFactory;
    private final BiFunction<URI, Map<String, Comparable<?>>, TLK> keyFactory;
    private final int maxPartitioningLevels;
    private Consumer<Consumer<URI>> uriListSupplier;

    public URIListKeyValuePartitionLayout(
            @NotNull final URI tableRootDirectory,
            @NotNull final Predicate<URI> pathFilter,
            @NotNull final Supplier<LocationTableBuilder> locationTableBuilderFactory,
            @NotNull final BiFunction<URI, Map<String, Comparable<?>>, TLK> keyFactory,
            final int maxPartitioningLevels) {
        this.tableRootDirectory = tableRootDirectory;
        this.pathFilter = pathFilter;
        this.locationTableBuilderFactory = locationTableBuilderFactory;
        this.keyFactory = keyFactory;
        this.maxPartitioningLevels = Require.geqZero(maxPartitioningLevels, "maxPartitioningLevels");
    }

    public String toString() {
        return URIListKeyValuePartitionLayout.class.getSimpleName() + '[' + tableRootDirectory + ']';
    }

    protected final void setURIListSupplier(@NotNull final Consumer<Consumer<URI>> uriSupplier) {
        this.uriListSupplier = uriSupplier;
    }

    @Override
    public final void findKeys(@NotNull final Consumer<TLK> locationKeyObserver) {
        Assert.neqNull(uriListSupplier, "uriListSupplier");
        final LocationTableBuilder locationTableBuilder = locationTableBuilderFactory.get();
        final Deque<URI> targetURIs = new ArrayDeque<>();
        final Set<String> takenNames = new HashSet<>();
        final List<String> partitionKeys = new ArrayList<>();
        final boolean[] registered = {false}; // Hack to make the variable final
        final Consumer<URI> uriProcessor = (final URI uri) -> {
            if (!pathFilter.test(uri)) {
                return;
            }
            final Collection<String> partitionValues = new ArrayList<>();
            final String fileRelativePath = uri.getPath().substring(tableRootDirectory.getPath().length());
            getPartitions(fileRelativePath, partitionKeys, partitionValues, takenNames, registered[0]);
            if (!registered[0]) {
                locationTableBuilder.registerPartitionKeys(partitionKeys);
                registered[0] = true;
            }
            locationTableBuilder.acceptLocation(partitionValues);
            targetURIs.add(uri);
        };
        uriListSupplier.accept(uriProcessor);
        final Table locationTable = locationTableBuilder.build();
        buildLocationKeys(locationTable, targetURIs, locationKeyObserver, keyFactory);
    }

    private void getPartitions(@NotNull final String path,
            @NotNull final List<String> partitionKeys,
            @NotNull final Collection<String> partitionValues,
            @NotNull final Set<String> takenNames,
            final boolean registered) {
        int partitioningColumnIndex = 0;
        int start = 0;
        // TODO Should I just do a split instead of iteration
        for (int i = 0; i < path.length(); i++) {
            // Find the next directory name delimited by "/" or end of the string
            if (path.charAt(i) != '/') {
                continue;
            }
            if (start < i) { // Ignore empty directory names
                final String dirName = path.substring(start, i);
                final String[] components = dirName.split("=");
                if (components.length != 2) {
                    throw new TableDataException("Unexpected directory name format (not key=value) at "
                            + new File(tableRootDirectory.getPath(), path));
                }
                if (partitioningColumnIndex == maxPartitioningLevels) {
                    throw new TableDataException("Too many partitioning levels at " + path + ", maximum " +
                            "expected partitioning levels are " + maxPartitioningLevels);
                }
                // TODO Is this legalizing adding any value? There is also a comment in the class description
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
            start = i + 1;
        }
    }
}


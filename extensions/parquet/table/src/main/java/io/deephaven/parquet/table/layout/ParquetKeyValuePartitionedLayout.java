/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.table.layout;

import io.deephaven.api.util.NameValidator;
import io.deephaven.csv.CsvTools;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.engine.table.impl.locations.impl.TableLocationKeyFinder;
import io.deephaven.engine.table.impl.locations.local.LocationTableBuilder;
import io.deephaven.engine.table.impl.locations.local.LocationTableBuilderDefinition;
import io.deephaven.parquet.table.ParquetInstructions;
import io.deephaven.parquet.table.location.ParquetTableLocationKey;
import io.deephaven.util.channel.SeekableChannelsProvider;
import io.deephaven.util.channel.SeekableChannelsProviderLoader;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static io.deephaven.engine.table.impl.locations.local.KeyValuePartitionLayout.buildLocationKeys;
import static io.deephaven.parquet.table.layout.ParquetFileHelper.isNonHiddenParquetURI;

/**
 * Key-Value partitioned layout for Parquet data.
 * 
 * @implNote
 *           <ul>
 *           <li>Unless table definition is provided, type inference for partitioning column uses
 *           {@link CsvTools#readCsv(java.io.InputStream) CsvTools.readCsv} as a conversion tool, and hence follows the
 *           same rules.</li>
 *           <li>Column names will be legalized via {@link NameValidator#legalizeColumnName(String, Set)
 *           NameValidator.legalizeColumnName}.</li>
 *           </ul>
 */
public class ParquetKeyValuePartitionedLayout implements TableLocationKeyFinder<ParquetTableLocationKey> {

    private final URI tableRootDirectory;
    private final Supplier<LocationTableBuilder> locationTableBuilderFactory;
    private final int maxPartitioningLevels;
    private final ParquetInstructions readInstructions;

    public ParquetKeyValuePartitionedLayout(
            @NotNull final File tableRootDirectory,
            @NotNull final TableDefinition tableDefinition,
            @NotNull final ParquetInstructions readInstructions) {
        this(tableRootDirectory.toURI(), tableDefinition, readInstructions);
    }

    public ParquetKeyValuePartitionedLayout(
            @NotNull final URI tableRootDirectory,
            @NotNull final TableDefinition tableDefinition,
            @NotNull final ParquetInstructions readInstructions) {
        this.tableRootDirectory = tableRootDirectory;
        this.locationTableBuilderFactory = () -> new LocationTableBuilderDefinition(tableDefinition);
        this.maxPartitioningLevels =
                Math.toIntExact(tableDefinition.getColumnStream().filter(ColumnDefinition::isPartitioning).count());
        this.readInstructions = readInstructions;
    }

    public ParquetKeyValuePartitionedLayout(
            @NotNull final File tableRootDirectory,
            final int maxPartitioningLevels,
            @NotNull final ParquetInstructions readInstructions) {
        this(tableRootDirectory.toURI(), maxPartitioningLevels, readInstructions);
    }

    public ParquetKeyValuePartitionedLayout(
            @NotNull final URI tableRootDirectory,
            final int maxPartitioningLevels,
            @NotNull final ParquetInstructions readInstructions) {
        this.tableRootDirectory = tableRootDirectory;
        this.locationTableBuilderFactory = () -> new LocationTableBuilderCsv(tableRootDirectory);
        this.maxPartitioningLevels = maxPartitioningLevels;
        this.readInstructions = readInstructions;
    }

    public String toString() {
        return ParquetKeyValuePartitionedLayout.class.getSimpleName() + '[' + tableRootDirectory + ']';
    }

    @Override
    public void findKeys(@NotNull Consumer<ParquetTableLocationKey> locationKeyObserver) {
        final SeekableChannelsProvider provider = SeekableChannelsProviderLoader.getInstance().fromServiceLoader(
                tableRootDirectory, readInstructions.getSpecialInstructions());
        final LocationTableBuilder locationTableBuilder = locationTableBuilderFactory.get();
        final Deque<URI> targetURIs = new ArrayDeque<>();
        {
            final Set<String> takenNames = new HashSet<>();
            final List<String> partitionKeys = new ArrayList<>();
            final boolean[] registered = {false}; // Hack to make the variable final
            try {
                provider.applyToChildURIsRecursively(tableRootDirectory, uri -> {
                    if (!isNonHiddenParquetURI(uri)) {
                        // Skip non-parquet URIs
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
                });
            } catch (final IOException e) {
                throw new TableDataException("Error finding parquet locations under " + tableRootDirectory, e);
            }
        }
        final Table locationTable = locationTableBuilder.build();
        buildLocationKeys(locationTable, targetURIs, locationKeyObserver,
                (uri, partitions) -> new ParquetTableLocationKey(uri, 0, partitions, readInstructions));
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

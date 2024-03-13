/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.locations.local;

import io.deephaven.base.verify.Require;
import io.deephaven.engine.table.Table;
import io.deephaven.api.util.NameValidator;
import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.engine.table.impl.locations.TableLocationKey;
import io.deephaven.engine.table.impl.locations.impl.TableLocationKeyFinder;
import io.deephaven.engine.table.ColumnSource;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * {@link TableLocationKeyFinder Location finder} that will traverse a directory hierarchy and infer partitions from
 * key-value pairs in the directory names, for example:
 * 
 * <pre>
 * tableRootDirectory/Country=France/City=Paris/parisData.parquet
 * </pre>
 * 
 * Traversal is depth-first, and assumes that target files will only be found at a single depth.
 *
 * @implNote Column names will be legalized via {@link NameValidator#legalizeColumnName(String, Set)}.
 */
public class KeyValuePartitionLayout<TLK extends TableLocationKey> implements TableLocationKeyFinder<TLK> {

    private final File tableRootDirectory;
    private final Predicate<Path> pathFilter;
    private final Supplier<LocationTableBuilder> locationTableBuilderFactory;
    private final BiFunction<Path, Map<String, Comparable<?>>, TLK> keyFactory;
    private final int maxPartitioningLevels;

    /**
     * @param tableRootDirectory The directory to traverse from
     * @param pathFilter Filter to determine whether a regular file should be used to create a key
     * @param locationTableBuilderFactory Factory for {@link LocationTableBuilder builders} used to organize partition
     *        information; as builders are typically stateful, a new builder is created each time this
     *        KeyValuePartitionLayout is used to {@link #findKeys(Consumer) find keys}
     * @param keyFactory Key factory function
     * @param maxPartitioningLevels Maximum partitioning levels to traverse. Must be {@code >= 0}. {@code 0} means only
     *        look at files in {@code tableRootDirectory} and find no partitions.
     */
    public KeyValuePartitionLayout(
            @NotNull final File tableRootDirectory,
            @NotNull final Predicate<Path> pathFilter,
            @NotNull final Supplier<LocationTableBuilder> locationTableBuilderFactory,
            @NotNull final BiFunction<Path, Map<String, Comparable<?>>, TLK> keyFactory,
            final int maxPartitioningLevels) {
        this.tableRootDirectory = tableRootDirectory;
        this.pathFilter = pathFilter;
        this.locationTableBuilderFactory = locationTableBuilderFactory;
        this.keyFactory = keyFactory;
        this.maxPartitioningLevels = Require.geqZero(maxPartitioningLevels, "maxPartitioningLevels");
    }

    public String toString() {
        return KeyValuePartitionLayout.class.getSimpleName() + '[' + tableRootDirectory + ']';
    }

    @Override
    public void findKeys(@NotNull final Consumer<TLK> locationKeyObserver) {
        final Deque<Path> targetFiles = new ArrayDeque<>();
        final LocationTableBuilder locationTableBuilder = locationTableBuilderFactory.get();
        try {
            Files.walkFileTree(tableRootDirectory.toPath(), EnumSet.of(FileVisitOption.FOLLOW_LINKS),
                    maxPartitioningLevels + 1, new SimpleFileVisitor<>() {
                        final Set<String> takenNames = new HashSet<>();
                        final List<String> partitionKeys = new ArrayList<>();
                        final List<String> partitionValues = new ArrayList<>();
                        boolean registered;
                        int columnCount = -1;

                        @Override
                        public FileVisitResult preVisitDirectory(
                                @NotNull final Path dir,
                                @NotNull final BasicFileAttributes attrs) {
                            final String dirName = dir.getFileName().toString();
                            // Skip dot directories
                            if (!dirName.isEmpty() && dirName.charAt(0) == '.') {
                                return FileVisitResult.SKIP_SUBTREE;
                            }
                            if (++columnCount > 0) {
                                // We're descending and past the root
                                final String[] components = dirName.split("=");
                                if (components.length != 2) {
                                    throw new TableDataException(
                                            "Unexpected directory name format (not key=value) at " + dir);
                                }
                                final String columnKey = NameValidator.legalizeColumnName(components[0], takenNames);
                                final int columnIndex = columnCount - 1;
                                if (columnCount > partitionKeys.size()) {
                                    partitionKeys.add(columnKey);
                                } else if (!partitionKeys.get(columnIndex).equals(columnKey)) {
                                    throw new TableDataException(String.format(
                                            "Column name mismatch at column index %d: expected %s found %s at %s",
                                            columnIndex, partitionKeys.get(columnIndex), columnKey, dir));
                                }
                                final String columnValue = components[1];
                                partitionValues.add(columnValue);
                            }
                            return FileVisitResult.CONTINUE;
                        }

                        @Override
                        public FileVisitResult visitFile(
                                @NotNull final Path file,
                                @NotNull final BasicFileAttributes attrs) {
                            if (attrs.isRegularFile() && pathFilter.test(file)) {
                                if (!registered) {
                                    locationTableBuilder.registerPartitionKeys(partitionKeys);
                                    registered = true;
                                }
                                locationTableBuilder.acceptLocation(partitionValues);
                                targetFiles.add(file);
                            }
                            return FileVisitResult.CONTINUE;
                        }

                        @Override
                        public FileVisitResult postVisitDirectory(
                                @NotNull final Path dir,
                                @Nullable final IOException exc) throws IOException {
                            if (--columnCount >= 0) {
                                partitionValues.remove(columnCount);
                            }
                            return super.postVisitDirectory(dir, exc);
                        }
                    });
        } catch (IOException e) {
            throw new TableDataException("Error finding locations for under " + tableRootDirectory, e);
        }

        final Table locationTable = locationTableBuilder.build();
        buildLocationKeys(locationTable, targetFiles, locationKeyObserver, keyFactory);
    }

    /**
     * Build location keys from a location table and a collection of target files.
     *
     * @param <TABLE_LOCATION_KEY> The type of the location key
     * @param <TARGET_FILE_TYPE> The type of the target files
     * @param locationTable The location table
     * @param targetFiles The target files
     * @param locationKeyObserver A consumer which will receive the location keys
     * @param keyFactory A factory for creating location keys
     */
    public static <TABLE_LOCATION_KEY extends TableLocationKey, TARGET_FILE_TYPE> void buildLocationKeys(
            @NotNull final Table locationTable,
            @NotNull final Deque<TARGET_FILE_TYPE> targetFiles,
            @NotNull final Consumer<TABLE_LOCATION_KEY> locationKeyObserver,
            final BiFunction<TARGET_FILE_TYPE, Map<String, Comparable<?>>, TABLE_LOCATION_KEY> keyFactory) {
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

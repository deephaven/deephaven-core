//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.locations.local;

import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import io.deephaven.base.verify.Require;
import io.deephaven.engine.table.Table;
import io.deephaven.api.util.NameValidator;
import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.engine.table.impl.locations.TableLocationKey;
import io.deephaven.engine.table.impl.locations.impl.TableLocationKeyFinder;
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
 * {@link TableLocationKeyFinder Location finder} that will take a directory file, traverse the directory hierarchy and
 * infer partitions from key-value pairs in the directory names, for example:
 *
 * <pre>
 * tableRootDirectory/Country=France/City=Paris/parisData.parquet
 * </pre>
 *
 * Traversal is depth-first, and assumes that target files will only be found at a single depth. This class is
 * specialized for handling of files. For handling of URIs, see {@link URIStreamKeyValuePartitionLayout}.
 *
 * @implNote Column names will be legalized via {@link NameValidator#legalizeColumnName(String, Set)}.
 */
public class FileKeyValuePartitionLayout<TLK extends TableLocationKey>
        extends KeyValuePartitionLayout<TLK, Path>
        implements TableLocationKeyFinder<TLK> {

    private final File tableRootDirectory;
    private final Predicate<Path> pathFilter;
    private final Supplier<LocationTableBuilder> locationTableBuilderFactory;
    private final int maxPartitioningLevels;

    /**
     * @param tableRootDirectory The directory to traverse from
     * @param pathFilter Filter to determine whether a regular file should be used to create a key
     * @param locationTableBuilderFactory Factory for {@link LocationTableBuilder builders} used to organize partition
     *        information; as builders are typically stateful, a new builder is created each time this
     *        {@link KeyValuePartitionLayout} is used to {@link #findKeys(Consumer) find keys}
     * @param keyFactory Factory function used to generate table location keys from target files and partition values
     * @param maxPartitioningLevels Maximum partitioning levels to traverse. Must be {@code >= 0}. {@code 0} means only
     *        look at files in {@code tableRootDirectory} and find no partitions.
     */
    public FileKeyValuePartitionLayout(
            @NotNull final File tableRootDirectory,
            @NotNull final Predicate<Path> pathFilter,
            @NotNull final Supplier<LocationTableBuilder> locationTableBuilderFactory,
            @NotNull final BiFunction<Path, Map<String, Comparable<?>>, TLK> keyFactory,
            final int maxPartitioningLevels) {
        super(keyFactory);
        this.tableRootDirectory = tableRootDirectory;
        this.pathFilter = pathFilter;
        this.locationTableBuilderFactory = locationTableBuilderFactory;
        this.maxPartitioningLevels = Require.geqZero(maxPartitioningLevels, "maxPartitioningLevels");
    }

    @Override
    public String toString() {
        return FileKeyValuePartitionLayout.class.getSimpleName() + '[' + tableRootDirectory + ']';
    }

    @Override
    public void findKeys(@NotNull final Consumer<TLK> locationKeyObserver) {
        final Queue<Path> targetFiles = new ArrayDeque<>();
        final LocationTableBuilder locationTableBuilder = locationTableBuilderFactory.get();
        try {
            Files.walkFileTree(tableRootDirectory.toPath(), EnumSet.of(FileVisitOption.FOLLOW_LINKS),
                    maxPartitioningLevels + 1, new SimpleFileVisitor<>() {
                        final Set<String> partitionKeys = new LinkedHashSet<>(); // Preserve order of insertion
                        final List<String> partitionValues = new ArrayList<>();
                        final TIntObjectMap<ColumnNameInfo> partitionColInfo = new TIntObjectHashMap<>();
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
                                final int columnIndex = columnCount - 1;
                                processSubdirectoryInternal(dirName, dir.toString(), columnIndex, partitionKeys,
                                        partitionValues, partitionColInfo);
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
        buildLocationKeys(locationTable, targetFiles, locationKeyObserver);
    }
}

package io.deephaven.db.v2.locations.local;

import io.deephaven.base.verify.Require;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.utils.TableTools;
import io.deephaven.db.v2.locations.TableDataException;
import io.deephaven.db.v2.locations.TableLocationKey;
import io.deephaven.db.v2.locations.impl.TableLocationKeyFinder;
import io.deephaven.db.v2.locations.parquet.local.ParquetTableLocationKey;
import io.deephaven.db.v2.parquet.ParquetTableWriter;
import io.deephaven.db.v2.sources.ColumnSource;
import org.apache.commons.text.StringEscapeUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * {@link TableLocationKeyFinder Location finder} that will traverse a directory hierarchy and infer partitions from
 * key-value paris in the directory names, e.g.
 * <pre>tableRootDirectory/Country=France/City=Paris/parisData.parquet</pre>.
 * Traversal is depth-first, and assumes that target files will only be found at a single depth.
 *
 * @implNote Type inference uses {@link TableTools#readCsv(java.io.InputStream)} as a conversion tool, and hence
 * follows the same rules.
 */
public final class HiveStylePartitionLayout<TLK extends TableLocationKey> implements TableLocationKeyFinder<TLK> {

    public TableLocationKeyFinder<ParquetTableLocationKey> forParquet(@NotNull final File tableRootDirectory,
                                                                      final int maxDepth) {
        return new HiveStylePartitionLayout<>(
                tableRootDirectory,
                path -> path.getFileName().endsWith(ParquetTableWriter.PARQUET_FILE_EXTENSION),
                (path, partitions) -> new ParquetTableLocationKey(path.toFile(), partitions),
                maxDepth
        );
    }

    private final File tableRootDirectory;
    private final Predicate<Path> pathFilter;
    private final BiFunction<Path, Map<String, Comparable<?>>, TLK> keyFactory;
    private final int maxDepth;

    /**
     * @param tableRootDirectory The directory to traverse from
     * @param pathFilter         Filter to determine whether a regular file should be used to create a key
     * @param keyFactory         Key factory function
     * @param maxDepth           Maximum depth to traverse. Must be &ge; 0.
     *                           0 means only look at {@code tableRootDirectory}.
     */
    private HiveStylePartitionLayout(@NotNull final File tableRootDirectory,
                                     @NotNull final Predicate<Path> pathFilter,
                                     @NotNull final BiFunction<Path, Map<String, Comparable<?>>, TLK> keyFactory,
                                     final int maxDepth) {
        this.tableRootDirectory = tableRootDirectory;
        this.pathFilter = pathFilter;
        this.keyFactory = keyFactory;
        this.maxDepth = Require.gtZero(maxDepth, "maxDepth");
    }

    @Override
    public void findKeys(@NotNull final Consumer<TLK> locationKeyObserver) {
        final StringBuilder csvBuilder = new StringBuilder();
        final Deque<Path> targetFiles = new ArrayDeque<>();

        try {
            Files.walkFileTree(tableRootDirectory.toPath(), EnumSet.of(FileVisitOption.FOLLOW_LINKS), maxDepth, new SimpleFileVisitor<Path>() {
                final String ls = System.lineSeparator();
                final List<String> columnKeys = new ArrayList<>();
                final List<String> rowValues = new ArrayList<>();
                String row;
                int columnIndex = -1;

                @Override
                public FileVisitResult preVisitDirectory(@NotNull final Path dir, @NotNull final BasicFileAttributes attrs) {
                    if (columnIndex >= 0) {
                        // We're descending and past the root
                        final String[] components = dir.getFileName().toString().split("=");
                        if (components.length != 2) {
                            throw new TableDataException("Unexpected directory name format (not key=value) at " + dir);
                        }
                        final String columnKey = components[0];
                        if (columnIndex >= columnKeys.size()) {
                            columnKeys.add(columnKey);
                        } else if (!columnKeys.get(columnIndex).equals(columnKey)) {
                            throw new TableDataException("Column name mismatch at index " + columnIndex
                                    + ": expected " + columnKeys.get(columnIndex) + " found " + columnKey + " at " + dir);
                        }
                        final String columnValue = components[1];
                        rowValues.add(columnValue);
                    }
                    ++columnIndex;
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFile(@NotNull final Path file, @NotNull final BasicFileAttributes attrs) {
                    if (pathFilter.test(file)) {
                        if (csvBuilder.length() == 0) {
                            csvBuilder.append(listToCsvRow(columnKeys)).append(ls);
                        }
                        if (row == null) {
                            row = listToCsvRow(rowValues);
                        }
                        csvBuilder.append(row).append(ls);
                        targetFiles.add(file);
                    }
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult postVisitDirectory(@NotNull final Path dir, @Nullable final IOException exc) throws IOException {
                    if (columnIndex >= 0) {
                        row = null;
                        rowValues.remove(columnIndex);
                    }
                    --columnIndex;
                    return super.postVisitDirectory(dir, exc);
                }
            });
        } catch (IOException e) {
            throw new TableDataException("Error finding locations for under " + tableRootDirectory, e);
        }

        final Table partitioningColumnTable;
        try {
            partitioningColumnTable = TableTools.readCsv(new ByteArrayInputStream(csvBuilder.toString().getBytes()));
        } catch (IOException e) {
            throw new TableDataException("Failed converting partition CSV to table for " + tableRootDirectory, e);
        }

        final Map<String, Comparable<?>> partitions = new LinkedHashMap<>();
        final String[] partitionKeys = partitioningColumnTable.getDefinition().getColumnNamesArray();
        //noinspection unchecked
        final ColumnSource<? extends Comparable<?>>[] partitionValueSources = partitioningColumnTable.getColumnSources().toArray(ColumnSource.ZERO_LENGTH_COLUMN_SOURCE_ARRAY);
        final int numColumns = partitionValueSources.length;
        partitioningColumnTable.getIndex().forAllLongs((final long indexKey) -> {
            for (int ci = 0; ci < numColumns; ++ci) {
                partitions.put(partitionKeys[ci], partitionValueSources[ci].get(indexKey));
            }
            locationKeyObserver.accept(keyFactory.apply(targetFiles.remove(), partitions));
        });
    }

    private static String listToCsvRow(@NotNull final List<String> list) {
        return list.stream().map(StringEscapeUtils::escapeCsv).collect(Collectors.joining(","));
    }
}

package io.deephaven.db.v2.locations.local;

import io.deephaven.base.verify.Require;
import io.deephaven.db.v2.locations.TableDataException;
import io.deephaven.db.v2.locations.TableLocationKey;
import io.deephaven.db.v2.locations.impl.TableLocationKeyFinder;
import io.deephaven.db.v2.locations.parquet.local.ParquetTableLocationKey;
import io.deephaven.db.v2.parquet.ParquetTableWriter;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * {@link TableLocationKeyFinder Location finder} that will traverse a directory hierarchy and infer partitions from
 * key-value paris in the directory names, e.g.
 * <pre>tableRootDirectory/Country=France/City=Paris/parisData.parquet</pre>.
 * Traversal is breadth-first, and stops at the first depth at which target files are found.
 */
public final class HiveStylePartitionLayout<TLK extends TableLocationKey> implements TableLocationKeyFinder<TLK> {

    public TableLocationKeyFinder<ParquetTableLocationKey> forParquet(@NotNull final File tableRootDirectory,
                                                                      final boolean inferPartitioningColumnTypes,
                                                                      final int maxDepth) {
        return new HiveStylePartitionLayout<>(
                tableRootDirectory,
                inferPartitioningColumnTypes,
                path -> path.getFileName().endsWith(ParquetTableWriter.PARQUET_FILE_EXTENSION),
                (path, partitions) -> new ParquetTableLocationKey(path.toFile(), partitions),
                maxDepth
        );
    }

    private final File tableRootDirectory;
    private final boolean inferPartitioningColumnTypes;
    private final Predicate<Path> pathFilter;
    private final BiFunction<Path, Map<String, Comparable<?>>, TLK> keyFactory;
    private final int maxDepth;

    /**
     * @param tableRootDirectory           The directory to traverse from
     * @param inferPartitioningColumnTypes Whether to infer partitioning column types.
     *                                     {@code false} means always use {@code String}.
     * @param pathFilter                   Filter to determine whether a regular file should be used to create a key
     * @param keyFactory                   Key factory function
     * @param maxDepth                     Maximum depth to traverse. Must be &ge; 0.
     *                                     0 means only look at {@code tableRootDirectory}.
     */
    private HiveStylePartitionLayout(@NotNull final File tableRootDirectory,
                                     final boolean inferPartitioningColumnTypes,
                                     @NotNull final Predicate<Path> pathFilter,
                                     @NotNull final BiFunction<Path, Map<String, Comparable<?>>, TLK> keyFactory,
                                     final int maxDepth) {
        this.tableRootDirectory = tableRootDirectory;
        this.inferPartitioningColumnTypes = inferPartitioningColumnTypes;
        this.pathFilter = pathFilter;
        this.keyFactory = keyFactory;
        this.maxDepth = Require.gtZero(maxDepth, "maxDepth");
    }

    private static final class Partition {

        private final Path path;
        private final String key;
        private final String stringValue;

        private final List<Partition> children = new ArrayList<>();

        private Comparable<?> resultValue;

        private Partition(@NotNull final Path path) {
            this.path = path;
            final String[] components = path.getFileName().toString().split("=");
            if (components.length != 2) {
                throw new TableDataException("Encountered partition with non-conforming (key=value) name: " + path);
            }
            key = components[0];
            stringValue = components[1];
        }
    }

    @Override
    public void findKeys(@NotNull final Consumer<TLK> locationKeyObserver) {
        boolean descending = true; // We have not reached our target depth yet
        int currentDepth = 0;

        Deque<Path> pathsAtPreviousDepth = new ArrayDeque<>();
        Deque<Path> pathsAtCurrentDepth = new ArrayDeque<>();
        final Deque<String> namesAtCurrentDepth = new ArrayDeque<>();

        pathsAtPreviousDepth.add(tableRootDirectory.toPath());
        while (descending && currentDepth++ < maxDepth && !pathsAtPreviousDepth.isEmpty()) {
            for (Path currentDir = pathsAtPreviousDepth.remove(); currentDir != null; currentDir = pathsAtPreviousDepth.poll()) {
                try (final DirectoryStream<Path> currentDirStream = Files.newDirectoryStream(currentDir)) {
                    for (final Path currentDirEntry : currentDirStream) {
                        final BasicFileAttributes attr = Files.readAttributes(currentDirEntry, BasicFileAttributes.class);
                        if (attr.isRegularFile()) {
                            if (pathFilter.test(currentDirEntry)) {
                                if (descending) {
                                    pathsAtCurrentDepth.clear(); // In case we saw any directories to ignore
                                    descending = false;
                                }
                                pathsAtCurrentDepth.add(currentDirEntry);
                            }
                        } else if (descending && attr.isDirectory()) {
                            pathsAtCurrentDepth.add(currentDirEntry);
                        }
                    }
                } catch (IOException e) {
                    throw new TableDataException("Error finding locations under " + currentDir, e);
                }
            }
            if (descending) {
                pathsAtCurrentDepth.stream().map(path -> path.getFileName().toString()).forEach(namesAtCurrentDepth::add);

            }
        }

        final Map<String, Comparable<?>> partitions = new LinkedHashMap<>();
    }
}

//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.base;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public final class IcebergTestUtils {
    /**
     * Get a stream of all {@link DataFile} objects from the given {@link Table} and {@link Snapshot}.
     *
     * @param table The {@link Table} to retrieve data files for.
     * @param snapshot The {@link Snapshot} to retrieve data files from.
     *
     * @return A stream of {@link DataFile} objects.
     */
    public static Stream<DataFile> allDataFiles(@NotNull final Table table, @NotNull final Snapshot snapshot) {
        return snapshot.allManifests(table.io())
                .stream()
                .map(x -> ManifestFiles.read(x, table.io()))
                .flatMap(IcebergTestUtils::toStream);
    }

    /**
     * Convert a {@link org.apache.iceberg.io.CloseableIterable} to a {@link Stream} that will close the iterable when
     * the stream is closed.
     */
    private static <T> Stream<T> toStream(final org.apache.iceberg.io.CloseableIterable<T> iterable) {
        return StreamSupport.stream(iterable.spliterator(), false).onClose(() -> {
            try {
                iterable.close();
            } catch (final IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }
}

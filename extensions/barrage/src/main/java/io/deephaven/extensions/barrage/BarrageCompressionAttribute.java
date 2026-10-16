//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage;

import io.deephaven.engine.table.AttributeMap;
import io.deephaven.engine.table.Table;
import io.deephaven.grpc.compression.CompressionType;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Sets the {@link Table#BARRAGE_COMPRESSION_ATTRIBUTE} of a table, rollup table or tree table, which allows the server
 * to compress its Barrage snapshots and subscriptions.
 */
public final class BarrageCompressionAttribute {

    private BarrageCompressionAttribute() {}

    /**
     * Returns a copy of {@code grid} that allows the server to compress its Barrage snapshots and subscriptions with
     * {@code preference}. The server uses the first type in {@code preference} that the client accepts, and sends
     * uncompressed data when none match. Repeated types keep their first position. With no types, the result is never
     * compressed, even when the server has default compression types.
     *
     * @param grid the table, rollup table or tree table to compress
     * @param preference the compression types to allow, most preferred first
     * @return a copy of {@code grid} with its {@link Table#BARRAGE_COMPRESSION_ATTRIBUTE} set, to an empty list if
     *         {@code preference} is empty
     */
    public static <T extends AttributeMap<T>> T withCompression(
            @NotNull final T grid,
            @NotNull final CompressionType... preference) {
        return withCompression(grid, Arrays.asList(preference));
    }

    /**
     * Like {@link #withCompression(AttributeMap, CompressionType...)}, with the compression types in a list.
     *
     * @param grid the table, rollup table or tree table to compress
     * @param preference the compression types to allow, most preferred first
     * @return a copy of {@code grid} with its {@link Table#BARRAGE_COMPRESSION_ATTRIBUTE} set, to an empty list if
     *         {@code preference} is empty
     */
    public static <T extends AttributeMap<T>> T withCompression(
            @NotNull final T grid,
            @NotNull final List<CompressionType> preference) {
        final String value = preference.stream()
                .map(type -> Objects.requireNonNull(type, "type").encodingName())
                .distinct()
                .collect(Collectors.joining(","));
        return grid.withAttributes(Map.of(Table.BARRAGE_COMPRESSION_ATTRIBUTE, value));
    }
}

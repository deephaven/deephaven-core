//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage;

import io.deephaven.engine.table.AttributeMap;
import io.deephaven.engine.table.Table;
import io.deephaven.grpc.compression.CompressionCodecs;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Sets the {@link Table#BARRAGE_COMPRESSION_ATTRIBUTE} of a table, rollup table or tree table, which allows the server
 * to compress its Barrage snapshots and subscriptions, or removes it, so that the server's default compression types
 * apply.
 */
public final class BarrageCompressionAttribute {

    /** Why {@code null} is not a list of compression types, and what to pass instead. */
    private static final String NULL_PREFERENCE = "compression types must not be null; pass none to never compress, "
            + "or call withDefaultCompression for the server's default compression types";

    private BarrageCompressionAttribute() {}

    /**
     * Returns a copy of {@code grid} that allows the server to compress its Barrage snapshots and subscriptions with
     * {@code preference}: the names of {@link CompressionCodecs#SUPPORTED supported} compression types, such as the
     * built-in {@code "zstd"}, {@code "gzip"}, {@code "snappy"} and {@code "deflate"}. Case and surrounding whitespace
     * are ignored. The server uses the first type in {@code preference} that the client accepts, and sends uncompressed
     * data when none match. Repeated types keep their first position. With no types, the result is never compressed,
     * even when the server has default compression types; {@link #withDefaultCompression(AttributeMap)} returns to
     * those instead.
     *
     * @param grid the table, rollup table or tree table to compress
     * @param preference the names of the compression types to allow, most preferred first
     * @return a copy of {@code grid} with its {@link Table#BARRAGE_COMPRESSION_ATTRIBUTE} set, to an empty list if
     *         {@code preference} is empty
     * @throws IllegalArgumentException if a name is not a supported compression type
     * @throws NullPointerException if {@code preference} or one of its names is {@code null}
     */
    public static <T extends AttributeMap<T>> T withCompression(
            @NotNull final T grid,
            @NotNull final String... preference) {
        return withCompression(grid, Arrays.asList(Objects.requireNonNull(preference, NULL_PREFERENCE)));
    }

    /**
     * Like {@link #withCompression(AttributeMap, String...)}, with the compression type names in a list.
     *
     * @param grid the table, rollup table or tree table to compress
     * @param preference the names of the compression types to allow, most preferred first
     * @return a copy of {@code grid} with its {@link Table#BARRAGE_COMPRESSION_ATTRIBUTE} set, to an empty list if
     *         {@code preference} is empty
     * @throws IllegalArgumentException if a name is not a supported compression type
     * @throws NullPointerException if {@code preference} or one of its names is {@code null}
     */
    public static <T extends AttributeMap<T>> T withCompression(
            @NotNull final T grid,
            @NotNull final List<String> preference) {
        final String value = Objects.requireNonNull(preference, NULL_PREFERENCE).stream()
                .map(name -> CompressionCodecs.checkSupported(Objects.requireNonNull(name, NULL_PREFERENCE)))
                .distinct()
                .collect(Collectors.joining(","));
        return grid.withAttributes(Map.of(Table.BARRAGE_COMPRESSION_ATTRIBUTE, value));
    }

    /**
     * Returns a copy of {@code grid} without a {@link Table#BARRAGE_COMPRESSION_ATTRIBUTE}, which undoes
     * {@link #withCompression(AttributeMap, String...) withCompression}: the server compresses the result's Barrage
     * snapshots and subscriptions with its default compression types, from the {@code BarrageCompression.defaultTypes}
     * property, and does not compress them when that property is unset.
     *
     * @param grid the table, rollup table or tree table
     * @return a copy of {@code grid} without a {@link Table#BARRAGE_COMPRESSION_ATTRIBUTE}
     */
    public static <T extends AttributeMap<T>> T withDefaultCompression(@NotNull final T grid) {
        return grid.withoutAttributes(Set.of(Table.BARRAGE_COMPRESSION_ATTRIBUTE));
    }
}

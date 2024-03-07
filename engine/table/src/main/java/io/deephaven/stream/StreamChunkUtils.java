//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.stream;

import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import org.jetbrains.annotations.NotNull;

import java.time.Instant;

/**
 * Common utilities to support stream ingestion to {@link WritableChunk chunks}, which is an important building block.
 * Includes support for determining ingestion {@link ChunkType chunk types} and allocating ingestion chunks. These
 * utilities establish "standard" {@link Class data type} to chunk type mappings.
 */
public class StreamChunkUtils {

    /**
     * Return the {@link ChunkType} for a given column index in the specified {@link TableDefinition}.
     *
     * @param tableDefinition the {@link TableDefinition}
     * @param columnIndex the column index to get the {@link ChunkType} for
     * @return the {@link ChunkType} for the specified column
     */
    @NotNull
    public static ChunkType chunkTypeForColumnIndex(
            @NotNull final TableDefinition tableDefinition,
            final int columnIndex) {
        return chunkTypeForColumn(tableDefinition.getColumns().get(columnIndex));
    }

    /**
     * Make output chunks for the specified {@link TableDefinition}.
     *
     * @param definition the {@link TableDefinition} to make chunks for
     * @param size the size of the returned chunks
     * @return an array of writable chunks
     */
    @NotNull
    public static <ATTR extends Any> WritableChunk<ATTR>[] makeChunksForDefinition(
            @NotNull final TableDefinition definition,
            final int size) {
        // noinspection unchecked
        return definition.getColumnStream().map(cd -> makeChunk(cd, size)).toArray(WritableChunk[]::new);
    }

    @NotNull
    private static WritableChunk<?> makeChunk(@NotNull final ColumnDefinition<?> cd, final int size) {
        final ChunkType chunkType = chunkTypeForColumn(cd);
        WritableChunk<Any> returnValue = chunkType.makeWritableChunk(size);
        returnValue.setSize(0);
        return returnValue;
    }

    @NotNull
    private static ChunkType chunkTypeForColumn(@NotNull final ColumnDefinition<?> cd) {
        final Class<?> replacementType = replacementType(cd.getDataType());
        final Class<?> useType = replacementType != null ? replacementType : cd.getDataType();
        return ChunkType.fromElementType(useType);
    }

    /**
     * For storage, we substitute {@code long} and {@code byte} for {@link Instant} and {@link Boolean}, respectively.
     * We expect ingesters to pass us primitive chunks for those types.
     *
     * @param columnDataType the "external" column data type
     * @return the "internal" data type to be supplied
     */
    public static Class<?> replacementType(@NotNull final Class<?> columnDataType) {
        if (columnDataType == Instant.class) {
            return long.class;
        }
        if (columnDataType == Boolean.class) {
            return byte.class;
        }
        return null;
    }
}

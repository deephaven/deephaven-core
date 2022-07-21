/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharacterColumnIterator and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.iterators;

import io.deephaven.base.Procedure;
import io.deephaven.chunk.FloatChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.Table;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;

import java.util.PrimitiveIterator;
import java.util.function.Consumer;

/**
 * {@link ColumnIterator} implementation for {@link ChunkSource chunk sources} of primitive floats.
 */
public final class FloatColumnIterator
        extends ColumnIterator<Float, FloatChunk<? extends Any>>
        implements PrimitiveIterator<Float, Procedure.UnaryFloat> {

    /**
     * Create a new FloatColumnIterator.
     *
     * @param chunkSource The {@link ChunkSource} to fetch values from; must have {@link ChunkSource#getChunkType()
     *        chunk type} of {@link ChunkType#Float}
     * @param rowSequence The {@link RowSequence} to iterate over
     * @param chunkSize The buffer size to use when fetching data
     */
    public FloatColumnIterator(
            @NotNull final ChunkSource<? extends Any> chunkSource,
            @NotNull final RowSequence rowSequence,
            final int chunkSize) {
        super(validateChunkType(chunkSource, ChunkType.Float), rowSequence, chunkSize);
    }

    /**
     * Create a new FloatColumnIterator.
     *
     * @param chunkSource The {@link ChunkSource} to fetch values from; must have {@link ChunkSource#getChunkType()
     *        chunk type} of {@link ChunkType#Float}
     * @param rowSequence The {@link RowSequence} to iterate over
     */
    public FloatColumnIterator(
            @NotNull final ChunkSource<? extends Any> chunkSource,
            @NotNull final RowSequence rowSequence) {
        this(chunkSource, rowSequence, DEFAULT_CHUNK_SIZE);
    }

    /**
     * Create a new FloatColumnIterator.
     *
     * @param table {@link Table} to create the iterator from
     * @param columnName Column name for iteration; must have {@link ChunkSource#getChunkType() chunk type} of
     *        {@link ChunkType#Float}
     */
    public FloatColumnIterator(@NotNull final Table table, @NotNull final String columnName) {
        this(table.getColumnSource(columnName), table.getRowSet(), DEFAULT_CHUNK_SIZE);
    }

    @Override
    FloatChunk<? extends Any> castChunk(@NotNull final Chunk<? extends Any> chunk) {
        return chunk.asFloatChunk();
    }

    public float nextFloat() {
        maybeAdvance();
        return currentData.get(currentOffset++);
    }

    @Override
    public Float next() {
        return TypeUtils.box(nextFloat());
    }

    @Override
    public void forEachRemaining(@NotNull final Procedure.UnaryFloat action) {
        consumeRemainingByChunks(() -> {
            final int currentSize = currentData.size();
            while (currentOffset < currentSize) {
                action.call(currentData.get(currentOffset++));
            }
        });
    }

    @Override
    public void forEachRemaining(@NotNull final Consumer<? super Float> action) {
        consumeRemainingByChunks(() -> {
            final int currentSize = currentData.size();
            while (currentOffset < currentSize) {
                action.accept(TypeUtils.box(currentData.get(currentOffset++)));
            }
        });
    }
}

/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharacterColumnIterator and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.iterators;

import io.deephaven.engine.primitive.function.ShortConsumer;
import io.deephaven.chunk.ShortChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.engine.primitive.function.ShortToIntFunction;
import io.deephaven.engine.primitive.iterator.CloseablePrimitiveIteratorOfShort;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.Table;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;

import java.util.PrimitiveIterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * {@link ColumnIterator} implementation for {@link ChunkSource chunk sources} of primitive shorts.
 */
public final class ShortColumnIterator
        extends ColumnIterator<Short, ShortChunk<? extends Any>>
        implements CloseablePrimitiveIteratorOfShort {

    /**
     * Create a new ShortColumnIterator.
     *
     * @param chunkSource The {@link ChunkSource} to fetch values from; must have {@link ChunkSource#getChunkType()
     *        chunk type} of {@link ChunkType#Short}
     * @param rowSequence The {@link RowSequence} to iterate over
     * @param chunkSize The buffer size to use when fetching data
     */
    public ShortColumnIterator(
            @NotNull final ChunkSource<? extends Any> chunkSource,
            @NotNull final RowSequence rowSequence,
            final int chunkSize) {
        super(validateChunkType(chunkSource, ChunkType.Short), rowSequence, chunkSize);
    }

    /**
     * Create a new ShortColumnIterator.
     *
     * @param chunkSource The {@link ChunkSource} to fetch values from; must have {@link ChunkSource#getChunkType()
     *        chunk type} of {@link ChunkType#Short}
     * @param rowSequence The {@link RowSequence} to iterate over
     */
    public ShortColumnIterator(
            @NotNull final ChunkSource<? extends Any> chunkSource,
            @NotNull final RowSequence rowSequence) {
        this(chunkSource, rowSequence, DEFAULT_CHUNK_SIZE);
    }

    /**
     * Create a new ShortColumnIterator.
     *
     * @param table {@link Table} to create the iterator from
     * @param columnName Column name for iteration; must have {@link ChunkSource#getChunkType() chunk type} of
     *        {@link ChunkType#Short}
     */
    public ShortColumnIterator(@NotNull final Table table, @NotNull final String columnName) {
        this(table.getColumnSource(columnName), table.getRowSet(), DEFAULT_CHUNK_SIZE);
    }

    @Override
    ShortChunk<? extends Any> castChunk(@NotNull final Chunk<? extends Any> chunk) {
        return chunk.asShortChunk();
    }

    public short nextShort() {
        maybeAdvance();
        return currentData.get(currentOffset++);
    }

    @Override
    public Short next() {
        return TypeUtils.box(nextShort());
    }

    @Override
    public void forEachRemaining(@NotNull final ShortConsumer action) {
        consumeRemainingByChunks(() -> {
            final int currentSize = currentData.size();
            while (currentOffset < currentSize) {
                action.accept(currentData.get(currentOffset++));
            }
        });
    }

    @Override
    public void forEachRemaining(@NotNull final Consumer<? super Short> action) {
        consumeRemainingByChunks(() -> {
            final int currentSize = currentData.size();
            while (currentOffset < currentSize) {
                action.accept(TypeUtils.box(currentData.get(currentOffset++)));
            }
        });
    }

    /**
     * Create a {@link IntStream} over the remaining elements of this ShortColumnIterator by applying
     * {@code adapter} to each element. The result <em>must</em> be {@link java.util.stream.BaseStream#close() closed}
     * in order to ensure resources are released. A try-with-resources block is strongly encouraged.
     *
     * @return A {@link IntStream} over the remaining contents of this iterator. Must be {@link Stream#close() closed}.
     */
    @Override
    public IntStream streamAsInt(@NotNull final ShortToIntFunction adapter) {
        final PrimitiveIterator.OfInt adapted = adaptToOfInt(adapter);
        return StreamSupport.intStream(
                Spliterators.spliterator(
                        adapted,
                        size(),
                        Spliterator.IMMUTABLE | Spliterator.ORDERED),
                false)
                .onClose(this::close);
    }

    /**
     * Create a {@link IntStream} over the remaining elements of this ShortColumnIterator by casting each element to
     * {@code int} with the appropriate adjustment of {@link io.deephaven.util.QueryConstants#NULL_SHORT NULL_SHORT} to
     * {@link io.deephaven.util.QueryConstants#NULL_INT NULL_INT}. The result <em>must</em> be
     * {@link java.util.stream.BaseStream#close() closed} in order to ensure resources are released. A
     * try-with-resources block is strongly encouraged.
     *
     * @return A {@link IntStream} over the remaining contents of this iterator. Must be {@link Stream#close() closed}.
     */
    @Override
    public IntStream streamAsInt() {
        return streamAsInt(
                (final short value) -> value == QueryConstants.NULL_SHORT ? QueryConstants.NULL_INT : (int) value);
    }

    /**
     * Create a {@link Stream} over the remaining elements of this ShortColumnIterator. The result <em>must</em> be
     * {@link java.util.stream.BaseStream#close() closed} in order to ensure resources are released. A
     * try-with-resources block is strongly encouraged.
     *
     * @return A {@link IntStream} over the remaining contents of this iterator. Must be {@link Stream#close() closed}.
     */
    @Override
    public Stream<Short> stream() {
        return StreamSupport.stream(
                Spliterators.spliterator(
                        this,
                        size(),
                        Spliterator.IMMUTABLE | Spliterator.ORDERED),
                false)
                .onClose(this::close);
    }
}

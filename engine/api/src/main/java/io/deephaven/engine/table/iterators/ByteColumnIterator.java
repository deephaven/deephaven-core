/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharacterColumnIterator and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.iterators;

import io.deephaven.engine.primitive.function.ByteConsumer;
import io.deephaven.chunk.ByteChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.engine.primitive.function.ByteToIntFunction;
import io.deephaven.engine.primitive.iterator.CloseablePrimitiveIteratorOfByte;
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
 * {@link ColumnIterator} implementation for {@link ChunkSource chunk sources} of primitive bytes.
 */
public final class ByteColumnIterator
        extends ColumnIterator<Byte, ByteChunk<? extends Any>>
        implements CloseablePrimitiveIteratorOfByte {

    /**
     * Create a new ByteColumnIterator.
     *
     * @param chunkSource The {@link ChunkSource} to fetch values from; must have {@link ChunkSource#getChunkType()
     *        chunk type} of {@link ChunkType#Byte}
     * @param rowSequence The {@link RowSequence} to iterate over
     * @param chunkSize The buffer size to use when fetching data
     */
    public ByteColumnIterator(
            @NotNull final ChunkSource<? extends Any> chunkSource,
            @NotNull final RowSequence rowSequence,
            final int chunkSize) {
        super(validateChunkType(chunkSource, ChunkType.Byte), rowSequence, chunkSize);
    }

    /**
     * Create a new ByteColumnIterator.
     *
     * @param chunkSource The {@link ChunkSource} to fetch values from; must have {@link ChunkSource#getChunkType()
     *        chunk type} of {@link ChunkType#Byte}
     * @param rowSequence The {@link RowSequence} to iterate over
     */
    public ByteColumnIterator(
            @NotNull final ChunkSource<? extends Any> chunkSource,
            @NotNull final RowSequence rowSequence) {
        this(chunkSource, rowSequence, DEFAULT_CHUNK_SIZE);
    }

    /**
     * Create a new ByteColumnIterator.
     *
     * @param table {@link Table} to create the iterator from
     * @param columnName Column name for iteration; must have {@link ChunkSource#getChunkType() chunk type} of
     *        {@link ChunkType#Byte}
     */
    public ByteColumnIterator(@NotNull final Table table, @NotNull final String columnName) {
        this(table.getColumnSource(columnName), table.getRowSet(), DEFAULT_CHUNK_SIZE);
    }

    @Override
    ByteChunk<? extends Any> castChunk(@NotNull final Chunk<? extends Any> chunk) {
        return chunk.asByteChunk();
    }

    public byte nextByte() {
        maybeAdvance();
        return currentData.get(currentOffset++);
    }

    @Override
    public Byte next() {
        return TypeUtils.box(nextByte());
    }

    @Override
    public void forEachRemaining(@NotNull final ByteConsumer action) {
        consumeRemainingByChunks(() -> {
            final int currentSize = currentData.size();
            while (currentOffset < currentSize) {
                action.accept(currentData.get(currentOffset++));
            }
        });
    }

    @Override
    public void forEachRemaining(@NotNull final Consumer<? super Byte> action) {
        consumeRemainingByChunks(() -> {
            final int currentSize = currentData.size();
            while (currentOffset < currentSize) {
                action.accept(TypeUtils.box(currentData.get(currentOffset++)));
            }
        });
    }

    /**
     * Create a {@link IntStream} over the remaining elements of this ByteColumnIterator by applying
     * {@code adapter} to each element. The result <em>must</em> be {@link java.util.stream.BaseStream#close() closed}
     * in order to ensure resources are released. A try-with-resources block is strongly encouraged.
     *
     * @return A {@link IntStream} over the remaining contents of this iterator. Must be {@link Stream#close() closed}.
     */
    @Override
    public IntStream streamAsInt(@NotNull final ByteToIntFunction adapter) {
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
     * Create a {@link IntStream} over the remaining elements of this ByteColumnIterator by casting each element to
     * {@code int} with the appropriate adjustment of {@link io.deephaven.util.QueryConstants#NULL_BYTE NULL_BYTE} to
     * {@link io.deephaven.util.QueryConstants#NULL_INT NULL_INT}. The result <em>must</em> be
     * {@link java.util.stream.BaseStream#close() closed} in order to ensure resources are released. A
     * try-with-resources block is strongly encouraged.
     *
     * @return A {@link IntStream} over the remaining contents of this iterator. Must be {@link Stream#close() closed}.
     */
    @Override
    public IntStream streamAsInt() {
        return streamAsInt(
                (final byte value) -> value == QueryConstants.NULL_BYTE ? QueryConstants.NULL_INT : (int) value);
    }

    /**
     * Create a {@link Stream} over the remaining elements of this ByteColumnIterator. The result <em>must</em> be
     * {@link java.util.stream.BaseStream#close() closed} in order to ensure resources are released. A
     * try-with-resources block is strongly encouraged.
     *
     * @return A {@link IntStream} over the remaining contents of this iterator. Must be {@link Stream#close() closed}.
     */
    @Override
    public Stream<Byte> stream() {
        return StreamSupport.stream(
                Spliterators.spliterator(
                        this,
                        size(),
                        Spliterator.IMMUTABLE | Spliterator.ORDERED),
                false)
                .onClose(this::close);
    }
}

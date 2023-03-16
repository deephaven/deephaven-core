/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit IntegerColumnIterator and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.iterators;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.DoubleChunk;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.engine.primitive.iterator.CloseablePrimitiveIteratorOfDouble;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;

import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.function.DoubleConsumer;
import java.util.stream.DoubleStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * {@link ColumnIterator} implementation for {@link ChunkSource chunk sources} of primitive doubles.
 */
public final class DoubleColumnIterator
        extends ColumnIterator<Double, DoubleChunk<? extends Any>>
        implements CloseablePrimitiveIteratorOfDouble {

    /**
     * Create a new DoubleColumnIterator.
     *
     * @param chunkSource The {@link ChunkSource} to fetch values from; must have {@link ChunkSource#getChunkType()
     *        chunk type} of {@link ChunkType#Double}
     * @param rowSequence The {@link RowSequence} to iterate over
     * @param chunkSize The buffer size to use when fetching data
     * @param firstRowKey The first row key from {@code rowSequence} to iterate
     * @param length The total number of rows to iterate
     */
    public DoubleColumnIterator(
            @NotNull final ChunkSource<? extends Any> chunkSource,
            @NotNull final RowSequence rowSequence,
            // @formatter:off
            // region chunkSize
            final int chunkSize,
            // endregion chunkSize
            // @formatter:on
            final long firstRowKey,
            final long length) {
        super(validateChunkType(chunkSource, ChunkType.Double), rowSequence, chunkSize, firstRowKey, length);
    }

    /**
     * Create a new DoubleColumnIterator.
     *
     * @param chunkSource The {@link ChunkSource} to fetch values from; must have {@link ChunkSource#getChunkType()
     *        chunk type} of {@link ChunkType#Double}
     * @param rowSequence The {@link RowSequence} to iterate over
     */
    public DoubleColumnIterator(
            @NotNull final ChunkSource<? extends Any> chunkSource,
            @NotNull final RowSequence rowSequence) {
        this(chunkSource, rowSequence, DEFAULT_CHUNK_SIZE, rowSequence.firstRowKey(), rowSequence.size());
    }

    @Override
    DoubleChunk<? extends Any> castChunk(@NotNull final Chunk<? extends Any> chunk) {
        return chunk.asDoubleChunk();
    }

    @Override
    public double nextDouble() {
        maybeAdvance();
        return currentData.get(currentOffset++);
    }

    @Override
    public Double next() {
        return TypeUtils.box(nextDouble());
    }

    @Override
    public void forEachRemaining(@NotNull final DoubleConsumer action) {
        consumeRemainingByChunks(() -> {
            // region currentSize
            final int currentSize = currentData.size();
            // endregion currentSize
            while (currentOffset < currentSize) {
                action.accept(currentData.get(currentOffset++));
            }
        });
    }

    @Override
    public void forEachRemaining(@NotNull final Consumer<? super Double> action) {
        consumeRemainingByChunks(() -> {
            // region currentSize
            final int currentSize = currentData.size();
            // endregion currentSize
            while (currentOffset < currentSize) {
                action.accept(TypeUtils.box(currentData.get(currentOffset++)));
            }
        });
    }

    /**
     * Create a {@link DoubleStream} over the remaining elements of this DoubleColumnIterator. The result <em>must</em> be
     * {@link java.util.stream.BaseStream#close() closed} in order to ensure resources are released. A
     * try-with-resources block is strongly encouraged.
     *
     * @return A {@link DoubleStream} over the remaining contents of this iterator. Must be {@link Stream#close() closed}.
     */
    @Override
    public DoubleStream doubleStream() {
        return StreamSupport.doubleStream(
                Spliterators.spliterator(
                        this,
                        remaining(),
                        Spliterator.IMMUTABLE | Spliterator.ORDERED),
                false)
                .onClose(this::close);
    }

    /**
     * Create a {@link Stream} over the remaining elements of this DoubleColumnIterator. The result <em>must</em> be
     * {@link java.util.stream.BaseStream#close() closed} in order to ensure resources are released. A
     * try-with-resources block is strongly encouraged.
     *
     * @return A {@link DoubleStream} over the remaining contents of this iterator. Must be {@link Stream#close() closed}.
     */
    @Override
    public Stream<Double> stream() {
        return StreamSupport.stream(
                Spliterators.spliterator(
                        this,
                        remaining(),
                        Spliterator.IMMUTABLE | Spliterator.ORDERED),
                false)
                .onClose(this::close);
    }
}

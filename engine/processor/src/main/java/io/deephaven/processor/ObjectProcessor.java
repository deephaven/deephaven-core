//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.processor;

import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.qst.type.BooleanType;
import io.deephaven.qst.type.BoxedType;
import io.deephaven.qst.type.ByteType;
import io.deephaven.qst.type.CharType;
import io.deephaven.qst.type.DoubleType;
import io.deephaven.qst.type.FloatType;
import io.deephaven.qst.type.GenericType;
import io.deephaven.qst.type.InstantType;
import io.deephaven.qst.type.IntType;
import io.deephaven.qst.type.LongType;
import io.deephaven.qst.type.ShortType;
import io.deephaven.qst.type.Type;

import java.util.List;
import java.util.Set;

/**
 * An interface for processing data from one or more input objects into output chunks on a 1-to-1 input record to output
 * row basis.
 *
 * @param <T> the object type
 */
public interface ObjectProcessor<T> {

    /**
     * Creates or returns an implementation that adds strict safety checks around {@code delegate}
     * {@link #processAll(ObjectChunk, List)}. The may be useful for development or debugging purposes.
     *
     * @param delegate the delegate
     * @return the strict implementation
     * @param <T> the object type
     */
    static <T> ObjectProcessor<T> strict(ObjectProcessor<T> delegate) {
        return ObjectProcessorStrict.create(delegate);
    }

    /**
     * Creates or returns a row-limited implementation. If {@code delegate} has already been limited more than
     * {@code rowLimit}, {@code delegate} is returned. Otherwise, a row-limited implementation is created that wraps
     * {@code delegate} and invokes {@link #processAll(ObjectChunk, List) delegate#processAll} with {@code rowLimit}
     * sized {@code out} chunks, except for the last invocation which may have size less-than {@code rowLimit}.
     *
     * <p>
     * Adding a row-limit may be useful in cases where the input objects are "wide". By limiting the number of rows
     * considered at any given time, there may be better opportunity for read caching.
     *
     * <p>
     * Callers should typically have knowledge of the {@code delegate} implementation; for example, in cases where the
     * {@code delegate} implementation already processes data in a row-oriented fashion, adding a row-limit here
     * introduces extraneous overhead.
     *
     * @param delegate the delegate
     * @param rowLimit the row limit
     * @return the row-limited processor
     * @param <T> the object type
     */
    static <T> ObjectProcessor<T> rowLimited(ObjectProcessor<T> delegate, int rowLimit) {
        return ObjectProcessorRowLimited.of(delegate, rowLimit);
    }

    /**
     * Creates a "no-operation" object processor that ignores the input object chunk. If {@code fillWithNullValue} is
     * {@code true}, during {@link ObjectProcessor#processAll(ObjectChunk, List) processAll}
     * {@link WritableChunk#fillWithNullValue(int, int) fillWithNullValue} will be invoked on each output chunk;
     * otherwise, the output chunk contents will not be modified. In either case, the processing will increment the
     * output chunks sizes.
     *
     * @param outputTypes the output types
     * @param fillWithNullValue if the output chunks should be filled with the appropriate null value
     * @return the no-op object processor
     * @param <T> the object type
     */
    static <T> ObjectProcessor<T> noop(List<Type<?>> outputTypes, boolean fillWithNullValue) {
        return new ObjectProcessorNoop<>(outputTypes, fillWithNullValue);
    }

    /**
     * The relationship between {@link #outputTypes() output types} and the {@link #processAll(ObjectChunk, List)
     * processAll out param} {@link WritableChunk#getChunkType()}.
     *
     * <table>
     * <tr>
     * <th>{@link Type}</th>
     * <th>{@link ChunkType}</th>
     * </tr>
     * <tr>
     * <td>{@link ByteType}</td>
     * <td>{@link ChunkType#Byte}</td>
     * </tr>
     * <tr>
     * <td>{@link ShortType}</td>
     * <td>{@link ChunkType#Short}</td>
     * </tr>
     * <tr>
     * <td>{@link IntType}</td>
     * <td>{@link ChunkType#Int}</td>
     * </tr>
     * <tr>
     * <td>{@link LongType}</td>
     * <td>{@link ChunkType#Long}</td>
     * </tr>
     * <tr>
     * <td>{@link FloatType}</td>
     * <td>{@link ChunkType#Float}</td>
     * </tr>
     * <tr>
     * <td>{@link DoubleType}</td>
     * <td>{@link ChunkType#Double}</td>
     * </tr>
     * <tr>
     * <td>{@link CharType}</td>
     * <td>{@link ChunkType#Char}</td>
     * </tr>
     * <tr>
     * <td>{@link BooleanType}</td>
     * <td>{@link ChunkType#Byte} (<b>not</b> {@link ChunkType#Boolean})</td>
     * </tr>
     * <tr>
     * <td>{@link BoxedType}</td>
     * <td>Same as {@link BoxedType#primitiveType()} would yield.</td>
     * </tr>
     * <tr>
     * <td>{@link InstantType}</td>
     * <td>{@link ChunkType#Long} (io.deephaven.time.DateTimeUtils#epochNanos(Instant))</td>
     * </tr>
     * <tr>
     * <td>All other {@link GenericType}</td>
     * <td>{@link ChunkType#Object}</td>
     * </tr>
     * </table>
     */
    static ChunkType chunkType(Type<?> type) {
        return ObjectProcessorTypes.of(type);
    }

    /**
     * The number of outputs. Equivalent to {@code outputTypes().size()}.
     *
     * @return the number of outputs
     */
    default int outputSize() {
        return outputTypes().size();
    }

    /**
     * The logical output types {@code this} instance processes. The size and types correspond to the expected size and
     * {@link io.deephaven.chunk.ChunkType chunk types} for {@link #processAll(ObjectChunk, List)} as specified by
     * {@link #chunkType(Type)}.
     *
     * @return the output types
     */
    List<Type<?>> outputTypes();

    /**
     * Processes {@code in} into {@code out} by appending {@code in.size()} values to each chunk. The size of each
     * {@code out} chunk will be incremented by {@code in.size()}. Implementations are free to process the data in a
     * row-oriented, column-oriented, or mix-oriented fashion. {@code in}, {@code out}, and the elements of {@code out}
     * are owned by the caller; implementations should not keep references to these. Implementations may copy references
     * from {@code in} into the elements of {@code out}; for example, if the input type {@code T} is {@code byte[]},
     * implementations may copy that reference into {@code out}. In general, implementations should document the
     * references they may copy from {@code in} into {@code out}.
     *
     * <p>
     * If an exception is thrown the output chunks will be in an unspecified state for all rows after their initial
     * size.
     *
     * @param in the input objects
     * @param out the output chunks as specified by {@link #outputTypes()}; each chunk must have remaining capacity of
     *        at least {@code in.size()}
     */
    void processAll(ObjectChunk<? extends T, ?> in, List<WritableChunk<?>> out);

    /**
     * An abstraction over {@link ObjectProcessor} that provides the same logical object processor for different input
     * types.
     */
    interface Provider {

        /**
         * The supported input types for {@link #processor(Type)}.
         *
         * @return the supported input types
         */
        Set<Type<?>> inputTypes();

        /**
         * The output types for the processors. Equivalent to the processors' {@link ObjectProcessor#outputTypes()}.
         *
         * @return the output types
         */
        List<Type<?>> outputTypes();

        /**
         * The number of output types for the processors. Equivalent to the processors'
         * {@link ObjectProcessor#outputSize()}.
         *
         * @return the number of output types
         */
        int outputSize();

        /**
         * Creates an object processor that can process the {@code inputType}. This will successfully create a processor
         * when {@code inputType} is one of, or extends from one of, {@link #inputTypes()}. Otherwise, an
         * {@link IllegalArgumentException} will be thrown.
         *
         * @param inputType the input type
         * @return the object processor
         * @param <T> the input type
         */
        <T> ObjectProcessor<? super T> processor(Type<T> inputType);
    }
}

/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.processor;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;

import java.util.List;

/**
 * A specialization that {@link #processAll(ObjectChunk, List) processes all} one row at a time.
 *
 * <p>
 * In particular, this is a useful construct when {@code T} is a {@link java.nio.ByteBuffer} or {@code byte[]} type and
 * it makes sense to parse straight from bytes into output chunks (as opposed to parsing into a more intermediate state
 * and then processing into chunks in a column-oriented fashion).
 *
 * @param <T> the object type
 */
public abstract class ObjectProcessorRowBase<T> implements ObjectProcessorRowLimited<T> {

    @Override
    public final int rowLimit() {
        return 1;
    }

    /**
     * Implementations are responsible for parsing {@code in} into {@code out} <b>without</b> increasing the output
     * chunk size; as such implementations are responsible for setting the cell at position {@code chunk.size() + i} for
     * each output {@code chunk}. Implementations must not keep any references to the passed-in chunks.
     *
     * @param in in the input object
     * @param out the output chunks as specified by {@link #outputTypes()}
     * @param i the cell offset relative to each output {@code chunk.size()}
     */
    protected abstract void parse(T in, List<WritableChunk<?>> out, int i);

    @Override
    public final void processAll(ObjectChunk<? extends T, ?> in, List<WritableChunk<?>> out) {
        final int inSize = in.size();
        for (int i = 0; i < inSize; ++i) {
            parse(in.get(i), out, i);
        }
        for (WritableChunk<?> outChunk : out) {
            outChunk.setSize(outChunk.size() + inSize);
        }
    }
}

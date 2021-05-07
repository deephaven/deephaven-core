/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.sources;

import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.sources.chunk.Attributes.Values;
import io.deephaven.db.v2.utils.OrderedKeys;
import org.jetbrains.annotations.NotNull;

 public interface WritableSource<T> extends ColumnSource<T>, WritableChunkSink<Attributes.Values> {
    default void set(long key, T value) { throw new UnsupportedOperationException(); }

    default void set(long key, byte value) { throw new UnsupportedOperationException(); }

    default void set(long key, char value) { throw new UnsupportedOperationException(); }

    default void set(long key, double value) { throw new UnsupportedOperationException(); }

    default void set(long key, float value) { throw new UnsupportedOperationException(); }

    default void set(long key, int value) { throw new UnsupportedOperationException(); }

    default void set(long key, long value) { throw new UnsupportedOperationException(); }

    default void set(long key, short value) { throw new UnsupportedOperationException(); }

    void copy(ColumnSource<T> sourceColumn, long sourceKey, long destKey);

    void ensureCapacity(long capacity);

    // WritableSource provides a slow, default implementation of fillFromChunk. Inheritors who care should provide
    // something more efficient.

    /**
     * Provide a default, empty {@link FillFromContext} for use with our default {@link WritableSource#fillFromChunk}.
     */
    @Override
    default FillFromContext makeFillFromContext(int chunkCapacity) {
        // chunkCapacity ignored
        return SinkFiller.create(getChunkType());
    }

    /**
     * Our default, inefficient, implementation. Inheritors who care should provide a better implementation.
     */
    @Override
    default void fillFromChunk(@NotNull FillFromContext context, @NotNull Chunk<? extends Values> src,
                               @NotNull OrderedKeys orderedKeys) {
        final SinkFiller filler = (SinkFiller) context;
        filler.reset(this, src);
        orderedKeys.forEachLong(filler);
    }

    @Override
    default void fillFromChunkUnordered(@NotNull FillFromContext context, @NotNull Chunk<? extends Values> src, @NotNull LongChunk<Attributes.KeyIndices> keys) {
        final SinkFiller filler = (SinkFiller) context;
        filler.reset(this, src);
        for (int ii = 0; ii < keys.size(); ++ii) {
            filler.accept(keys.get(ii));
        }
    }
}

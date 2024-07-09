//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table;

import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.util.annotations.FinalDefault;
import io.deephaven.util.datastructures.LongAbortableConsumer;
import org.jetbrains.annotations.NotNull;

public interface WritableColumnSource<T> extends ColumnSource<T>, ChunkSink<Values> {

    default void set(long key, T value) {
        throw new UnsupportedOperationException();
    }

    default void set(long key, byte value) {
        throw new UnsupportedOperationException();
    }

    default void set(long key, char value) {
        throw new UnsupportedOperationException();
    }

    default void set(long key, double value) {
        throw new UnsupportedOperationException();
    }

    default void set(long key, float value) {
        throw new UnsupportedOperationException();
    }

    default void set(long key, int value) {
        throw new UnsupportedOperationException();
    }

    default void set(long key, long value) {
        throw new UnsupportedOperationException();
    }

    default void set(long key, short value) {
        throw new UnsupportedOperationException();
    }

    void setNull(long key);

    default void setNull(RowSequence orderedKeys) {
        orderedKeys.forAllRowKeys(this::setNull);
    }

    /**
     * Equivalent to {@code ensureCapacity(capacity, true)}.
     */
    @FinalDefault
    default void ensureCapacity(long capacity) {
        ensureCapacity(capacity, true);
    }

    /**
     * Ensure that this WritableColumnSource can accept row keys in range {@code [0, capacity)}.
     *
     * @param capacity The new minimum capacity
     * @param nullFilled Whether data should be "null-filled". If true, get operations at row keys that have not been
     *        set will return the appropriate null value; otherwise such gets produce undefined results.
     */
    void ensureCapacity(long capacity, boolean nullFilled);

    // WritableColumnSource provides a slow, default implementation of fillFromChunk. Inheritors who care should provide
    // something more efficient.

    /**
     * Provide a default, empty {@link FillFromContext} for use with our default
     * {@link WritableColumnSource#fillFromChunk}.
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
            @NotNull RowSequence rowSequence) {
        final SinkFiller filler = (SinkFiller) context;
        filler.reset(this, src);
        rowSequence.forEachRowKey(filler);
    }

    @Override
    default void fillFromChunkUnordered(@NotNull FillFromContext context, @NotNull Chunk<? extends Values> src,
            @NotNull LongChunk<RowKeys> keys) {
        final SinkFiller filler = (SinkFiller) context;
        filler.reset(this, src);
        for (int ii = 0; ii < keys.size(); ++ii) {
            filler.accept(keys.get(ii));
        }
    }

    // region Sink Fillers

    abstract class SinkFiller implements ChunkSink.FillFromContext, LongAbortableConsumer {

        public static SinkFiller create(final ChunkType chunkType) {
            switch (chunkType) {
                case Byte:
                    return new ByteFiller();
                case Char:
                    return new CharFiller();
                case Double:
                    return new DoubleFiller();
                case Float:
                    return new FloatFiller();
                case Int:
                    return new IntFiller();
                case Long:
                    return new LongFiller();
                case Short:
                    return new ShortFiller();
                case Object:
                    return new ObjectFiller();

                // Boolean Chunks will be passing in chunkType = Object, so there is no use case for passing in
                // ChunkType.Boolean.
                case Boolean:
                default:
                    throw new UnsupportedOperationException("Unexpected chunkType " + chunkType);
            }
        }

        WritableColumnSource dest;
        int srcIndex;

        final void reset(WritableColumnSource dest, Chunk<? extends Values> src) {
            this.dest = dest;
            srcIndex = 0;
            resetSrc(src);
        }

        abstract void resetSrc(Chunk<? extends Values> src);
    }


    class ByteFiller extends SinkFiller {
        private ByteChunk<? extends Values> typedSrc;

        @Override
        final void resetSrc(Chunk<? extends Values> src) {
            typedSrc = src.asByteChunk();
        }

        @Override
        public final boolean accept(long v) {
            dest.set(v, typedSrc.get(srcIndex++));
            return true;
        }
    }


    class CharFiller extends SinkFiller {
        private CharChunk<? extends Values> typedSrc;

        @Override
        final void resetSrc(Chunk<? extends Values> src) {
            typedSrc = src.asCharChunk();
        }

        @Override
        public final boolean accept(long v) {
            dest.set(v, typedSrc.get(srcIndex++));
            return true;
        }
    }


    class DoubleFiller extends SinkFiller {
        private DoubleChunk<? extends Values> typedSrc;

        @Override
        final void resetSrc(Chunk<? extends Values> src) {
            typedSrc = src.asDoubleChunk();
        }

        @Override
        public final boolean accept(long v) {
            dest.set(v, typedSrc.get(srcIndex++));
            return true;
        }
    }


    class FloatFiller extends SinkFiller {
        private FloatChunk<? extends Values> typedSrc;

        @Override
        final void resetSrc(Chunk<? extends Values> src) {
            typedSrc = src.asFloatChunk();
        }

        @Override
        public final boolean accept(long v) {
            dest.set(v, typedSrc.get(srcIndex++));
            return true;
        }
    }


    class IntFiller extends SinkFiller {
        private IntChunk<? extends Values> typedSrc;

        @Override
        final void resetSrc(Chunk<? extends Values> src) {
            typedSrc = src.asIntChunk();
        }

        @Override
        public final boolean accept(long v) {
            dest.set(v, typedSrc.get(srcIndex++));
            return true;
        }
    }


    class LongFiller extends SinkFiller {
        private LongChunk<? extends Values> typedSrc;

        @Override
        final void resetSrc(Chunk<? extends Values> src) {
            typedSrc = src.asLongChunk();
        }

        @Override
        public final boolean accept(long v) {
            dest.set(v, typedSrc.get(srcIndex++));
            return true;
        }
    }


    class ShortFiller extends SinkFiller {
        private ShortChunk<? extends Values> typedSrc;

        @Override
        final void resetSrc(Chunk<? extends Values> src) {
            typedSrc = src.asShortChunk();
        }

        @Override
        public final boolean accept(long v) {
            dest.set(v, typedSrc.get(srcIndex++));
            return true;
        }
    }


    class ObjectFiller extends SinkFiller {
        private ObjectChunk<?, ? extends Values> typedSrc;

        @Override
        final void resetSrc(Chunk<? extends Values> src) {
            typedSrc = src.asObjectChunk();
        }

        @Override
        public final boolean accept(long v) {
            // noinspection unchecked
            dest.set(v, typedSrc.get(srcIndex++));
            return true;
        }
    }

    // endregion Sink Fillers
}

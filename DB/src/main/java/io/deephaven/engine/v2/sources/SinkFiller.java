package io.deephaven.engine.v2.sources;

import io.deephaven.engine.structures.chunk.*;
import io.deephaven.engine.structures.iterationhelpers.LongAbortableConsumer;

/**
 * Helper for {@link ChunkSink} implementations.
 */
public abstract class SinkFiller implements ChunkSink.FillFromContext, LongAbortableConsumer {

    public static SinkFiller create(final ChunkType chunkType) {
        switch (chunkType) {
            case Byte:
                return ByteFiller.INSTANCE;
            case Char:
                return CharFiller.INSTANCE;
            case Double:
                return DoubleFiller.INSTANCE;
            case Float:
                return FloatFiller.INSTANCE;
            case Int:
                return IntFiller.INSTANCE;
            case Long:
                return LongFiller.INSTANCE;
            case Short:
                return ShortFiller.INSTANCE;
            case Object:
                return ObjectFiller.INSTANCE;

            // Boolean Chunks will be passing in chunkType = Object, so there is no use case for passing in
            // ChunkType.Boolean.
            case Boolean:
            default:
                throw new UnsupportedOperationException("Unexpected chunkType " + chunkType);
        }
    }

    WritableSource dest;
    int srcIndex;

    final void reset(WritableSource dest, Chunk<? extends Attributes.Values> src) {
        this.dest = dest;
        srcIndex = 0;
        resetSrc(src);
    }

    abstract void resetSrc(Chunk<? extends Attributes.Values> src);

    static final class ByteFiller extends SinkFiller {
        static final ByteFiller INSTANCE = new ByteFiller();

        private ByteChunk<? extends Attributes.Values> typedSrc;

        @Override
        final void resetSrc(Chunk<? extends Attributes.Values> src) {
            typedSrc = src.asByteChunk();
        }

        @Override
        public final boolean accept(long v) {
            dest.set(v, typedSrc.get(srcIndex++));
            return true;
        }
    }

    static final class CharFiller extends SinkFiller {
        static final CharFiller INSTANCE = new CharFiller();

        private CharChunk<? extends Attributes.Values> typedSrc;

        @Override
        final void resetSrc(Chunk<? extends Attributes.Values> src) {
            typedSrc = src.asCharChunk();
        }

        @Override
        public final boolean accept(long v) {
            dest.set(v, typedSrc.get(srcIndex++));
            return true;
        }
    }

    static final class DoubleFiller extends SinkFiller {
        static final DoubleFiller INSTANCE = new DoubleFiller();

        private DoubleChunk<? extends Attributes.Values> typedSrc;

        @Override
        final void resetSrc(Chunk<? extends Attributes.Values> src) {
            typedSrc = src.asDoubleChunk();
        }

        @Override
        public final boolean accept(long v) {
            dest.set(v, typedSrc.get(srcIndex++));
            return true;
        }
    }

    static final class FloatFiller extends SinkFiller {
        static final FloatFiller INSTANCE = new FloatFiller();

        private FloatChunk<? extends Attributes.Values> typedSrc;

        @Override
        final void resetSrc(Chunk<? extends Attributes.Values> src) {
            typedSrc = src.asFloatChunk();
        }

        @Override
        public final boolean accept(long v) {
            dest.set(v, typedSrc.get(srcIndex++));
            return true;
        }
    }

    static final class IntFiller extends SinkFiller {
        static final IntFiller INSTANCE = new IntFiller();

        private IntChunk<? extends Attributes.Values> typedSrc;

        @Override
        final void resetSrc(Chunk<? extends Attributes.Values> src) {
            typedSrc = src.asIntChunk();
        }

        @Override
        public final boolean accept(long v) {
            dest.set(v, typedSrc.get(srcIndex++));
            return true;
        }
    }

    static final class LongFiller extends SinkFiller {
        static final LongFiller INSTANCE = new LongFiller();

        private LongChunk<? extends Attributes.Values> typedSrc;

        @Override
        final void resetSrc(Chunk<? extends Attributes.Values> src) {
            typedSrc = src.asLongChunk();
        }

        @Override
        public final boolean accept(long v) {
            dest.set(v, typedSrc.get(srcIndex++));
            return true;
        }
    }

    static final class ShortFiller extends SinkFiller {
        static final ShortFiller INSTANCE = new ShortFiller();

        private ShortChunk<? extends Attributes.Values> typedSrc;

        @Override
        final void resetSrc(Chunk<? extends Attributes.Values> src) {
            typedSrc = src.asShortChunk();
        }

        @Override
        public final boolean accept(long v) {
            dest.set(v, typedSrc.get(srcIndex++));
            return true;
        }
    }

    static final class ObjectFiller extends SinkFiller {
        static final ObjectFiller INSTANCE = new ObjectFiller();

        private ObjectChunk<?, ? extends Attributes.Values> typedSrc;

        @Override
        final void resetSrc(Chunk<? extends Attributes.Values> src) {
            typedSrc = src.asObjectChunk();
        }

        @Override
        public final boolean accept(long v) {
            // noinspection unchecked
            dest.set(v, typedSrc.get(srcIndex++));
            return true;
        }
    }
}

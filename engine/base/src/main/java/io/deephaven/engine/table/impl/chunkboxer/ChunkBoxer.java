package io.deephaven.engine.table.impl.chunkboxer;

import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.Context;
import io.deephaven.util.type.TypeUtils;

/**
 * Convert an arbitrary chunk to a chunk of boxed objects.
 */
public class ChunkBoxer {

    /**
     * Return a chunk that contains boxed Objects representing the primitive values in primitives.
     */
    public interface BoxerKernel extends Context {
        /**
         * Convert all primitives to an object.
         *
         * @param primitives the primitives to convert
         *
         * @return a chunk containing primitives as an object
         */
        ObjectChunk<?, ? extends Values> box(Chunk<? extends Values> primitives);
    }

    public static BoxerKernel getBoxer(ChunkType type, int capacity) {
        switch (type) {
            case Boolean:
                return new BooleanBoxer(capacity);
            case Char:
                return new CharBoxer(capacity);
            case Byte:
                return new ByteBoxer(capacity);
            case Short:
                return new ShortBoxer(capacity);
            case Int:
                return new IntBoxer(capacity);
            case Long:
                return new LongBoxer(capacity);
            case Float:
                return new FloatBoxer(capacity);
            case Double:
                return new DoubleBoxer(capacity);
            case Object:
                return OBJECT_BOXER;
        }
        throw new IllegalArgumentException("Unknown type: " + type);
    }

    private static final ObjectBoxer OBJECT_BOXER = new ObjectBoxer();

    private static class ObjectBoxer implements BoxerKernel {
        @Override
        public ObjectChunk<?, ? extends Values> box(Chunk<? extends Values> primitives) {
            return primitives.asObjectChunk();
        }
    }

    private static abstract class BoxerCommon implements ChunkBoxer.BoxerKernel {
        final WritableObjectChunk<Object, Values> objectChunk;

        private BoxerCommon(int capacity) {
            objectChunk = WritableObjectChunk.makeWritableChunk(capacity);
        }

        @Override
        public void close() {
            objectChunk.close();
        }
    }

    private static class BooleanBoxer extends BoxerCommon {
        BooleanBoxer(int capacity) {
            super(capacity);
        }

        @Override
        public ObjectChunk<?, ? extends Values> box(Chunk<? extends Values> primitives) {
            final BooleanChunk<? extends Values> booleanChunk = primitives.asBooleanChunk();
            for (int ii = 0; ii < primitives.size(); ++ii) {
                // noinspection UnnecessaryBoxing
                objectChunk.set(ii, Boolean.valueOf(booleanChunk.get(ii)));
            }
            objectChunk.setSize(primitives.size());
            return objectChunk;
        }
    }

    private static class CharBoxer extends BoxerCommon {
        CharBoxer(int capacity) {
            super(capacity);
        }

        @Override
        public ObjectChunk<?, ? extends Values> box(Chunk<? extends Values> primitives) {
            final CharChunk<? extends Values> charChunk = primitives.asCharChunk();
            for (int ii = 0; ii < primitives.size(); ++ii) {
                objectChunk.set(ii, io.deephaven.util.type.TypeUtils.box(charChunk.get(ii)));
            }
            objectChunk.setSize(primitives.size());
            return objectChunk;
        }
    }

    private static class ByteBoxer extends BoxerCommon {
        ByteBoxer(int capacity) {
            super(capacity);
        }

        @Override
        public ObjectChunk<?, ? extends Values> box(Chunk<? extends Values> primitives) {
            final ByteChunk<? extends Values> byteChunk = primitives.asByteChunk();
            for (int ii = 0; ii < primitives.size(); ++ii) {
                objectChunk.set(ii, io.deephaven.util.type.TypeUtils.box(byteChunk.get(ii)));
            }
            objectChunk.setSize(primitives.size());
            return objectChunk;
        }
    }

    private static class ShortBoxer extends BoxerCommon {
        ShortBoxer(int capacity) {
            super(capacity);
        }

        @Override
        public ObjectChunk<?, ? extends Values> box(Chunk<? extends Values> primitives) {
            final ShortChunk<? extends Values> shortChunk = primitives.asShortChunk();
            for (int ii = 0; ii < primitives.size(); ++ii) {
                objectChunk.set(ii, io.deephaven.util.type.TypeUtils.box(shortChunk.get(ii)));
            }
            objectChunk.setSize(primitives.size());
            return objectChunk;
        }
    }

    private static class IntBoxer extends BoxerCommon {
        IntBoxer(int capacity) {
            super(capacity);
        }

        @Override
        public ObjectChunk<?, ? extends Values> box(Chunk<? extends Values> primitives) {
            final IntChunk<? extends Values> intChunk = primitives.asIntChunk();
            for (int ii = 0; ii < primitives.size(); ++ii) {
                objectChunk.set(ii, io.deephaven.util.type.TypeUtils.box(intChunk.get(ii)));
            }
            objectChunk.setSize(primitives.size());
            return objectChunk;
        }
    }

    private static class LongBoxer extends BoxerCommon {
        LongBoxer(int capacity) {
            super(capacity);
        }

        @Override
        public ObjectChunk<?, ? extends Values> box(Chunk<? extends Values> primitives) {
            final LongChunk<? extends Values> longChunk = primitives.asLongChunk();
            for (int ii = 0; ii < primitives.size(); ++ii) {
                objectChunk.set(ii, io.deephaven.util.type.TypeUtils.box(longChunk.get(ii)));
            }
            objectChunk.setSize(primitives.size());
            return objectChunk;
        }
    }

    private static class FloatBoxer extends BoxerCommon {
        FloatBoxer(int capacity) {
            super(capacity);
        }

        @Override
        public ObjectChunk<?, ? extends Values> box(Chunk<? extends Values> primitives) {
            final FloatChunk<? extends Values> floatChunk = primitives.asFloatChunk();
            for (int ii = 0; ii < primitives.size(); ++ii) {
                objectChunk.set(ii, io.deephaven.util.type.TypeUtils.box(floatChunk.get(ii)));
            }
            objectChunk.setSize(primitives.size());
            return objectChunk;
        }
    }

    private static class DoubleBoxer extends BoxerCommon {
        DoubleBoxer(int capacity) {
            super(capacity);
        }

        @Override
        public ObjectChunk<?, ? extends Values> box(Chunk<? extends Values> primitives) {
            final DoubleChunk<? extends Values> doubleChunk = primitives.asDoubleChunk();
            for (int ii = 0; ii < primitives.size(); ++ii) {
                objectChunk.set(ii, TypeUtils.box(doubleChunk.get(ii)));
            }
            objectChunk.setSize(primitives.size());
            return objectChunk;
        }
    }
}

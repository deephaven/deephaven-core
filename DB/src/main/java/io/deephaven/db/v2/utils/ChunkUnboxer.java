package io.deephaven.db.v2.utils;

import io.deephaven.db.v2.sources.chunk.Attributes.Values;
import io.deephaven.db.v2.sources.chunk.*;

/**
 * Convert an Object chunk to a chunk of primitives.
 */
public class ChunkUnboxer {
    static final CharUnboxer CHAR_EMPTY_INSTANCE = new CharUnboxer(0);
    static final ByteUnboxer BYTE_EMPTY_INSTANCE = new ByteUnboxer(0);
    static final ShortUnboxer SHORT_EMPTY_INSTANCE = new ShortUnboxer(0);
    static final IntUnboxer INT_EMPTY_INSTANCE = new IntUnboxer(0);
    static final LongUnboxer LONG_EMPTY_INSTANCE = new LongUnboxer(0);
    static final FloatUnboxer FLOAT_EMPTY_INSTANCE = new FloatUnboxer(0);
    static final DoubleUnboxer DOUBLE_EMPTY_INSTANCE = new DoubleUnboxer(0);
    static final BooleanUnboxer BOOLEAN_EMPTY_INSTANCE = new BooleanUnboxer(0);

    /**
     * Return a chunk that contains boxed Objects representing the primitive values in primitives.
     */
    public interface UnboxerKernel extends Context {
        /**
         * Convert all boxed primitives to real primitives.
         *
         * @param boxedPrimitives the boxed primitives to convert
         *
         * @return a chunk containing primitives
         */
        Chunk<? extends Values> unbox(ObjectChunk<?, ? extends Values> boxedPrimitives);

        void unboxTo(ObjectChunk<?, ? extends Values> boxedPrimitives, WritableChunk<? extends Values> primitives, int sourceOffset, int destOffset);
    }

    public static UnboxerKernel getUnboxer(ChunkType type, int capacity) {
        if (capacity == 0) {
            return getEmptyUnboxer(type);
        }
        switch (type) {
            case Boolean:
                return new BooleanUnboxer(capacity);
            case Char:
                return new CharUnboxer(capacity);
            case Byte:
                return new ByteUnboxer(capacity);
            case Short:
                return new ShortUnboxer(capacity);
            case Int:
                return new IntUnboxer(capacity);
            case Long:
                return new LongUnboxer(capacity);
            case Float:
                return new FloatUnboxer(capacity);
            case Double:
                return new DoubleUnboxer(capacity);
            case Object:
                throw new IllegalArgumentException("Can not unbox objects");
        }
        throw new IllegalArgumentException("Unknown type: " + type);
    }

    public static UnboxerKernel getEmptyUnboxer(ChunkType type) {
        switch (type) {
            case Boolean:
                return BOOLEAN_EMPTY_INSTANCE;
            case Char:
                return CHAR_EMPTY_INSTANCE;
            case Byte:
                return BYTE_EMPTY_INSTANCE;
            case Short:
                return SHORT_EMPTY_INSTANCE;
            case Int:
                return INT_EMPTY_INSTANCE;
            case Long:
                return LONG_EMPTY_INSTANCE;
            case Float:
                return FLOAT_EMPTY_INSTANCE;
            case Double:
                return DOUBLE_EMPTY_INSTANCE;
            case Object:
                throw new IllegalArgumentException("Can not unbox objects");
        }
        throw new IllegalArgumentException("Unknown type: " + type);
    }

    private static class BooleanUnboxer implements UnboxerKernel {
        final WritableBooleanChunk<Values> primitiveChunk;

        BooleanUnboxer(int capacity) {
            primitiveChunk = WritableBooleanChunk.makeWritableChunk(capacity);
        }

        @Override
        public void close() {
            primitiveChunk.close();
        }

        @Override
        public BooleanChunk<? extends Values> unbox(ObjectChunk<?, ? extends Values> boxed) {
            unboxTo(boxed, primitiveChunk, 0, 0);
            primitiveChunk.setSize(boxed.size());
            return primitiveChunk;
        }

        public void unboxTo(ObjectChunk<?, ? extends Values> boxed, WritableChunk<? extends Values> primitives, int sourceOffset, int destOffset) {
            unboxTo(boxed, primitives.asWritableBooleanChunk(), sourceOffset, destOffset);
        }

        public void unboxTo(ObjectChunk<?, ? extends Values> boxed, WritableBooleanChunk<? extends Values> primitives, int sourceOffset, int destOffset) {
            final ObjectChunk<Boolean, ? extends Values> booleanChunk = boxed.asObjectChunk();
            for (int ii = 0; ii < boxed.size(); ++ii) {
                primitives.set(destOffset + ii, booleanChunk.get(ii + sourceOffset));
            }
        }
    }

    private static class CharUnboxer implements UnboxerKernel {
        final WritableCharChunk<Values> primitiveChunk;

        CharUnboxer(int capacity) {
            primitiveChunk = WritableCharChunk.makeWritableChunk(capacity);
        }

        @Override
        public void close() {
            primitiveChunk.close();
        }

        @Override
        public CharChunk<? extends Values> unbox(ObjectChunk<?, ? extends Values> boxed) {
            unboxTo(boxed, primitiveChunk, 0, 0);
            primitiveChunk.setSize(boxed.size());
            return primitiveChunk;
        }

        public void unboxTo(ObjectChunk<?, ? extends Values> boxed, WritableChunk<? extends Values> primitives, int sourceOffset, int destOffset) {
            unboxTo(boxed, primitives.asWritableCharChunk(), sourceOffset, destOffset);
        }

        public void unboxTo(ObjectChunk<?, ? extends Values> boxed, WritableCharChunk<? extends Values> primitives, int sourceOffset, int destOffset) {
            final ObjectChunk<Character, ? extends Values> charChunk = boxed.asObjectChunk();
            for (int ii = 0; ii < boxed.size(); ++ii) {
                primitives.set(ii + destOffset, charChunk.get(ii + sourceOffset));
            }
        }
    }

    private static class ByteUnboxer implements UnboxerKernel {
        final WritableByteChunk<Values> primitiveChunk;

        ByteUnboxer(int capacity) {
            primitiveChunk = WritableByteChunk.makeWritableChunk(capacity);
        }

        @Override
        public void close() {
            primitiveChunk.close();
        }

        @Override
        public ByteChunk<? extends Values> unbox(ObjectChunk<?, ? extends Values> boxed) {
            unboxTo(boxed, primitiveChunk, 0, 0);
            primitiveChunk.setSize(boxed.size());
            return primitiveChunk;
        }

        public void unboxTo(ObjectChunk<?, ? extends Values> boxed, WritableChunk<? extends Values> primitives, int sourceOffset, int destOffset) {
            unboxTo(boxed, primitives.asWritableByteChunk(), sourceOffset, destOffset);
        }

        public void unboxTo(ObjectChunk<?, ? extends Values> boxed, WritableByteChunk<? extends Values> primitives, int sourceOffset, int destOffset) {
            final ObjectChunk<Byte, ? extends Values> byteChunk = boxed.asObjectChunk();
            for (int ii = 0; ii < boxed.size(); ++ii) {
                primitives.set(ii + destOffset, byteChunk.get(ii + sourceOffset));
            }
        }
    }

    private static class ShortUnboxer implements UnboxerKernel {
        final WritableShortChunk<Values> primitiveChunk;

        ShortUnboxer(int capacity) {
            primitiveChunk = WritableShortChunk.makeWritableChunk(capacity);
        }

        @Override
        public void close() {
            primitiveChunk.close();
        }

        @Override
        public ShortChunk<? extends Values> unbox(ObjectChunk<?, ? extends Values> boxed) {
            unboxTo(boxed, primitiveChunk, 0, 0);
            primitiveChunk.setSize(boxed.size());
            return primitiveChunk;
        }

        public void unboxTo(ObjectChunk<?, ? extends Values> boxed, WritableChunk<? extends Values> primitives, int sourceOffset, int destOffset) {
            unboxTo(boxed, primitives.asWritableShortChunk(), sourceOffset, destOffset);
        }

        public void unboxTo(ObjectChunk<?, ? extends Values> boxed, WritableShortChunk<? extends Values> primitives, int sourceOffset, int destOffset) {
            final ObjectChunk<Short, ? extends Values> shortChunk = boxed.asObjectChunk();
            for (int ii = 0; ii < boxed.size(); ++ii) {
                primitives.set(ii + destOffset, shortChunk.get(ii + sourceOffset));
            }
        }
    }

    private static class IntUnboxer implements UnboxerKernel {
        final WritableIntChunk<Values> primitiveChunk;

        IntUnboxer(int capacity) {
            primitiveChunk = WritableIntChunk.makeWritableChunk(capacity);
        }

        @Override
        public void close() {
            primitiveChunk.close();
        }

        @Override
        public IntChunk<? extends Values> unbox(ObjectChunk<?, ? extends Values> boxed) {
            unboxTo(boxed, primitiveChunk, 0, 0);
            primitiveChunk.setSize(boxed.size());
            return primitiveChunk;
        }

        public void unboxTo(ObjectChunk<?, ? extends Values> boxed, WritableChunk<? extends Values> primitives, int sourceOffset, int destOffset) {
            unboxTo(boxed, primitives.asWritableIntChunk(), sourceOffset, destOffset);
        }

        public void unboxTo(ObjectChunk<?, ? extends Values> boxed, WritableIntChunk<? extends Values> primitives, int sourceOffset, int destOffset) {
            final ObjectChunk<Integer, ? extends Values> intChunk = boxed.asObjectChunk();
            for (int ii = 0; ii < boxed.size(); ++ii) {
                primitives.set(ii + destOffset, intChunk.get(ii + sourceOffset));
            }
        }
    }

    private static class LongUnboxer implements UnboxerKernel {
        final WritableLongChunk<Values> primitiveChunk;

        LongUnboxer(int capacity) {
            primitiveChunk = WritableLongChunk.makeWritableChunk(capacity);
        }

        @Override
        public void close() {
            primitiveChunk.close();
        }

        @Override
        public LongChunk<? extends Values> unbox(ObjectChunk<?, ? extends Values> boxed) {
            unboxTo(boxed, primitiveChunk, 0, 0);
            primitiveChunk.setSize(boxed.size());
            return primitiveChunk;
        }

        public void unboxTo(ObjectChunk<?, ? extends Values> boxed, WritableChunk<? extends Values> primitives, int sourceOffset, int destOffset) {
            unboxTo(boxed, primitives.asWritableLongChunk(), sourceOffset, destOffset);
        }

        public void unboxTo(ObjectChunk<?, ? extends Values> boxed, WritableLongChunk<? extends Values> primitives, int sourceOffset, int destOffset) {
            final ObjectChunk<Long, ? extends Values> longChunk = boxed.asObjectChunk();
            for (int ii = 0; ii < boxed.size(); ++ii) {
                primitives.set(ii + destOffset, longChunk.get(ii + sourceOffset));
            }
        }
    }

    private static class FloatUnboxer implements UnboxerKernel {
        final WritableFloatChunk<Values> primitiveChunk;

        FloatUnboxer(int capacity) {
            primitiveChunk = WritableFloatChunk.makeWritableChunk(capacity);
        }

        @Override
        public void close() {
            primitiveChunk.close();
        }

        @Override
        public FloatChunk<? extends Values> unbox(ObjectChunk<?, ? extends Values> boxed) {
            unboxTo(boxed, primitiveChunk, 0, 0);
            primitiveChunk.setSize(boxed.size());
            return primitiveChunk;
        }

        public void unboxTo(ObjectChunk<?, ? extends Values> boxed, WritableChunk<? extends Values> primitives, int sourceOffset, int destOffset) {
            unboxTo(boxed, primitives.asWritableFloatChunk(), sourceOffset, destOffset);
        }

        public void unboxTo(ObjectChunk<?, ? extends Values> boxed, WritableFloatChunk<? extends Values> primitives, int sourceOffset, int destOffset) {
            final ObjectChunk<Float, ? extends Values> floatChunk = boxed.asObjectChunk();
            for (int ii = 0; ii < boxed.size(); ++ii) {
                primitives.set(ii + destOffset, floatChunk.get(ii + sourceOffset));
            }
        }
    }

    private static class DoubleUnboxer implements UnboxerKernel {
        final WritableDoubleChunk<Values> primitiveChunk;

        DoubleUnboxer(int capacity) {
            primitiveChunk = WritableDoubleChunk.makeWritableChunk(capacity);
        }

        @Override
        public void close() {
            primitiveChunk.close();
        }

        @Override
        public DoubleChunk<? extends Values> unbox(ObjectChunk<?, ? extends Values> boxed) {
            unboxTo(boxed, primitiveChunk, 0, 0);
            primitiveChunk.setSize(boxed.size());
            return primitiveChunk;
        }

        public void unboxTo(ObjectChunk<?, ? extends Values> boxed, WritableChunk<? extends Values> primitives, int sourceOffset, int destOffset) {
            unboxTo(boxed, primitives.asWritableDoubleChunk(), sourceOffset, destOffset);
        }

        public void unboxTo(ObjectChunk<?, ? extends Values> boxed, WritableDoubleChunk<? extends Values> primitives, int sourceOffset, int destOffset) {
            final ObjectChunk<Double, ? extends Values> doubleChunk = boxed.asObjectChunk();
            for (int ii = 0; ii < boxed.size(); ++ii) {
                primitives.set(ii + destOffset, doubleChunk.get(ii + sourceOffset));
            }
        }
    }
}
